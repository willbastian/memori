package cli

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/willbastian/memori/internal/dbschema"
	"github.com/willbastian/memori/internal/store"
)

type dbStatusData struct {
	CurrentVersion    int `json:"current_version"`
	HeadVersion       int `json:"head_version"`
	PendingMigrations int `json:"pending_migrations"`
}

type dbMigrateData struct {
	FromVersion       int    `json:"from_version"`
	CurrentVersion    int    `json:"current_version"`
	HeadVersion       int    `json:"head_version"`
	PendingMigrations int    `json:"pending_migrations"`
	BackupPath        string `json:"backup_path,omitempty"`
}

type dbBackupData struct {
	SourcePath string `json:"source_path"`
	TargetPath string `json:"target_path"`
	Status     string `json:"status"`
}

func runDB(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("db subcommand required: status|migrate|verify|backup|replay")
	}

	switch args[0] {
	case "status":
		return runDBStatus(args[1:], out)
	case "migrate":
		return runDBMigrate(args[1:], out)
	case "verify":
		return runDBVerify(args[1:], out)
	case "backup":
		return runDBBackup(args[1:], out)
	case "replay":
		return runDBReplay(args[1:], out)
	default:
		return fmt.Errorf("unknown db subcommand %q", args[0])
	}
}

func runDBStatus(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("db status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	status, err := dbschema.StatusOf(ctx, s.DB())
	if err != nil {
		return err
	}
	dbVersion, err := s.SchemaVersion(ctx)
	if err != nil {
		dbVersion = 0
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "db status",
			Data: dbStatusData{
				CurrentVersion:    status.CurrentVersion,
				HeadVersion:       status.HeadVersion,
				PendingMigrations: status.PendingMigrations,
			},
		})
	}

	_, _ = fmt.Fprintf(out, "Current schema version: %d\n", status.CurrentVersion)
	_, _ = fmt.Fprintf(out, "Head schema version: %d\n", status.HeadVersion)
	_, _ = fmt.Fprintf(out, "Pending migrations: %d\n", status.PendingMigrations)
	return nil
}

func runDBMigrate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("db migrate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	to := fs.Int("to", 0, "target migration version (optional)")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	before, err := dbschema.StatusOf(ctx, s.DB())
	if err != nil {
		return err
	}

	toSet := hasFlag(args, "to")
	var toPtr *int
	if toSet {
		toPtr = to
	}
	if toPtr != nil {
		if *toPtr < before.CurrentVersion {
			return fmt.Errorf("invalid --to %d (must be >= current version %d)", *toPtr, before.CurrentVersion)
		}
		if *toPtr > before.HeadVersion {
			return fmt.Errorf("invalid --to %d (must be <= head version %d)", *toPtr, before.HeadVersion)
		}
	}
	backupPath, err := createMigrationBackup(ctx, s.DB(), *dbPath)
	if err != nil {
		return err
	}
	after, err := dbschema.Migrate(ctx, s.DB(), toPtr)
	if err != nil {
		if backupPath != "" {
			return fmt.Errorf("%w (restore from %s)", err, backupPath)
		}
		return err
	}

	dbVersion, err := s.SchemaVersion(ctx)
	if err != nil {
		dbVersion = 0
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "db migrate",
			Data: dbMigrateData{
				FromVersion:       before.CurrentVersion,
				CurrentVersion:    after.CurrentVersion,
				HeadVersion:       after.HeadVersion,
				PendingMigrations: after.PendingMigrations,
				BackupPath:        backupPath,
			},
		})
	}

	_, _ = fmt.Fprintf(out, "Migrated database from version %d to %d\n", before.CurrentVersion, after.CurrentVersion)
	if backupPath != "" {
		_, _ = fmt.Fprintf(out, "Backup path: %s\n", backupPath)
	}
	_, _ = fmt.Fprintf(out, "Head schema version: %d\n", after.HeadVersion)
	_, _ = fmt.Fprintf(out, "Pending migrations: %d\n", after.PendingMigrations)
	return nil
}

func runDBVerify(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("db verify", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	result, err := dbschema.Verify(ctx, s.DB())
	if err != nil {
		return err
	}

	dbVersion, err := s.SchemaVersion(ctx)
	if err != nil {
		dbVersion = 0
	}

	if *jsonOut {
		if err := printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "db verify",
			Data:                  result,
		}); err != nil {
			return err
		}
	} else if result.OK {
		_, _ = fmt.Fprintln(out, "Database verify: OK")
	} else {
		_, _ = fmt.Fprintln(out, "Database verify: FAILED")
	}

	if !result.OK {
		return errors.New(strings.Join(result.Checks, "; "))
	}
	if !*jsonOut {
		_, _ = fmt.Fprintln(out, strings.Join(result.Checks, "; "))
	}
	return nil
}

func runDBReplay(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("db replay", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	result, err := s.ReplayProjections(ctx)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "db replay",
			Data:                  result,
		})
	}

	_, _ = fmt.Fprintf(out, "Rebuilt projections from %d events\n", result.EventsApplied)
	return nil
}

func runDBBackup(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("db backup", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	outPath := fs.String("out", "", "backup destination path")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*outPath) == "" {
		return errors.New("--out is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	absSource, err := filepath.Abs(*dbPath)
	if err != nil {
		return fmt.Errorf("resolve --db path: %w", err)
	}
	absTarget, err := filepath.Abs(*outPath)
	if err != nil {
		return fmt.Errorf("resolve --out path: %w", err)
	}
	if absSource == absTarget {
		return errors.New("--out must be different from --db path")
	}
	if _, err := os.Stat(absTarget); err == nil {
		return fmt.Errorf("backup target already exists: %s", absTarget)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat backup target %s: %w", absTarget, err)
	}
	if err := os.MkdirAll(filepath.Dir(absTarget), 0o755); err != nil {
		return fmt.Errorf("create backup directory: %w", err)
	}
	if err := sqliteVacuumInto(ctx, s.DB(), absTarget); err != nil {
		return fmt.Errorf("backup database to %s: %w", absTarget, err)
	}

	data := dbBackupData{
		SourcePath: absSource,
		TargetPath: absTarget,
		Status:     "ok",
	}
	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "db backup",
			Data:                  data,
		})
	}

	_, _ = fmt.Fprintf(out, "Backed up %s -> %s\n", absSource, absTarget)
	return nil
}

func createMigrationBackup(ctx context.Context, db *sql.DB, dbPath string) (string, error) {
	absSource, err := filepath.Abs(dbPath)
	if err != nil {
		return "", fmt.Errorf("resolve --db path: %w", err)
	}
	timestamp := time.Now().UTC().Format("20060102T150405.000000000Z")
	backupPath := absSource + ".pre-migrate-" + timestamp + ".bak"
	if err := os.MkdirAll(filepath.Dir(backupPath), 0o755); err != nil {
		return "", fmt.Errorf("create migration backup directory: %w", err)
	}
	if err := sqliteVacuumInto(ctx, db, backupPath); err != nil {
		return "", fmt.Errorf("backup database before migrate to %s: %w", backupPath, err)
	}
	return backupPath, nil
}

func sqliteVacuumInto(ctx context.Context, db *sql.DB, outPath string) error {
	escapedOutPath := strings.ReplaceAll(outPath, "'", "''")
	_, err := db.ExecContext(ctx, "VACUUM INTO '"+escapedOutPath+"'")
	return err
}
