package dbschema

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"time"

	"github.com/pressly/goose/v3"
)

const (
	DefaultIssueKeyPrefix = "mem"
	MinSupportedVersion   = 1
	migrationsDir         = "migrations"
)

type Status struct {
	CurrentVersion    int `json:"current_version"`
	HeadVersion       int `json:"head_version"`
	PendingMigrations int `json:"pending_migrations"`
}

type VerifyResult struct {
	OK                bool     `json:"ok"`
	CurrentVersion    int      `json:"current_version"`
	HeadVersion       int      `json:"head_version"`
	SchemaMetaVersion int      `json:"schema_meta_version"`
	Checks            []string `json:"checks"`
}

//go:embed migrations/*.sql
var migrationsFS embed.FS

func StatusOf(ctx context.Context, db *sql.DB) (Status, error) {
	headVersion, err := headVersion()
	if err != nil {
		return Status{}, err
	}
	currentVersion, err := currentVersion(ctx, db)
	if err != nil {
		return Status{}, err
	}
	pending := headVersion - currentVersion
	if pending < 0 {
		pending = 0
	}
	return Status{
		CurrentVersion:    currentVersion,
		HeadVersion:       headVersion,
		PendingMigrations: pending,
	}, nil
}

func Migrate(ctx context.Context, db *sql.DB, to *int) (Status, error) {
	statusBefore, err := StatusOf(ctx, db)
	if err != nil {
		return Status{}, err
	}

	if to != nil {
		if *to < statusBefore.CurrentVersion {
			return Status{}, fmt.Errorf(
				"invalid --to %d (must be >= current version %d)",
				*to,
				statusBefore.CurrentVersion,
			)
		}
		if *to > statusBefore.HeadVersion {
			return Status{}, fmt.Errorf(
				"invalid --to %d (must be <= head version %d)",
				*to,
				statusBefore.HeadVersion,
			)
		}
	}

	if err := configureGoose(); err != nil {
		return Status{}, err
	}
	if to == nil {
		if err := goose.UpContext(ctx, db, migrationsDir); err != nil {
			return Status{}, fmt.Errorf("migrate up: %w", err)
		}
	} else {
		if err := goose.UpToContext(ctx, db, migrationsDir, int64(*to)); err != nil {
			return Status{}, fmt.Errorf("migrate up to %d: %w", *to, err)
		}
	}

	statusAfter, err := StatusOf(ctx, db)
	if err != nil {
		return Status{}, err
	}
	if err := syncSchemaMeta(ctx, db, statusAfter.CurrentVersion); err != nil {
		return Status{}, err
	}

	return statusAfter, nil
}

func Verify(ctx context.Context, db *sql.DB) (VerifyResult, error) {
	status, err := StatusOf(ctx, db)
	if err != nil {
		return VerifyResult{}, err
	}

	result := VerifyResult{
		OK:             true,
		CurrentVersion: status.CurrentVersion,
		HeadVersion:    status.HeadVersion,
		Checks:         make([]string, 0, 4),
	}

	metaVersion, err := schemaMetaVersion(ctx, db)
	if err != nil {
		result.OK = false
		result.Checks = append(result.Checks, err.Error())
		return result, nil
	}
	result.SchemaMetaVersion = metaVersion

	if status.CurrentVersion == 0 {
		result.OK = false
		result.Checks = append(result.Checks, "database has no applied migrations")
	}
	if metaVersion != status.CurrentVersion {
		result.OK = false
		result.Checks = append(
			result.Checks,
			fmt.Sprintf("schema_meta db_schema_version=%d does not match goose version=%d", metaVersion, status.CurrentVersion),
		)
	}
	if status.CurrentVersion > status.HeadVersion {
		result.OK = false
		result.Checks = append(
			result.Checks,
			fmt.Sprintf("database version %d is ahead of binary head %d", status.CurrentVersion, status.HeadVersion),
		)
	}

	if result.OK {
		result.Checks = append(result.Checks, "schema versions are consistent")
	}
	return result, nil
}

func configureGoose() error {
	goose.SetBaseFS(migrationsFS)
	goose.SetVerbose(false)
	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("sqlite3"); err != nil {
		return fmt.Errorf("set goose dialect: %w", err)
	}
	return nil
}

func currentVersion(ctx context.Context, db *sql.DB) (int, error) {
	var currentVersion int
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(version_id), 0)
		FROM goose_db_version
		WHERE is_applied = 1
	`).Scan(&currentVersion)
	if err != nil {
		// Database has not been migrated yet.
		if strings.Contains(strings.ToLower(err.Error()), "no such table: goose_db_version") {
			return 0, nil
		}
		return 0, fmt.Errorf("read goose_db_version: %w", err)
	}
	return currentVersion, nil
}

func headVersion() (int, error) {
	entries, err := fs.ReadDir(migrationsFS, migrationsDir)
	if err != nil {
		return 0, fmt.Errorf("read embedded migrations: %w", err)
	}

	maxVersion := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		prefix, _, hasUnderscore := strings.Cut(name, "_")
		if !hasUnderscore {
			continue
		}
		version, err := strconv.Atoi(prefix)
		if err != nil {
			continue
		}
		if version > maxVersion {
			maxVersion = version
		}
	}
	return maxVersion, nil
}

func syncSchemaMeta(ctx context.Context, db *sql.DB, currentVersion int) error {
	if currentVersion <= 0 {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx for schema_meta sync: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("ensure schema_meta table: %w", err)
	}

	now := nowUTC()
	meta := []struct {
		key   string
		value string
	}{
		{key: "db_schema_version", value: strconv.Itoa(currentVersion)},
		{key: "min_supported_db_schema_version", value: strconv.Itoa(MinSupportedVersion)},
		{key: "last_migrated_at_utc", value: now},
	}
	for _, item := range meta {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO schema_meta(key, value, updated_at)
			VALUES(?, ?, ?)
			ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
		`, item.key, item.value, now); err != nil {
			return fmt.Errorf("upsert schema_meta %s: %w", item.key, err)
		}
	}

	var existingPrefix sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = 'issue_key_prefix'`).Scan(&existingPrefix)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read issue_key_prefix: %w", err)
	}
	if !existingPrefix.Valid {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO schema_meta(key, value, updated_at) VALUES('issue_key_prefix', ?, ?)
		`, DefaultIssueKeyPrefix, now); err != nil {
			return fmt.Errorf("insert issue_key_prefix: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema_meta sync tx: %w", err)
	}
	return nil
}

func schemaMetaVersion(ctx context.Context, db *sql.DB) (int, error) {
	var raw string
	err := db.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = 'db_schema_version'`).Scan(&raw)
	if err != nil {
		return 0, fmt.Errorf("read schema_meta db_schema_version: %w", err)
	}
	version, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse schema_meta db_schema_version %q: %w", raw, err)
	}
	return version, nil
}

func nowUTC() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
