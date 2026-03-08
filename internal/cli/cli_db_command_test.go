package cli

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

type dbStatusEnvelope struct {
	ResponseSchemaVersion int    `json:"response_schema_version"`
	DBSchemaVersion       int    `json:"db_schema_version"`
	Command               string `json:"command"`
	Data                  struct {
		CurrentVersion    int `json:"current_version"`
		HeadVersion       int `json:"head_version"`
		PendingMigrations int `json:"pending_migrations"`
	} `json:"data"`
}

type dbVerifyEnvelope struct {
	ResponseSchemaVersion int    `json:"response_schema_version"`
	DBSchemaVersion       int    `json:"db_schema_version"`
	Command               string `json:"command"`
	Data                  struct {
		OK     bool     `json:"ok"`
		Checks []string `json:"checks"`
	} `json:"data"`
}

type dbMigrateEnvelope struct {
	ResponseSchemaVersion int    `json:"response_schema_version"`
	DBSchemaVersion       int    `json:"db_schema_version"`
	Command               string `json:"command"`
	Data                  struct {
		FromVersion       int    `json:"from_version"`
		CurrentVersion    int    `json:"current_version"`
		HeadVersion       int    `json:"head_version"`
		PendingMigrations int    `json:"pending_migrations"`
		BackupPath        string `json:"backup_path"`
	} `json:"data"`
}

type dbBackupEnvelope struct {
	ResponseSchemaVersion int    `json:"response_schema_version"`
	DBSchemaVersion       int    `json:"db_schema_version"`
	Command               string `json:"command"`
	Data                  struct {
		SourcePath string `json:"source_path"`
		TargetPath string `json:"target_path"`
		Status     string `json:"status"`
	} `json:"data"`
}

type dbReplayEnvelope struct {
	ResponseSchemaVersion int    `json:"response_schema_version"`
	DBSchemaVersion       int    `json:"db_schema_version"`
	Command               string `json:"command"`
	Data                  struct {
		EventsApplied int `json:"events_applied"`
	} `json:"data"`
}

func TestDBStatusMigrateAndVerifyJSON(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-db-cli.db")

	before := runDBStatusJSONForTest(t, dbPath)
	assertEnvelopeMetadata(t, before.ResponseSchemaVersion, before.DBSchemaVersion)
	if before.Command != "db status" {
		t.Fatalf("expected db status command, got %q", before.Command)
	}
	if before.Data.CurrentVersion != 0 {
		t.Fatalf("expected current version 0 before migrate, got %d", before.Data.CurrentVersion)
	}
	if before.Data.HeadVersion == 0 {
		t.Fatalf("expected non-zero head version")
	}
	if before.Data.PendingMigrations == 0 {
		t.Fatalf("expected pending migrations before migrate")
	}

	stdout, stderr, err := runMemoriForTest("db", "migrate", "--db", dbPath, "--json")
	if err != nil {
		t.Fatalf("run db migrate: %v\nstderr: %s", err, stderr)
	}
	var migrate dbMigrateEnvelope
	if err := json.Unmarshal([]byte(stdout), &migrate); err != nil {
		t.Fatalf("decode db migrate json output: %v\nstdout: %s", err, stdout)
	}
	assertEnvelopeMetadata(t, migrate.ResponseSchemaVersion, migrate.DBSchemaVersion)
	if migrate.Command != "db migrate" || migrate.Data.BackupPath == "" {
		t.Fatalf("expected db migrate backup metadata, got %+v", migrate)
	}
	if _, err := os.Stat(migrate.Data.BackupPath); err != nil {
		t.Fatalf("migration backup missing: %v", err)
	}

	after := runDBStatusJSONForTest(t, dbPath)
	assertEnvelopeMetadata(t, after.ResponseSchemaVersion, after.DBSchemaVersion)
	if after.Data.CurrentVersion != after.Data.HeadVersion {
		t.Fatalf("expected current=head after migrate, got current=%d head=%d", after.Data.CurrentVersion, after.Data.HeadVersion)
	}
	if after.Data.PendingMigrations != 0 {
		t.Fatalf("expected no pending migrations after migrate, got %d", after.Data.PendingMigrations)
	}

	stdout, stderr, err = runMemoriForTest("db", "verify", "--db", dbPath, "--json")
	if err != nil {
		t.Fatalf("run db verify: %v\nstderr: %s", err, stderr)
	}
	var verify dbVerifyEnvelope
	if err := json.Unmarshal([]byte(stdout), &verify); err != nil {
		t.Fatalf("decode db verify json output: %v\nstdout: %s", err, stdout)
	}
	assertEnvelopeMetadata(t, verify.ResponseSchemaVersion, verify.DBSchemaVersion)
	if verify.Command != "db verify" || !verify.Data.OK {
		t.Fatalf("expected db verify ok, got %+v", verify)
	}

	backupPath := filepath.Join(t.TempDir(), "memori-backup.db")
	stdout, stderr, err = runMemoriForTest("db", "backup", "--db", dbPath, "--out", backupPath, "--json")
	if err != nil {
		t.Fatalf("run db backup: %v\nstderr: %s", err, stderr)
	}
	var backup dbBackupEnvelope
	if err := json.Unmarshal([]byte(stdout), &backup); err != nil {
		t.Fatalf("decode db backup json output: %v\nstdout: %s", err, stdout)
	}
	assertEnvelopeMetadata(t, backup.ResponseSchemaVersion, backup.DBSchemaVersion)
	if backup.Command != "db backup" || backup.Data.Status != "ok" {
		t.Fatalf("expected db backup ok, got %+v", backup)
	}
	info, err := os.Stat(backupPath)
	if err != nil {
		t.Fatalf("backup file missing: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("backup file should be non-empty")
	}

	_, stderr, err = runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--type", "task",
		"--title", "post migrate create",
		"--command-id", "cmd-db-cli-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("expected issue create after db migrate to succeed: %v\nstderr: %s", err, stderr)
	}
}

func TestDBMigrateRejectsInvalidToVersion(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-db-cli-invalid.db")
	_, _, err := runMemoriForTest("db", "migrate", "--db", dbPath, "--to", "999")
	if err == nil || !strings.Contains(err.Error(), "must be <= head version") {
		t.Fatalf("expected invalid --to error, got: %v", err)
	}
}

func TestDBMigrateToSpecificVersionHumanOutput(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-db-cli-targeted.db")

	stdout, stderr, err := runMemoriForTest("db", "migrate", "--db", dbPath, "--to", "3")
	if err != nil {
		t.Fatalf("run db migrate --to 3: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "Migrated database from version 0 to 3") {
		t.Fatalf("expected targeted migrate summary, got %q", stdout)
	}
	if !strings.Contains(stdout, "Pending migrations:") {
		t.Fatalf("expected pending migrations summary, got %q", stdout)
	}
}

func TestDBReplayJSONReportsAppliedEvents(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-db-cli-replay.db")

	if _, stderr, err := runMemoriForTest("db", "migrate", "--db", dbPath, "--json"); err != nil {
		t.Fatalf("run db migrate: %v\nstderr: %s", err, stderr)
	}

	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-c6d7e8f",
		"--type", "task",
		"--title", "Replay after targeted migrate",
		"--command-id", "cmd-db-replay-issue-1",
		"--json",
	); err != nil {
		t.Fatalf("create issue before replay: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest("db", "replay", "--db", dbPath, "--json")
	if err != nil {
		t.Fatalf("run db replay: %v\nstderr: %s", err, stderr)
	}
	var replay dbReplayEnvelope
	if err := json.Unmarshal([]byte(stdout), &replay); err != nil {
		t.Fatalf("decode db replay json output: %v\nstdout: %s", err, stdout)
	}
	assertEnvelopeMetadata(t, replay.ResponseSchemaVersion, replay.DBSchemaVersion)
	if replay.Command != "db replay" {
		t.Fatalf("expected db replay command, got %q", replay.Command)
	}
	if replay.Data.EventsApplied == 0 {
		t.Fatalf("expected replay to apply at least one event, got %+v", replay)
	}
}

func TestDBBackupRequiresOutPath(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-db-cli-backup-required.db")
	_, _, err := runMemoriForTest("db", "backup", "--db", dbPath)
	if err == nil || !strings.Contains(err.Error(), "--out is required") {
		t.Fatalf("expected missing --out error, got %v", err)
	}
}

func TestDBVerifyFailsWhenEventsTableMissing(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-db-cli-missing-events.db")
	if _, stderr, err := runMemoriForTest("db", "migrate", "--db", dbPath, "--json"); err != nil {
		t.Fatalf("run db migrate: %v\nstderr: %s", err, stderr)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`DROP TABLE events`); err != nil {
		t.Fatalf("drop events table: %v", err)
	}

	stdout, stderr, err := runMemoriForTest("db", "verify", "--db", dbPath, "--json")
	if err == nil || !strings.Contains(err.Error(), "required table missing: events") {
		t.Fatalf("expected missing events table error, got err=%v stderr=%s stdout=%s", err, stderr, stdout)
	}

	var verify dbVerifyEnvelope
	if err := json.Unmarshal([]byte(stdout), &verify); err != nil {
		t.Fatalf("decode db verify json output: %v\nstdout: %s", err, stdout)
	}
	assertEnvelopeMetadata(t, verify.ResponseSchemaVersion, verify.DBSchemaVersion)
	if verify.Command != "db verify" {
		t.Fatalf("expected db verify command, got %q", verify.Command)
	}
	if verify.Data.OK {
		t.Fatalf("expected db verify to fail when events table is missing")
	}
	if !containsString(verify.Data.Checks, "required table missing: events") {
		t.Fatalf("expected missing events check in JSON response, got %v", verify.Data.Checks)
	}
}

func TestDBVerifyFailsWhenWorkItemsTableMissing(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-db-cli-missing-work-items.db")
	if _, stderr, err := runMemoriForTest("db", "migrate", "--db", dbPath, "--json"); err != nil {
		t.Fatalf("run db migrate: %v\nstderr: %s", err, stderr)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`DROP TABLE work_items`); err != nil {
		t.Fatalf("drop work_items table: %v", err)
	}

	stdout, stderr, err := runMemoriForTest("db", "verify", "--db", dbPath, "--json")
	if err == nil || !strings.Contains(err.Error(), "required table missing: work_items") {
		t.Fatalf("expected missing work_items table error, got err=%v stderr=%s stdout=%s", err, stderr, stdout)
	}

	var verify dbVerifyEnvelope
	if err := json.Unmarshal([]byte(stdout), &verify); err != nil {
		t.Fatalf("decode db verify json output: %v\nstdout: %s", err, stdout)
	}
	assertEnvelopeMetadata(t, verify.ResponseSchemaVersion, verify.DBSchemaVersion)
	if verify.Data.OK {
		t.Fatalf("expected db verify to fail when work_items table is missing")
	}
	if !containsString(verify.Data.Checks, "required table missing: work_items") {
		t.Fatalf("expected missing work_items check in JSON response, got %v", verify.Data.Checks)
	}
}

func TestDBVerifyFailsWhenSchemaMigrationChecksumDrifts(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-db-cli-schema-migration-drift.db")
	if _, stderr, err := runMemoriForTest("db", "migrate", "--db", dbPath, "--json"); err != nil {
		t.Fatalf("run db migrate: %v\nstderr: %s", err, stderr)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`UPDATE schema_migrations SET checksum = 'tampered-checksum' WHERE version = (SELECT MAX(version) FROM schema_migrations)`); err != nil {
		t.Fatalf("tamper schema_migrations checksum: %v", err)
	}

	stdout, stderr, err := runMemoriForTest("db", "verify", "--db", dbPath, "--json")
	if err == nil || !strings.Contains(err.Error(), "schema_migrations checksum mismatch") {
		t.Fatalf("expected schema_migrations checksum mismatch error, got err=%v stderr=%s stdout=%s", err, stderr, stdout)
	}

	var verify dbVerifyEnvelope
	if err := json.Unmarshal([]byte(stdout), &verify); err != nil {
		t.Fatalf("decode db verify json output: %v\nstdout: %s", err, stdout)
	}
	if verify.Data.OK {
		t.Fatalf("expected db verify to fail when schema_migrations checksum drifts")
	}
	if !containsString(verify.Data.Checks, "schema_migrations checksum mismatch") {
		t.Fatalf("expected checksum drift check in JSON response, got %v", verify.Data.Checks)
	}
}

func runDBStatusJSONForTest(t *testing.T, dbPath string) dbStatusEnvelope {
	t.Helper()

	stdout, stderr, err := runMemoriForTest("db", "status", "--db", dbPath, "--json")
	if err != nil {
		t.Fatalf("run db status json command: %v\nstderr: %s", err, stderr)
	}

	var resp dbStatusEnvelope
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		t.Fatalf("decode db status json output: %v\nstdout: %s", err, stdout)
	}
	return resp
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}
