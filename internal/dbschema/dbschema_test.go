package dbschema

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestStatusMigrateAndVerify(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)

	before, err := StatusOf(ctx, db)
	if err != nil {
		t.Fatalf("status before migrate: %v", err)
	}
	if before.CurrentVersion != 0 {
		t.Fatalf("expected current version 0 before migrate, got %d", before.CurrentVersion)
	}
	if before.HeadVersion == 0 {
		t.Fatalf("expected non-zero head version")
	}
	if before.PendingMigrations == 0 {
		t.Fatalf("expected pending migrations before migrate")
	}

	after, err := Migrate(ctx, db, nil)
	if err != nil {
		t.Fatalf("migrate to head: %v", err)
	}
	if after.CurrentVersion != after.HeadVersion {
		t.Fatalf("expected current to equal head after migrate, got current=%d head=%d", after.CurrentVersion, after.HeadVersion)
	}
	if after.PendingMigrations != 0 {
		t.Fatalf("expected no pending migrations after migrate, got %d", after.PendingMigrations)
	}

	verify, err := Verify(ctx, db)
	if err != nil {
		t.Fatalf("verify migrated db: %v", err)
	}
	if !verify.OK {
		t.Fatalf("expected verify OK, got checks: %v", verify.Checks)
	}

	var auditCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(1) FROM schema_migrations WHERE success = 1`).Scan(&auditCount); err != nil {
		t.Fatalf("count schema_migrations rows: %v", err)
	}
	if auditCount != after.CurrentVersion {
		t.Fatalf("expected schema_migrations rows for each applied version, got %d want %d", auditCount, after.CurrentVersion)
	}
}

func TestMigrateRejectsInvalidToVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)

	headStatus, err := StatusOf(ctx, db)
	if err != nil {
		t.Fatalf("status before migrate: %v", err)
	}
	invalidHigh := headStatus.HeadVersion + 1
	if _, err := Migrate(ctx, db, &invalidHigh); err == nil || !strings.Contains(err.Error(), "must be <= head version") {
		t.Fatalf("expected invalid high --to error, got: %v", err)
	}

	if _, err := Migrate(ctx, db, nil); err != nil {
		t.Fatalf("migrate to head: %v", err)
	}
	invalidLow := 0
	if _, err := Migrate(ctx, db, &invalidLow); err == nil || !strings.Contains(err.Error(), "must be >= current version") {
		t.Fatalf("expected invalid low --to error, got: %v", err)
	}
}

func TestVerifyFailsWhenEventsTableMissing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	if _, err := Migrate(ctx, db, nil); err != nil {
		t.Fatalf("migrate to head: %v", err)
	}

	if _, err := db.ExecContext(ctx, `DROP TABLE events`); err != nil {
		t.Fatalf("drop events table: %v", err)
	}

	verify, err := Verify(ctx, db)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if verify.OK {
		t.Fatalf("expected verify to fail when events table is missing")
	}
	if !containsCheck(verify.Checks, "required table missing: events") {
		t.Fatalf("expected missing events table check, got: %v", verify.Checks)
	}
}

func TestVerifyFailsWhenRequiredHeadSchemaTableMissing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	if _, err := Migrate(ctx, db, nil); err != nil {
		t.Fatalf("migrate to head: %v", err)
	}

	if _, err := db.ExecContext(ctx, `DROP TABLE work_items`); err != nil {
		t.Fatalf("drop work_items table: %v", err)
	}

	verify, err := Verify(ctx, db)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if verify.OK {
		t.Fatalf("expected verify to fail when work_items is missing")
	}
	if !containsCheck(verify.Checks, "required table missing: work_items") {
		t.Fatalf("expected missing work_items check, got: %v", verify.Checks)
	}
}

func TestVerifyChecksTablesForCurrentMigrationLevelOnly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	targetVersion := 4
	if _, err := Migrate(ctx, db, &targetVersion); err != nil {
		t.Fatalf("migrate to version %d: %v", targetVersion, err)
	}

	verify, err := Verify(ctx, db)
	if err != nil {
		t.Fatalf("verify migrated db at version %d: %v", targetVersion, err)
	}
	if !verify.OK {
		t.Fatalf("expected verify OK at version %d, got checks: %v", targetVersion, verify.Checks)
	}

	if _, err := db.ExecContext(ctx, `DROP TABLE sessions`); err != nil {
		t.Fatalf("drop sessions table: %v", err)
	}

	verify, err = Verify(ctx, db)
	if err != nil {
		t.Fatalf("verify after dropping sessions: %v", err)
	}
	if verify.OK {
		t.Fatalf("expected verify to fail when version %d table sessions is missing", targetVersion)
	}
	if !containsCheck(verify.Checks, "required table missing: sessions") {
		t.Fatalf("expected missing sessions check, got: %v", verify.Checks)
	}
	if containsCheck(verify.Checks, "required table missing: context_chunks") {
		t.Fatalf("did not expect version 5 tables to be required at version %d: %v", targetVersion, verify.Checks)
	}
}

func TestVerifyFailsWhenSchemaMigrationAuditRowMissing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	status, err := Migrate(ctx, db, nil)
	if err != nil {
		t.Fatalf("migrate to head: %v", err)
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM schema_migrations WHERE version = ?`, status.CurrentVersion); err != nil {
		t.Fatalf("delete schema_migrations row: %v", err)
	}

	verify, err := Verify(ctx, db)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if verify.OK {
		t.Fatalf("expected verify to fail when schema_migrations row is missing")
	}
	if !containsCheck(verify.Checks, "schema_migrations missing row for version") {
		t.Fatalf("expected missing schema_migrations check, got %v", verify.Checks)
	}
}

func TestVerifyFailsWhenSchemaMigrationChecksumDrifts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	status, err := Migrate(ctx, db, nil)
	if err != nil {
		t.Fatalf("migrate to head: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE schema_migrations
		SET checksum = 'tampered-checksum'
		WHERE version = ?
	`, status.CurrentVersion); err != nil {
		t.Fatalf("tamper schema_migrations checksum: %v", err)
	}

	verify, err := Verify(ctx, db)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if verify.OK {
		t.Fatalf("expected verify to fail when schema_migrations checksum drifts")
	}
	if !containsCheck(verify.Checks, "schema_migrations checksum mismatch for version") {
		t.Fatalf("expected schema_migrations checksum mismatch check, got %v", verify.Checks)
	}
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "memori-dbschema-test.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	stmts := []string{
		"PRAGMA journal_mode = WAL;",
		"PRAGMA foreign_keys = ON;",
		"PRAGMA busy_timeout = 5000;",
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("apply pragma %q: %v", stmt, err)
		}
	}
	return db
}

type verifyTestEvent struct {
	EventID             string
	EventOrder          int64
	EntityType          string
	EntityID            string
	EntitySeq           int64
	EventType           string
	PayloadJSON         string
	Actor               string
	CommandID           string
	CausationID         string
	CorrelationID       string
	CreatedAt           string
	Hash                string
	PrevHash            string
	EventPayloadVersion int
}

func insertEventForVerifyTest(t *testing.T, db *sql.DB, event verifyTestEvent) {
	t.Helper()

	_, err := db.Exec(`
		INSERT INTO events(
			event_id, event_order, entity_type, entity_id, entity_seq,
			event_type, payload_json, actor, command_id, causation_id,
			correlation_id, created_at, hash, prev_hash, event_payload_version
		)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		event.EventID,
		event.EventOrder,
		event.EntityType,
		event.EntityID,
		event.EntitySeq,
		event.EventType,
		event.PayloadJSON,
		event.Actor,
		event.CommandID,
		nullString(event.CausationID),
		nullString(event.CorrelationID),
		event.CreatedAt,
		event.Hash,
		nullString(event.PrevHash),
		event.EventPayloadVersion,
	)
	if err != nil {
		t.Fatalf("insert test event %s: %v", event.EventID, err)
	}
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func containsCheck(checks []string, want string) bool {
	for _, check := range checks {
		if strings.Contains(check, want) {
			return true
		}
	}
	return false
}
