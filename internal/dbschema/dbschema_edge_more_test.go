package dbschema

import (
	"context"
	"strings"
	"testing"
)

func TestVerifyUninitializedDatabaseReportsSchemaMetaFailure(t *testing.T) {
	t.Parallel()

	verify, err := Verify(context.Background(), openTestDB(t))
	if err != nil {
		t.Fatalf("verify uninitialized database: %v", err)
	}
	if verify.OK {
		t.Fatal("expected verify to fail for uninitialized database")
	}
	if !containsCheck(verify.Checks, "read schema_meta db_schema_version") {
		t.Fatalf("expected schema_meta read failure, got %v", verify.Checks)
	}
}

func TestSQLiteTableExistsAndSchemaMetaVersionErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	if _, err := Migrate(ctx, db, nil); err != nil {
		t.Fatalf("migrate database: %v", err)
	}

	exists, err := sqliteTableExists(ctx, db, "events")
	if err != nil {
		t.Fatalf("sqliteTableExists events: %v", err)
	}
	if !exists {
		t.Fatal("expected events table to exist")
	}

	exists, err = sqliteTableExists(ctx, db, "missing_table")
	if err != nil {
		t.Fatalf("sqliteTableExists missing_table: %v", err)
	}
	if exists {
		t.Fatal("expected missing table to report false")
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE schema_meta SET value = 'not-a-number' WHERE key = 'db_schema_version'
	`); err != nil {
		t.Fatalf("tamper schema_meta version: %v", err)
	}
	if _, err := schemaMetaVersion(ctx, db); err == nil {
		t.Fatal("expected schemaMetaVersion parse error")
	}
}

func TestSyncSchemaMetaNoopsAtZeroVersionAndVerifyMigrationAuditUnexpectedRow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)

	if err := syncSchemaMeta(ctx, db, 0); err != nil {
		t.Fatalf("syncSchemaMeta zero version: %v", err)
	}
	exists, err := sqliteTableExists(ctx, db, "schema_meta")
	if err != nil {
		t.Fatalf("check schema_meta existence: %v", err)
	}
	if exists {
		t.Fatal("expected syncSchemaMeta at version 0 to be a no-op")
	}

	status, err := Migrate(ctx, db, nil)
	if err != nil {
		t.Fatalf("migrate database: %v", err)
	}
	if status.CurrentVersion < 15 {
		t.Fatalf("expected current version >= 15, got %d", status.CurrentVersion)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO schema_migrations(version, name, checksum, applied_at, applied_by, duration_ms, success, error_message)
		VALUES(999, 'future_migration', 'checksum', CURRENT_TIMESTAMP, 'test', 0, 1, NULL)
	`); err != nil {
		t.Fatalf("insert unexpected schema_migrations row: %v", err)
	}

	failures, err := verifyMigrationAudit(ctx, db, status.CurrentVersion)
	if err != nil {
		t.Fatalf("verifyMigrationAudit: %v", err)
	}
	if !containsCheck(failures, "unexpected row for unapplied version 999") {
		t.Fatalf("expected unexpected unapplied row failure, got %v", failures)
	}
}

func TestMigrateToSpecificVersionKeepsVerifyHealthy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	target := 3

	status, err := Migrate(ctx, db, &target)
	if err != nil {
		t.Fatalf("migrate to specific version: %v", err)
	}
	if status.CurrentVersion != target {
		t.Fatalf("expected current version %d, got %d", target, status.CurrentVersion)
	}
	if status.HeadVersion <= target || status.PendingMigrations == 0 {
		t.Fatalf("expected pending migrations above target, got %#v", status)
	}

	metaVersion, err := schemaMetaVersion(ctx, db)
	if err != nil {
		t.Fatalf("schemaMetaVersion after targeted migrate: %v", err)
	}
	if metaVersion != target {
		t.Fatalf("expected schema_meta version %d, got %d", target, metaVersion)
	}

	verify, err := Verify(ctx, db)
	if err != nil {
		t.Fatalf("verify targeted migrate: %v", err)
	}
	if !verify.OK {
		t.Fatalf("expected verify OK after targeted migrate, got %v", verify.Checks)
	}
}

func TestVerifyReportsRequiredTableAndSchemaMetaMismatchTogether(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)

	status, err := Migrate(ctx, db, nil)
	if err != nil {
		t.Fatalf("migrate database: %v", err)
	}

	if _, err := db.ExecContext(ctx, `DROP TABLE gate_status_projection`); err != nil {
		t.Fatalf("drop gate_status_projection: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE schema_meta SET value = ? WHERE key = 'db_schema_version'
	`, status.CurrentVersion-1); err != nil {
		t.Fatalf("tamper schema_meta version: %v", err)
	}

	verify, err := Verify(ctx, db)
	if err != nil {
		t.Fatalf("verify tampered database: %v", err)
	}
	if verify.OK {
		t.Fatalf("expected verify failure for missing table and schema mismatch")
	}
	if !containsCheck(verify.Checks, "required table missing: gate_status_projection") {
		t.Fatalf("expected missing required table check, got %v", verify.Checks)
	}
	if !containsCheck(verify.Checks, "schema_meta db_schema_version") {
		t.Fatalf("expected schema_meta mismatch check, got %v", verify.Checks)
	}
}

func TestClosedDBHelpersSurfaceQueryErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	if _, err := currentVersion(ctx, db); err == nil || !strings.Contains(err.Error(), "read goose_db_version") {
		t.Fatalf("expected currentVersion query error, got %v", err)
	}
	if _, err := sqliteTableExists(ctx, db, "events"); err == nil || !strings.Contains(err.Error(), `lookup table "events"`) {
		t.Fatalf("expected sqliteTableExists query error, got %v", err)
	}
}

func TestSyncMigrationAuditBackfillsMissingAppliedAtAndMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	status, err := Migrate(ctx, db, nil)
	if err != nil {
		t.Fatalf("migrate database: %v", err)
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM schema_migrations`); err != nil {
		t.Fatalf("clear schema_migrations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE goose_db_version SET tstamp = NULL WHERE version_id = ?`, status.CurrentVersion); err != nil {
		t.Fatalf("clear goose tstamp: %v", err)
	}

	if err := syncMigrationAudit(ctx, db); err != nil {
		t.Fatalf("syncMigrationAudit: %v", err)
	}

	var (
		appliedAt string
		appliedBy string
		success   int
	)
	if err := db.QueryRowContext(ctx, `
		SELECT applied_at, applied_by, success
		FROM schema_migrations
		WHERE version = ?
	`, status.CurrentVersion).Scan(&appliedAt, &appliedBy, &success); err != nil {
		t.Fatalf("read synced migration audit row: %v", err)
	}
	if strings.TrimSpace(appliedAt) == "" {
		t.Fatal("expected syncMigrationAudit to backfill applied_at")
	}
	if appliedBy != "github.com/willbastian/memori/internal/dbschema" || success != 1 {
		t.Fatalf("unexpected synced migration audit metadata applied_by=%q success=%d", appliedBy, success)
	}
}

func TestSyncSchemaMetaInsertsDefaultIssuePrefixWhenMissing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)

	if err := syncSchemaMeta(ctx, db, 2); err != nil {
		t.Fatalf("initial syncSchemaMeta: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM schema_meta WHERE key = 'issue_key_prefix'`); err != nil {
		t.Fatalf("delete issue_key_prefix: %v", err)
	}

	if err := syncSchemaMeta(ctx, db, 3); err != nil {
		t.Fatalf("syncSchemaMeta after removing prefix: %v", err)
	}

	var (
		prefix        string
		schemaVersion string
	)
	if err := db.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = 'issue_key_prefix'`).Scan(&prefix); err != nil {
		t.Fatalf("read restored issue_key_prefix: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = 'db_schema_version'`).Scan(&schemaVersion); err != nil {
		t.Fatalf("read db_schema_version: %v", err)
	}
	if prefix != DefaultIssueKeyPrefix {
		t.Fatalf("expected restored default issue prefix %q, got %q", DefaultIssueKeyPrefix, prefix)
	}
	if schemaVersion != "3" {
		t.Fatalf("expected updated db_schema_version %q, got %q", "3", schemaVersion)
	}
}

func TestSyncMigrationAuditFailsWhenAppliedVersionIsMissingFromEmbeddedCatalog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	status, err := Migrate(ctx, db, nil)
	if err != nil {
		t.Fatalf("migrate database: %v", err)
	}

	missingVersion := status.HeadVersion + 1
	if _, err := db.ExecContext(ctx, `
		INSERT INTO goose_db_version(version_id, is_applied, tstamp)
		VALUES(?, 1, CURRENT_TIMESTAMP)
	`, missingVersion); err != nil {
		t.Fatalf("insert ahead-of-catalog goose version: %v", err)
	}

	if err := syncMigrationAudit(ctx, db); err == nil || !containsCheck([]string{err.Error()}, "missing from embedded catalog") {
		t.Fatalf("expected missing embedded catalog error, got %v", err)
	}
}

func TestSyncSchemaMetaFailsOnClosedDB(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	if err := syncSchemaMeta(ctx, db, 2); err == nil || !containsCheck([]string{err.Error()}, "begin tx for schema_meta sync") {
		t.Fatalf("expected syncSchemaMeta closed-db error, got %v", err)
	}
}

func TestStatusOfMigrateAndVerifySurfaceClosedDBErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	if _, err := StatusOf(ctx, db); err == nil || !containsCheck([]string{err.Error()}, "read goose_db_version") {
		t.Fatalf("expected StatusOf closed-db error, got %v", err)
	}
	if _, err := Migrate(ctx, db, nil); err == nil || !containsCheck([]string{err.Error()}, "read goose_db_version") {
		t.Fatalf("expected Migrate closed-db error, got %v", err)
	}
	if _, err := Verify(ctx, db); err == nil || !containsCheck([]string{err.Error()}, "read goose_db_version") {
		t.Fatalf("expected Verify closed-db error, got %v", err)
	}
}

func TestVerifyReportsDatabaseAheadOfBinaryHeadAndStatusClampsPending(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	status, err := Migrate(ctx, db, nil)
	if err != nil {
		t.Fatalf("migrate database: %v", err)
	}

	aheadVersion := status.HeadVersion + 1
	if _, err := db.ExecContext(ctx, `
		INSERT INTO goose_db_version(version_id, is_applied, tstamp)
		VALUES(?, 1, CURRENT_TIMESTAMP)
	`, aheadVersion); err != nil {
		t.Fatalf("insert ahead-of-head goose version: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE schema_meta SET value = ? WHERE key = 'db_schema_version'
	`, aheadVersion); err != nil {
		t.Fatalf("update schema_meta ahead version: %v", err)
	}

	gotStatus, err := StatusOf(ctx, db)
	if err != nil {
		t.Fatalf("status of ahead-of-head database: %v", err)
	}
	if gotStatus.CurrentVersion != aheadVersion {
		t.Fatalf("expected current version %d, got %d", aheadVersion, gotStatus.CurrentVersion)
	}
	if gotStatus.PendingMigrations != 0 {
		t.Fatalf("expected pending migrations to clamp at 0, got %d", gotStatus.PendingMigrations)
	}

	verify, err := Verify(ctx, db)
	if err != nil {
		t.Fatalf("verify ahead-of-head database: %v", err)
	}
	if verify.OK {
		t.Fatal("expected verify to fail when database is ahead of binary head")
	}
	if !containsCheck(verify.Checks, "ahead of binary head") {
		t.Fatalf("expected ahead-of-head verify check, got %v", verify.Checks)
	}
}
