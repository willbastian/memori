package dbschema

import (
	"context"
	"testing"
)

func TestCurrentVersionWithoutGooseTableReturnsZero(t *testing.T) {
	t.Parallel()

	got, err := currentVersion(context.Background(), openTestDB(t))
	if err != nil {
		t.Fatalf("current version without goose table: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected version 0, got %d", got)
	}
}

func TestStatusOfClampsPendingWhenDatabaseAheadOfHead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	status, err := Migrate(ctx, db, nil)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO goose_db_version(version_id, is_applied, tstamp)
		VALUES(?, 1, CURRENT_TIMESTAMP)
	`, status.HeadVersion+1); err != nil {
		t.Fatalf("insert ahead-of-head goose version: %v", err)
	}

	got, err := StatusOf(ctx, db)
	if err != nil {
		t.Fatalf("status of ahead-of-head db: %v", err)
	}
	if got.PendingMigrations != 0 {
		t.Fatalf("expected pending migrations to clamp at 0, got %d", got.PendingMigrations)
	}
}

func TestVerifyReportsDatabaseAheadOfHead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	status, err := Migrate(ctx, db, nil)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ahead := status.HeadVersion + 1
	if _, err := db.ExecContext(ctx, `
		INSERT INTO goose_db_version(version_id, is_applied, tstamp)
		VALUES(?, 1, CURRENT_TIMESTAMP)
	`, ahead); err != nil {
		t.Fatalf("insert ahead-of-head goose version: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE schema_meta SET value = ? WHERE key = 'db_schema_version'
	`, ahead); err != nil {
		t.Fatalf("update schema_meta version: %v", err)
	}

	verify, err := Verify(ctx, db)
	if err != nil {
		t.Fatalf("verify ahead-of-head db: %v", err)
	}
	if verify.OK {
		t.Fatalf("expected verify to fail for db ahead of head")
	}
	if !containsCheck(verify.Checks, "ahead of binary head") {
		t.Fatalf("expected ahead-of-head check, got %v", verify.Checks)
	}
}

func TestParseMigrationFilenameAndHelpers(t *testing.T) {
	t.Parallel()

	version, name, ok := parseMigrationFilename("00015_schema_migrations.sql")
	if !ok || version != 15 || name != "schema_migrations" {
		t.Fatalf("unexpected parsed migration filename: version=%d name=%q ok=%v", version, name, ok)
	}
	if _, _, ok := parseMigrationFilename("not-a-migration.txt"); ok {
		t.Fatal("expected invalid migration filename to be rejected")
	}
	if got := filepathExt("file.sql"); got != ".sql" {
		t.Fatalf("unexpected filepath ext: %q", got)
	}
	if got := filepathExt("noext"); got != "" {
		t.Fatalf("expected empty filepath ext, got %q", got)
	}
}

func TestReadMigrationDefinitionsAndMapAreConsistent(t *testing.T) {
	t.Parallel()

	defs, err := readMigrationDefinitions()
	if err != nil {
		t.Fatalf("read migration definitions: %v", err)
	}
	if len(defs) == 0 {
		t.Fatal("expected embedded migration definitions")
	}

	byVersion, err := migrationDefinitionMap()
	if err != nil {
		t.Fatalf("migration definition map: %v", err)
	}
	for _, def := range defs {
		mapped, ok := byVersion[def.Version]
		if !ok {
			t.Fatalf("missing version %d in definition map", def.Version)
		}
		if mapped.Name != def.Name || mapped.Checksum != def.Checksum {
			t.Fatalf("mapped definition mismatch for version %d", def.Version)
		}
	}
}

func TestSyncSchemaMetaPreservesExistingIssuePrefix(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)

	if err := syncSchemaMeta(ctx, db, 3); err != nil {
		t.Fatalf("initial sync schema meta: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE schema_meta SET value = 'ops' WHERE key = 'issue_key_prefix'
	`); err != nil {
		t.Fatalf("set custom issue key prefix: %v", err)
	}
	if err := syncSchemaMeta(ctx, db, 4); err != nil {
		t.Fatalf("second sync schema meta: %v", err)
	}

	var prefix string
	if err := db.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = 'issue_key_prefix'`).Scan(&prefix); err != nil {
		t.Fatalf("read issue_key_prefix: %v", err)
	}
	if prefix != "ops" {
		t.Fatalf("expected custom issue prefix to be preserved, got %q", prefix)
	}
}

func TestSyncMigrationAuditNoopsWithoutAuditTable(t *testing.T) {
	t.Parallel()

	if err := syncMigrationAudit(context.Background(), openTestDB(t)); err != nil {
		t.Fatalf("expected syncMigrationAudit noop without schema_migrations table, got %v", err)
	}
}
