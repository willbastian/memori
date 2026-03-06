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
