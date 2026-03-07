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

func TestVerifyDetectsHashChainPrevMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	if _, err := Migrate(ctx, db, nil); err != nil {
		t.Fatalf("migrate to head: %v", err)
	}

	createdAt1 := "2026-03-07T00:00:00Z"
	hash1 := computeEventHash(
		1, "issue", "mem-a111111", 1,
		"issue.created", `{"issue_id":"mem-a111111"}`,
		"test", "cmd-hash-prev-1", "", "", createdAt1, "", 1,
	)
	insertEventForVerifyTest(t, db, verifyTestEvent{
		EventID:             "evt_hash_prev_1",
		EventOrder:          1,
		EntityType:          "issue",
		EntityID:            "mem-a111111",
		EntitySeq:           1,
		EventType:           "issue.created",
		PayloadJSON:         `{"issue_id":"mem-a111111"}`,
		Actor:               "test",
		CommandID:           "cmd-hash-prev-1",
		CreatedAt:           createdAt1,
		Hash:                hash1,
		PrevHash:            "",
		EventPayloadVersion: 1,
	})

	createdAt2 := "2026-03-07T00:00:01Z"
	tamperedPrev := "tampered-prev-hash"
	hash2 := computeEventHash(
		2, "issue", "mem-a111111", 2,
		"issue.updated", `{"issue_id":"mem-a111111","status_to":"InProgress"}`,
		"test", "cmd-hash-prev-2", "", "", createdAt2, tamperedPrev, 1,
	)
	insertEventForVerifyTest(t, db, verifyTestEvent{
		EventID:             "evt_hash_prev_2",
		EventOrder:          2,
		EntityType:          "issue",
		EntityID:            "mem-a111111",
		EntitySeq:           2,
		EventType:           "issue.updated",
		PayloadJSON:         `{"issue_id":"mem-a111111","status_to":"InProgress"}`,
		Actor:               "test",
		CommandID:           "cmd-hash-prev-2",
		CreatedAt:           createdAt2,
		Hash:                hash2,
		PrevHash:            tamperedPrev,
		EventPayloadVersion: 1,
	})

	verify, err := Verify(ctx, db)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if verify.OK {
		t.Fatalf("expected verify to fail due to prev_hash mismatch")
	}
	if !containsCheck(verify.Checks, "prev_hash mismatch at event_order 2") {
		t.Fatalf("expected prev_hash mismatch check, got: %v", verify.Checks)
	}
}

func TestVerifyDetectsHashMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	if _, err := Migrate(ctx, db, nil); err != nil {
		t.Fatalf("migrate to head: %v", err)
	}

	insertEventForVerifyTest(t, db, verifyTestEvent{
		EventID:             "evt_hash_bad_1",
		EventOrder:          1,
		EntityType:          "issue",
		EntityID:            "mem-b222222",
		EntitySeq:           1,
		EventType:           "issue.created",
		PayloadJSON:         `{"issue_id":"mem-b222222"}`,
		Actor:               "test",
		CommandID:           "cmd-hash-bad-1",
		CreatedAt:           "2026-03-07T00:00:02Z",
		Hash:                "not-a-valid-chain-hash",
		PrevHash:            "",
		EventPayloadVersion: 1,
	})

	verify, err := Verify(ctx, db)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if verify.OK {
		t.Fatalf("expected verify to fail due to hash mismatch")
	}
	if !containsCheck(verify.Checks, "hash mismatch at event_order 1") {
		t.Fatalf("expected hash mismatch check, got: %v", verify.Checks)
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
