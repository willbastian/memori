package dbschema

import (
	"context"
	"testing"
)

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

func TestVerifyDetectsEntitySequenceMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	if _, err := Migrate(ctx, db, nil); err != nil {
		t.Fatalf("migrate to head: %v", err)
	}

	createdAt1 := "2026-03-07T00:00:00Z"
	hash1 := computeEventHash(
		1, "issue", "mem-seq111", 1,
		"issue.created", `{"issue_id":"mem-seq111"}`,
		"test", "cmd-entity-seq-1", "", "", createdAt1, "", 1,
	)
	insertEventForVerifyTest(t, db, verifyTestEvent{
		EventID:             "evt_entity_seq_1",
		EventOrder:          1,
		EntityType:          "issue",
		EntityID:            "mem-seq111",
		EntitySeq:           1,
		EventType:           "issue.created",
		PayloadJSON:         `{"issue_id":"mem-seq111"}`,
		Actor:               "test",
		CommandID:           "cmd-entity-seq-1",
		CreatedAt:           createdAt1,
		Hash:                hash1,
		PrevHash:            "",
		EventPayloadVersion: 1,
	})

	createdAt2 := "2026-03-07T00:00:01Z"
	hash2 := computeEventHash(
		2, "issue", "mem-seq111", 3,
		"issue.updated", `{"issue_id":"mem-seq111","status_to":"InProgress"}`,
		"test", "cmd-entity-seq-2", "", "", createdAt2, hash1, 1,
	)
	insertEventForVerifyTest(t, db, verifyTestEvent{
		EventID:             "evt_entity_seq_2",
		EventOrder:          2,
		EntityType:          "issue",
		EntityID:            "mem-seq111",
		EntitySeq:           3,
		EventType:           "issue.updated",
		PayloadJSON:         `{"issue_id":"mem-seq111","status_to":"InProgress"}`,
		Actor:               "test",
		CommandID:           "cmd-entity-seq-2",
		CreatedAt:           createdAt2,
		Hash:                hash2,
		PrevHash:            hash1,
		EventPayloadVersion: 1,
	})

	verify, err := Verify(ctx, db)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if verify.OK {
		t.Fatalf("expected verify to fail due to entity_seq mismatch")
	}
	if !containsCheck(verify.Checks, "entity_seq mismatch for issue:mem-seq111 at event_order 2: expected 2 got 3") {
		t.Fatalf("expected entity_seq mismatch check, got: %v", verify.Checks)
	}
}
