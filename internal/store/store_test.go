package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/willbastian/memori/internal/dbschema"
)

func TestCreateIssueIdempotencyByActorAndCommandID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	firstIssue, firstEvent, firstIdempotent, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-a1b2c3d",
		Type:      "task",
		Title:     "First title",
		Actor:     "agent-1",
		CommandID: "cmd-001",
	})
	if err != nil {
		t.Fatalf("first create issue: %v", err)
	}
	if firstIdempotent {
		t.Fatalf("first create should not be idempotent")
	}
	if firstEvent.EventOrder != 1 || firstEvent.EntitySeq != 1 {
		t.Fatalf("unexpected first event ordering: order=%d seq=%d", firstEvent.EventOrder, firstEvent.EntitySeq)
	}

	secondIssue, secondEvent, secondIdempotent, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-b1c2d3e",
		Type:      "task",
		Title:     "Second title should not apply",
		Actor:     "agent-1",
		CommandID: "cmd-001",
	})
	if err != nil {
		t.Fatalf("second create issue (idempotent retry): %v", err)
	}
	if !secondIdempotent {
		t.Fatalf("second create with same actor+command_id should be idempotent")
	}
	if secondEvent.EventID != firstEvent.EventID {
		t.Fatalf("expected same event id, got %s vs %s", secondEvent.EventID, firstEvent.EventID)
	}
	if secondIssue.ID != firstIssue.ID {
		t.Fatalf("expected same issue id on idempotent retry, got %s vs %s", secondIssue.ID, firstIssue.ID)
	}

	alphaEvents, err := s.ListEventsForEntity(ctx, "issue", "mem-a1b2c3d")
	if err != nil {
		t.Fatalf("list events for mem-a1b2c3d: %v", err)
	}
	if len(alphaEvents) != 1 {
		t.Fatalf("expected 1 event for mem-a1b2c3d, got %d", len(alphaEvents))
	}

	betaEvents, err := s.ListEventsForEntity(ctx, "issue", "mem-b1c2d3e")
	if err != nil {
		t.Fatalf("list events for mem-b1c2d3e: %v", err)
	}
	if len(betaEvents) != 0 {
		t.Fatalf("expected 0 events for mem-b1c2d3e, got %d", len(betaEvents))
	}

	storedIssue, err := s.GetIssue(ctx, "mem-a1b2c3d")
	if err != nil {
		t.Fatalf("get issue mem-a1b2c3d: %v", err)
	}
	if storedIssue.Title != "First title" {
		t.Fatalf("idempotent retry should not mutate title, got %q", storedIssue.Title)
	}
}

func TestMutatingCommandsRequireCommandID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID: "mem-a0a0a0a",
		Type:    "task",
		Title:   "Missing command id create",
		Actor:   "agent-1",
	})
	if err == nil || !strings.Contains(err.Error(), "--command-id is required") {
		t.Fatalf("expected missing command id error for create, got: %v", err)
	}

	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-a0a0a0b",
		Type:      "epic",
		Title:     "Parent",
		Actor:     "agent-1",
		CommandID: "cmd-required-1",
	})
	if err != nil {
		t.Fatalf("create parent issue: %v", err)
	}
	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-a0a0a0c",
		Type:      "story",
		Title:     "Child",
		Actor:     "agent-1",
		CommandID: "cmd-required-2",
	})
	if err != nil {
		t.Fatalf("create child issue: %v", err)
	}

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID: "mem-a0a0a0c",
		Status:  "inprogress",
		Actor:   "agent-1",
	})
	if err == nil || !strings.Contains(err.Error(), "--command-id is required") {
		t.Fatalf("expected missing command id error for update, got: %v", err)
	}

	_, _, _, err = s.LinkIssue(ctx, LinkIssueParams{
		ChildIssueID:  "mem-a0a0a0c",
		ParentIssueID: "mem-a0a0a0b",
		Actor:         "agent-1",
	})
	if err == nil || !strings.Contains(err.Error(), "--command-id is required") {
		t.Fatalf("expected missing command id error for link, got: %v", err)
	}
}

func TestEventOrderingAndEntitySequenceContracts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-1010101",
		Type:      "task",
		Title:     "A",
		Actor:     "agent-1",
		CommandID: "cmd-order-1",
	})
	if err != nil {
		t.Fatalf("create issue A: %v", err)
	}
	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-2020202",
		Type:      "task",
		Title:     "B",
		Actor:     "agent-1",
		CommandID: "cmd-order-2",
	})
	if err != nil {
		t.Fatalf("create issue B: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-1010101",
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-order-3",
	})
	if err != nil {
		t.Fatalf("update issue A status inprogress: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-2020202",
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-order-4",
	})
	if err != nil {
		t.Fatalf("update issue B status inprogress: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-1010101",
		Status:    "blocked",
		Actor:     "agent-1",
		CommandID: "cmd-order-5",
	})
	if err != nil {
		t.Fatalf("update issue A status blocked: %v", err)
	}

	rows, err := s.db.QueryContext(ctx, `SELECT event_order FROM events ORDER BY event_order ASC`)
	if err != nil {
		t.Fatalf("query event ordering: %v", err)
	}
	defer rows.Close()

	var (
		prevOrder int64
		count     int64
	)
	for rows.Next() {
		var order int64
		if err := rows.Scan(&order); err != nil {
			t.Fatalf("scan event_order: %v", err)
		}
		if order != prevOrder+1 {
			t.Fatalf("expected contiguous event_order sequence, prev=%d got=%d", prevOrder, order)
		}
		prevOrder = order
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate event ordering rows: %v", err)
	}
	if count != 5 {
		t.Fatalf("expected 5 events, got %d", count)
	}

	eventsA, err := s.ListEventsForEntity(ctx, "issue", "mem-1010101")
	if err != nil {
		t.Fatalf("list events for issue A: %v", err)
	}
	if len(eventsA) != 3 {
		t.Fatalf("expected 3 events for issue A, got %d", len(eventsA))
	}
	for i, event := range eventsA {
		if event.EntitySeq != int64(i+1) {
			t.Fatalf("issue A entity_seq mismatch at index %d: got %d", i, event.EntitySeq)
		}
	}

	eventsB, err := s.ListEventsForEntity(ctx, "issue", "mem-2020202")
	if err != nil {
		t.Fatalf("list events for issue B: %v", err)
	}
	if len(eventsB) != 2 {
		t.Fatalf("expected 2 events for issue B, got %d", len(eventsB))
	}
	for i, event := range eventsB {
		if event.EntitySeq != int64(i+1) {
			t.Fatalf("issue B entity_seq mismatch at index %d: got %d", i, event.EntitySeq)
		}
	}
}

func TestCommandIDScopedByActorAndMutationKind(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	first, _, firstIdempotent, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-a1a1a1a",
		Type:      "task",
		Title:     "First issue",
		Actor:     "agent-1",
		CommandID: "cmd-shared",
	})
	if err != nil {
		t.Fatalf("create first issue: %v", err)
	}
	if firstIdempotent {
		t.Fatalf("first create should not be idempotent")
	}

	second, _, secondIdempotent, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-b2b2b2b",
		Type:      "task",
		Title:     "Second issue",
		Actor:     "agent-2",
		CommandID: "cmd-shared",
	})
	if err != nil {
		t.Fatalf("create with same command id for different actor should succeed: %v", err)
	}
	if secondIdempotent {
		t.Fatalf("different actor should not be treated as idempotent replay")
	}
	if second.ID == first.ID {
		t.Fatalf("expected different issue ids for separate actor command")
	}

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   first.ID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-shared",
	})
	if err == nil || !strings.Contains(err.Error(), `command id already used by "issue.created"`) {
		t.Fatalf("expected mutation kind conflict for reused command id, got: %v", err)
	}
}

func TestEnumCheckConstraintsRejectInvalidDirectWrites(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO work_items(
			id, type, title, parent_id, status, labels_json,
			current_cycle_no, active_gate_set_id, created_at, updated_at, last_event_id
		) VALUES(?, ?, ?, NULL, ?, '[]', 1, NULL, ?, ?, ?)
	`, "mem-c3c3c3c", "Feature", "invalid type", "Todo", nowUTC(), nowUTC(), "evt_test_work_item")
	if err == nil {
		t.Fatalf("expected invalid work item type to fail check constraint")
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO events(
			event_id, event_order, entity_type, entity_id, entity_seq, event_type,
			payload_json, actor, command_id, causation_id, correlation_id, created_at,
			hash, prev_hash, event_payload_version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL, ?)
	`, "evt_test_entity_type", 1, "unknown", "mem-d4d4d4d", 1, "issue.created", "{}", "agent-1", "cmd-enum-1", nowUTC(), "hash_test_1", 1)
	if err == nil {
		t.Fatalf("expected invalid events.entity_type to fail check constraint")
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO events(
			event_id, event_order, entity_type, entity_id, entity_seq, event_type,
			payload_json, actor, command_id, causation_id, correlation_id, created_at,
			hash, prev_hash, event_payload_version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL, ?)
	`, "evt_test_event_type", 2, "issue", "mem-e5e5e5e", 1, "issue.closed", "{}", "agent-1", "cmd-enum-2", nowUTC(), "hash_test_2", 1)
	if err == nil {
		t.Fatalf("expected invalid events.event_type to fail check constraint")
	}
}

func TestReplayProjectionsDeterministicAcrossRuns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-a1b2c3d",
		Type:      "epic",
		Title:     "Epic one",
		Actor:     "agent-1",
		CommandID: "cmd-epic-1",
	})
	if err != nil {
		t.Fatalf("create epic_1: %v", err)
	}
	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-b2c3d4e",
		Type:      "story",
		Title:     "Story one",
		ParentID:  "mem-a1b2c3d",
		Actor:     "agent-1",
		CommandID: "cmd-task-1",
	})
	if err != nil {
		t.Fatalf("create mem-b2c3d4e story: %v", err)
	}

	beforeEpic, err := s.GetIssue(ctx, "mem-a1b2c3d")
	if err != nil {
		t.Fatalf("get mem-a1b2c3d before replay: %v", err)
	}
	beforeTask, err := s.GetIssue(ctx, "mem-b2c3d4e")
	if err != nil {
		t.Fatalf("get mem-b2c3d4e before replay: %v", err)
	}

	replay1, err := s.ReplayProjections(ctx)
	if err != nil {
		t.Fatalf("first replay: %v", err)
	}
	if replay1.EventsApplied != 2 {
		t.Fatalf("expected first replay to apply 2 events, got %d", replay1.EventsApplied)
	}

	afterFirstEpic, err := s.GetIssue(ctx, "mem-a1b2c3d")
	if err != nil {
		t.Fatalf("get mem-a1b2c3d after first replay: %v", err)
	}
	afterFirstTask, err := s.GetIssue(ctx, "mem-b2c3d4e")
	if err != nil {
		t.Fatalf("get mem-b2c3d4e after first replay: %v", err)
	}
	assertIssueEqual(t, beforeEpic, afterFirstEpic)
	assertIssueEqual(t, beforeTask, afterFirstTask)

	replay2, err := s.ReplayProjections(ctx)
	if err != nil {
		t.Fatalf("second replay: %v", err)
	}
	if replay2.EventsApplied != 2 {
		t.Fatalf("expected second replay to apply 2 events, got %d", replay2.EventsApplied)
	}

	afterSecondEpic, err := s.GetIssue(ctx, "mem-a1b2c3d")
	if err != nil {
		t.Fatalf("get mem-a1b2c3d after second replay: %v", err)
	}
	afterSecondTask, err := s.GetIssue(ctx, "mem-b2c3d4e")
	if err != nil {
		t.Fatalf("get mem-b2c3d4e after second replay: %v", err)
	}
	assertIssueEqual(t, afterFirstEpic, afterSecondEpic)
	assertIssueEqual(t, afterFirstTask, afterSecondTask)
}

func TestReplayProjectionsSurfacesMalformedEventPayloads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-f1f1f1f",
		Type:      "task",
		Title:     "Replay corruption check",
		Actor:     "agent-1",
		CommandID: "cmd-replay-malformed-1",
	}); err != nil {
		t.Fatalf("create issue for replay corruption test: %v", err)
	}

	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO events(
			event_id, event_order, entity_type, entity_id, entity_seq, event_type,
			payload_json, actor, command_id, causation_id, correlation_id, created_at,
			hash, prev_hash, event_payload_version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL, ?)
	`, "evt_replay_bad_payload", 2, "issue", "mem-f2f2f2f", 1, "issue.created", `{"issue_id":"mem-f2f2f2f","type":"Feature","title":"Bad replay type","status":"Todo","created_at":"2026-03-08T00:00:00Z"}`, "agent-1", "cmd-replay-malformed-2", nowUTC(), "hash_replay_bad_payload", 1); err != nil {
		t.Fatalf("insert invalid projected issue.created event: %v", err)
	}

	if _, err := s.ReplayProjections(ctx); err == nil || !strings.Contains(err.Error(), "upsert work_item from event") {
		t.Fatalf("expected replay projection failure, got %v", err)
	}
}

func TestReplayProjectionsFailsOnClosedDB(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	if _, err := s.ReplayProjections(ctx); err == nil || !strings.Contains(err.Error(), "begin tx") {
		t.Fatalf("expected replay closed-db error, got %v", err)
	}
}

func TestEventsTableAppendOnlyTriggersBlockMutation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, event, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-c3d4e5f",
		Type:      "task",
		Title:     "Trigger test",
		Actor:     "agent-1",
		CommandID: "cmd-trigger",
	})
	if err != nil {
		t.Fatalf("create mem-c3d4e5f: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `UPDATE events SET event_type = 'issue.updated' WHERE event_id = ?`, event.EventID)
	if err == nil {
		t.Fatalf("expected update on events to fail due to append-only trigger")
	}
	if !strings.Contains(err.Error(), "events are append-only") {
		t.Fatalf("expected append-only trigger error, got: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `DELETE FROM events WHERE event_id = ?`, event.EventID)
	if err == nil {
		t.Fatalf("expected delete on events to fail due to append-only trigger")
	}
	if !strings.Contains(err.Error(), "events are append-only") {
		t.Fatalf("expected append-only trigger error on delete, got: %v", err)
	}
}

func TestGateTemplatesImmutabilityTriggersBlockMutation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_templates(
			template_id, version, applies_to_json, definition_json,
			definition_hash, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?)
	`, "tmpl-default", 1, `["Task"]`, `{"gates":[{"id":"build"}]}`, "tmplhash1", nowUTC(), "agent-1")
	if err != nil {
		t.Fatalf("insert gate template: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE gate_templates SET definition_json = ? WHERE template_id = ? AND version = ?
	`, `{"gates":[{"id":"lint"}]}`, "tmpl-default", 1)
	if err == nil {
		t.Fatalf("expected update on gate_templates to fail due to immutability trigger")
	}
	if !strings.Contains(err.Error(), "gate_templates are immutable") {
		t.Fatalf("expected gate template immutability error, got: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		DELETE FROM gate_templates WHERE template_id = ? AND version = ?
	`, "tmpl-default", 1)
	if err == nil {
		t.Fatalf("expected delete on gate_templates to fail due to immutability trigger")
	}
	if !strings.Contains(err.Error(), "gate_templates are immutable") {
		t.Fatalf("expected gate template delete immutability error, got: %v", err)
	}
}

func TestGateSetItemsImmutabilityTriggersBlockMutation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-1212121",
		Type:      "task",
		Title:     "Gate item trigger test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-item-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, NULL, ?, ?)
	`, "gs_1", "mem-1212121", 1, `["tmpl-default@1"]`, `{"gates":[{"id":"build"}]}`, "gshash1", nowUTC(), "agent-1")
	if err != nil {
		t.Fatalf("insert gate set: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES(?, ?, ?, ?, ?)
	`, "gs_1", "build", "check", 1, `{"command":"go test ./..."}`)
	if err != nil {
		t.Fatalf("insert gate set item: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE gate_set_items SET required = 0 WHERE gate_set_id = ? AND gate_id = ?
	`, "gs_1", "build")
	if err == nil {
		t.Fatalf("expected update on gate_set_items to fail due to immutability trigger")
	}
	if !strings.Contains(err.Error(), "gate_set_items are immutable") {
		t.Fatalf("expected gate_set_items immutability error, got: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		DELETE FROM gate_set_items WHERE gate_set_id = ? AND gate_id = ?
	`, "gs_1", "build")
	if err == nil {
		t.Fatalf("expected delete on gate_set_items to fail due to immutability trigger")
	}
	if !strings.Contains(err.Error(), "gate_set_items are immutable") {
		t.Fatalf("expected gate_set_items delete immutability error, got: %v", err)
	}
}

func TestGateSetsImmutabilityTriggersEnforceFrozenFieldsAndLocking(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-2323232",
		Type:      "task",
		Title:     "Gate set trigger test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-set-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	createdAt := nowUTC()
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, NULL, ?, ?)
	`, "gs_2", "mem-2323232", 1, `["tmpl-default@1"]`, `{"gates":[{"id":"build"}]}`, "gshash2", createdAt, "agent-1")
	if err != nil {
		t.Fatalf("insert gate set: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE gate_sets SET template_refs_json = ? WHERE gate_set_id = ?
	`, `["tmpl-default@2"]`, "gs_2")
	if err == nil {
		t.Fatalf("expected frozen field update to fail")
	}
	if !strings.Contains(err.Error(), "gate_set definitions are immutable") {
		t.Fatalf("expected frozen definition immutability error, got: %v", err)
	}

	lockedAt := nowUTC()
	_, err = s.db.ExecContext(ctx, `
		UPDATE gate_sets SET locked_at = ? WHERE gate_set_id = ?
	`, lockedAt, "gs_2")
	if err != nil {
		t.Fatalf("lock gate set (set locked_at once): %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE gate_sets SET created_by = ? WHERE gate_set_id = ?
	`, "agent-2", "gs_2")
	if err == nil {
		t.Fatalf("expected update on locked gate_set to fail")
	}
	if !strings.Contains(err.Error(), "locked gate_sets are immutable") {
		t.Fatalf("expected locked gate_set immutability error, got: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE gate_sets SET locked_at = ? WHERE gate_set_id = ?
	`, nowUTC(), "gs_2")
	if err == nil {
		t.Fatalf("expected second lock attempt to fail")
	}
	if !strings.Contains(err.Error(), "gate_set is already locked") {
		t.Fatalf("expected clear lock no-op rejection error, got: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		DELETE FROM gate_sets WHERE gate_set_id = ?
	`, "gs_2")
	if err == nil {
		t.Fatalf("expected delete on gate_sets to fail due to immutability trigger")
	}
	if !strings.Contains(err.Error(), "gate_sets are immutable") {
		t.Fatalf("expected gate_sets delete immutability error, got: %v", err)
	}
}

func TestListIssuesFiltersByTypeStatusAndParent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-c3d4e5f",
		Type:      "epic",
		Title:     "Backlog epic",
		Actor:     "agent-1",
		CommandID: "cmd-backlog-1",
	})
	if err != nil {
		t.Fatalf("create epic_backlog: %v", err)
	}
	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-d4e5f6a",
		Type:      "story",
		Title:     "Backlog story",
		ParentID:  "mem-c3d4e5f",
		Actor:     "agent-1",
		CommandID: "cmd-backlog-2",
	})
	if err != nil {
		t.Fatalf("create story_backlog: %v", err)
	}
	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-e5f6a7b",
		Type:      "task",
		Title:     "Backlog task",
		ParentID:  "mem-d4e5f6a",
		Actor:     "agent-1",
		CommandID: "cmd-backlog-3",
	})
	if err != nil {
		t.Fatalf("create mem-e5f6a7b: %v", err)
	}

	all, err := s.ListIssues(ctx, ListIssuesParams{})
	if err != nil {
		t.Fatalf("list all issues: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 issues in backlog, got %d", len(all))
	}

	tasks, err := s.ListIssues(ctx, ListIssuesParams{Type: "task"})
	if err != nil {
		t.Fatalf("list task issues: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ID != "mem-e5f6a7b" {
		t.Fatalf("expected only mem-e5f6a7b, got %#v", tasks)
	}

	todo, err := s.ListIssues(ctx, ListIssuesParams{Status: "todo"})
	if err != nil {
		t.Fatalf("list todo issues: %v", err)
	}
	if len(todo) != 3 {
		t.Fatalf("expected 3 todo issues, got %d", len(todo))
	}

	children, err := s.ListIssues(ctx, ListIssuesParams{ParentID: "mem-c3d4e5f"})
	if err != nil {
		t.Fatalf("list children by parent: %v", err)
	}
	if len(children) != 1 || children[0].ID != "mem-d4e5f6a" {
		t.Fatalf("expected only mem-d4e5f6a child, got %#v", children)
	}

	if _, err := s.ListIssues(ctx, ListIssuesParams{Type: "invalid"}); err == nil {
		t.Fatalf("expected invalid type filter to fail")
	}
	if _, err := s.ListIssues(ctx, ListIssuesParams{Status: "notastatus"}); err == nil {
		t.Fatalf("expected invalid status filter to fail")
	}
}

func TestListPendingExecutableGateTemplates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	pending, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "pending-exec",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`,
		Actor:          "llm:openai:gpt-5",
		CommandID:      "cmd-pending-template-1",
	})
	if err != nil {
		t.Fatalf("create pending executable template: %v", err)
	}

	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "approved-exec",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"lint","criteria":{"command":"go test ./internal/store"}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-approved-template-1",
	}); err != nil {
		t.Fatalf("create approved executable template: %v", err)
	}

	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "manual-check",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"review","criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "llm:openai:gpt-5",
		CommandID:      "cmd-manual-template-1",
	}); err != nil {
		t.Fatalf("create non-executable template: %v", err)
	}

	templates, err := s.ListPendingExecutableGateTemplates(ctx)
	if err != nil {
		t.Fatalf("list pending executable gate templates: %v", err)
	}
	if len(templates) != 1 {
		t.Fatalf("expected 1 pending executable template, got %d", len(templates))
	}

	got := templates[0]
	if got.TemplateID != pending.TemplateID || got.Version != pending.Version {
		t.Fatalf("expected pending template %s@%d, got %s@%d", pending.TemplateID, pending.Version, got.TemplateID, got.Version)
	}
	if !got.Executable {
		t.Fatalf("expected pending template to be executable")
	}
	if got.ApprovedBy != "" || got.ApprovedAt != "" {
		t.Fatalf("expected pending template approval fields to be empty, got approved_by=%q approved_at=%q", got.ApprovedBy, got.ApprovedAt)
	}
	if got.DefinitionHash == "" || got.CreatedBy == "" || got.CreatedAt == "" {
		t.Fatalf("expected pending template provenance metadata, got %+v", got)
	}
}

func TestApproveGateTemplateRemovesTemplateFromPendingList(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "agent-authored",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`,
		Actor:          "llm:openai:gpt-5",
		CommandID:      "cmd-pending-approve-create-1",
	}); err != nil {
		t.Fatalf("create pending executable template: %v", err)
	}

	pending, err := s.ListPendingExecutableGateTemplates(ctx)
	if err != nil {
		t.Fatalf("list pending executable templates before approval: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending executable template before approval, got %d", len(pending))
	}

	approved, _, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: "agent-authored",
		Version:    1,
		Actor:      "human:alice",
		CommandID:  "cmd-pending-approve-human-1",
	})
	if err != nil {
		t.Fatalf("approve executable template: %v", err)
	}
	if approved.ApprovedBy != "human:alice" {
		t.Fatalf("expected approval actor recorded, got %q", approved.ApprovedBy)
	}

	pending, err = s.ListPendingExecutableGateTemplates(ctx)
	if err != nil {
		t.Fatalf("list pending executable templates after approval: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending executable templates after approval, got %d", len(pending))
	}
}

func TestCreateIssueKeyPolicyValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "task-a1b2c3d",
		Type:      "task",
		Title:     "Invalid key prefix",
		Actor:     "agent-1",
		CommandID: "cmd-key-1",
	})
	if err == nil || !strings.Contains(err.Error(), "type must be in --type") {
		t.Fatalf("expected type-in-prefix validation error, got: %v", err)
	}

	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-nothexx",
		Type:      "task",
		Title:     "Invalid short sha",
		Actor:     "agent-1",
		CommandID: "cmd-key-2",
	})
	if err == nil || !strings.Contains(err.Error(), "shortSHA must be hex") {
		t.Fatalf("expected shortSHA validation error, got: %v", err)
	}
}

func TestCreateIssueGeneratedKeysFollowPrefixShortSHAPattern(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStoreWithPrefix(t, "ops")

	issue, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		Type:      "task",
		Title:     "Generated key",
		Actor:     "agent-1",
		CommandID: "cmd-key-3",
	})
	if err != nil {
		t.Fatalf("create with generated key: %v", err)
	}

	pattern := regexp.MustCompile(`^ops-[0-9a-f]{7}$`)
	if !pattern.MatchString(issue.ID) {
		t.Fatalf("expected generated key to match ops-shortSHA, got %q", issue.ID)
	}
}

func TestCreateIssuePersistsRichContextFields(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	created, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:            "mem-0f0f0f0",
		Type:               "task",
		Title:              "Rich context",
		Description:        "Implement richer ticket context rendering",
		AcceptanceCriteria: "Shows description and acceptance criteria in issue show",
		References:         []string{"https://example.com/spec", " https://example.com/spec ", "notes.md"},
		Actor:              "agent-1",
		CommandID:          "cmd-rich-create-1",
	})
	if err != nil {
		t.Fatalf("create issue with rich context: %v", err)
	}

	if created.Description != "Implement richer ticket context rendering" {
		t.Fatalf("unexpected description: %q", created.Description)
	}
	if created.Acceptance != "Shows description and acceptance criteria in issue show" {
		t.Fatalf("unexpected acceptance criteria: %q", created.Acceptance)
	}
	expectedRefs := []string{"https://example.com/spec", "notes.md"}
	if !reflect.DeepEqual(created.References, expectedRefs) {
		t.Fatalf("unexpected references: %#v", created.References)
	}
}

func TestUpdateIssueAllowsContextOnlyMutation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-1f1f1f1",
		Type:      "task",
		Title:     "Context updates",
		Actor:     "agent-1",
		CommandID: "cmd-rich-update-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	title := "Context updates renamed"
	description := "Track rich context as first-class metadata"
	acceptance := "Issue show surfaces context fields"
	references := []string{"https://example.com/rfc", "adr-001.md"}
	updated, event, idempotent, err := s.UpdateIssue(ctx, UpdateIssueParams{
		IssueID:            "mem-1f1f1f1",
		Title:              &title,
		Description:        &description,
		AcceptanceCriteria: &acceptance,
		References:         &references,
		Actor:              "agent-1",
		CommandID:          "cmd-rich-update-1",
	})
	if err != nil {
		t.Fatalf("update issue context: %v", err)
	}
	if idempotent {
		t.Fatalf("first context update should not be idempotent")
	}
	if event.EventType != "issue.updated" {
		t.Fatalf("expected issue.updated event, got %s", event.EventType)
	}
	if updated.Status != "Todo" {
		t.Fatalf("status should remain Todo when only context changes, got %s", updated.Status)
	}
	if updated.Title != title {
		t.Fatalf("unexpected title: %q", updated.Title)
	}
	if updated.Description != description {
		t.Fatalf("unexpected description: %q", updated.Description)
	}
	if updated.Acceptance != acceptance {
		t.Fatalf("unexpected acceptance criteria: %q", updated.Acceptance)
	}
	if !reflect.DeepEqual(updated.References, references) {
		t.Fatalf("unexpected references: %#v", updated.References)
	}
}

func TestUpdateIssueRejectsBlankTitle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-2f2f2f2",
		Type:      "task",
		Title:     "Valid title",
		Actor:     "agent-1",
		CommandID: "cmd-blank-title-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	blank := "   "
	if _, _, _, err := s.UpdateIssue(ctx, UpdateIssueParams{
		IssueID:   "mem-2f2f2f2",
		Title:     &blank,
		Actor:     "agent-1",
		CommandID: "cmd-blank-title-update-1",
	}); err == nil || !strings.Contains(err.Error(), "--title is required") {
		t.Fatalf("expected blank title validation error, got: %v", err)
	}
}

func TestUpdateIssueStatusValidTransitionsAndIdempotency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	created, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-1111111",
		Type:      "task",
		Title:     "Status transition test",
		Actor:     "agent-1",
		CommandID: "cmd-update-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if created.Status != "Todo" {
		t.Fatalf("expected initial status Todo, got %s", created.Status)
	}

	updated, event, idempotent, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-1111111",
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-update-1",
	})
	if err != nil {
		t.Fatalf("update issue status: %v", err)
	}
	if idempotent {
		t.Fatalf("first update should not be idempotent")
	}
	if updated.Status != "InProgress" {
		t.Fatalf("expected status InProgress, got %s", updated.Status)
	}
	if event.EventType != "issue.updated" {
		t.Fatalf("expected issue.updated event, got %s", event.EventType)
	}

	retryIssue, retryEvent, retryIdempotent, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-1111111",
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-update-1",
	})
	if err != nil {
		t.Fatalf("retry update with same command id should succeed idempotently: %v", err)
	}
	if !retryIdempotent {
		t.Fatalf("expected idempotent retry")
	}
	if retryEvent.EventID != event.EventID {
		t.Fatalf("expected same event id on retry, got %s vs %s", retryEvent.EventID, event.EventID)
	}
	if retryIssue.Status != "InProgress" {
		t.Fatalf("expected status to remain InProgress on idempotent retry, got %s", retryIssue.Status)
	}
}

func TestConcurrentCreateIssuePreservesEventOrdering(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	const writers = 12
	start := make(chan struct{})
	errCh := make(chan error, writers)
	var wg sync.WaitGroup

	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
				IssueID:   fmt.Sprintf("mem-a%06x", i),
				Type:      "task",
				Title:     fmt.Sprintf("Concurrent %d", i),
				Actor:     "agent-1",
				CommandID: fmt.Sprintf("cmd-concur-create-%03d", i),
			})
			errCh <- err
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent create issue failed: %v", err)
		}
	}

	rows, err := s.db.QueryContext(ctx, `SELECT event_order FROM events ORDER BY event_order ASC`)
	if err != nil {
		t.Fatalf("query event ordering after concurrency: %v", err)
	}
	defer rows.Close()

	var (
		prevOrder int64
		count     int64
	)
	for rows.Next() {
		var order int64
		if err := rows.Scan(&order); err != nil {
			t.Fatalf("scan concurrent event_order: %v", err)
		}
		if order != prevOrder+1 {
			t.Fatalf("expected contiguous event_order under concurrency, prev=%d got=%d", prevOrder, order)
		}
		prevOrder = order
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate concurrent event_order rows: %v", err)
	}
	if count != writers {
		t.Fatalf("expected %d events, got %d", writers, count)
	}
}

func TestConcurrentDuplicateCreateIssueCommandIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	const callers = 8
	start := make(chan struct{})
	errCh := make(chan error, callers)
	idempotentCh := make(chan bool, callers)
	var wg sync.WaitGroup

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, idempotent, err := s.CreateIssue(ctx, CreateIssueParams{
				IssueID:   "mem-a1d0e0f",
				Type:      "task",
				Title:     "Concurrent idempotent create",
				Actor:     "agent-1",
				CommandID: "cmd-concur-idem-1",
			})
			errCh <- err
			idempotentCh <- idempotent
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)
	close(idempotentCh)

	idempotentCount := 0
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent idempotent create failed: %v", err)
		}
	}
	for idempotent := range idempotentCh {
		if idempotent {
			idempotentCount++
		}
	}
	if idempotentCount == 0 {
		t.Fatalf("expected at least one concurrent duplicate create to return idempotent=true")
	}

	var eventCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM events WHERE entity_id = ?`, "mem-a1d0e0f").Scan(&eventCount); err != nil {
		t.Fatalf("count idempotent concurrent events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("expected one event for concurrent duplicate command, got %d", eventCount)
	}
}

func TestReplayProjectionsAppliesIssueUpdatedEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-3333333",
		Type:      "task",
		Title:     "Replay update test",
		Actor:     "agent-1",
		CommandID: "cmd-update-create-3",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-3333333",
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-update-6",
	})
	if err != nil {
		t.Fatalf("update status before replay: %v", err)
	}

	replay, err := s.ReplayProjections(ctx)
	if err != nil {
		t.Fatalf("replay projections: %v", err)
	}
	if replay.EventsApplied != 2 {
		t.Fatalf("expected 2 events applied in replay, got %d", replay.EventsApplied)
	}

	issue, err := s.GetIssue(ctx, "mem-3333333")
	if err != nil {
		t.Fatalf("get issue after replay: %v", err)
	}
	if issue.Status != "InProgress" {
		t.Fatalf("expected replayed status InProgress, got %s", issue.Status)
	}
}

func TestInitializeMatchesMigratedSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	initStore := newTestStore(t)
	initSchema := sqliteSchemaObjectsForTest(t, initStore.DB())

	migratedPath := filepath.Join(t.TempDir(), "memori-migrated.db")
	migratedStore, err := Open(migratedPath)
	if err != nil {
		t.Fatalf("open migrated store: %v", err)
	}
	t.Cleanup(func() {
		_ = migratedStore.Close()
	})
	if _, err := dbschema.Migrate(ctx, migratedStore.DB(), nil); err != nil {
		t.Fatalf("migrate schema: %v", err)
	}
	migratedSchema := sqliteSchemaObjectsForTest(t, migratedStore.DB())

	if !reflect.DeepEqual(initSchema, migratedSchema) {
		t.Fatalf("expected Initialize schema to match migrated schema\ninit=%v\nmigrate=%v", initSchema, migratedSchema)
	}
}

func newTestStore(t *testing.T) *Store {
	return newTestStoreWithPrefix(t, DefaultIssueKeyPrefix)
}

func newTestStoreWithPrefix(t *testing.T, prefix string) *Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "memori-test.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open test store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	if err := s.Initialize(context.Background(), InitializeParams{IssueKeyPrefix: prefix}); err != nil {
		t.Fatalf("initialize test store: %v", err)
	}

	return s
}

func seedLockedGateSetForTest(t *testing.T, s *Store, issueID, gateSetID string) {
	t.Helper()

	seedLockedGateSetWithProvenanceForTest(t, s, issueID, gateSetID, `["tmpl-default@1"]`, `{"gates":[{"id":"build"}]}`, "agent-1")
}

func seedLockedGateSetWithProvenanceForTest(t *testing.T, s *Store, issueID, gateSetID, templateRefsJSON, frozenDefinitionJSON, createdBy string) {
	t.Helper()

	ctx := context.Background()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, gateSetID, issueID, 1, templateRefsJSON, frozenDefinitionJSON, gateSetID+"_hash", nowUTC(), nowUTC(), createdBy)
	if err != nil {
		t.Fatalf("insert locked gate set %s: %v", gateSetID, err)
	}
}

func seedGateTemplateRowForTest(t *testing.T, s *Store, templateID string, version int, appliesTo []string, definitionJSON, createdBy string) {
	t.Helper()

	ctx := context.Background()
	canonicalDefinition, definitionHash, err := canonicalizeGateDefinition(definitionJSON)
	if err != nil {
		t.Fatalf("canonicalize gate template %s@%d: %v", templateID, version, err)
	}
	appliesToJSON, err := json.Marshal(appliesTo)
	if err != nil {
		t.Fatalf("marshal applies_to for gate template %s@%d: %v", templateID, version, err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_templates(
			template_id, version, applies_to_json, definition_json,
			definition_hash, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?)
	`, templateID, version, string(appliesToJSON), canonicalDefinition, definitionHash, nowUTC(), createdBy); err != nil {
		t.Fatalf("insert gate template %s@%d: %v", templateID, version, err)
	}
	if gateDefinitionContainsExecutableCommand(canonicalDefinition) && actorIsHumanGoverned(createdBy) {
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO gate_template_approvals(template_id, version, approved_at, approved_by)
			VALUES(?, ?, ?, ?)
		`, templateID, version, nowUTC(), createdBy); err != nil {
			t.Fatalf("insert gate template approval %s@%d: %v", templateID, version, err)
		}
	}
}

func createLockedGateSetEventSourcedForTest(t *testing.T, s *Store, issueID, templateID, gateID, commandPrefix string) string {
	t.Helper()

	ctx := context.Background()
	definition := fmt.Sprintf(`{"gates":[{"id":%q,"kind":"check","required":true,"criteria":{"command":"echo verified"}}]}`, gateID)
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     templateID,
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: definition,
		Actor:          "human:alice",
		CommandID:      commandPrefix + "-template-1",
	}); err != nil {
		t.Fatalf("create event-sourced gate template %s@1: %v", templateID, err)
	}
	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{templateID + "@1"},
		Actor:        "agent-1",
		CommandID:    commandPrefix + "-instantiate-1",
	})
	if err != nil {
		t.Fatalf("instantiate event-sourced gate set for %s: %v", issueID, err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: commandPrefix + "-lock-1",
	}); err != nil {
		t.Fatalf("lock event-sourced gate set for %s: %v", issueID, err)
	}
	return gateSet.GateSetID
}

func sqliteSchemaObjectsForTest(t *testing.T, db *sql.DB) []string {
	t.Helper()

	rows, err := db.Query(`
		SELECT type || ':' || name
		FROM sqlite_master
		WHERE type IN ('table', 'index', 'trigger')
			AND name NOT LIKE 'sqlite_%'
		ORDER BY type ASC, name ASC
	`)
	if err != nil {
		t.Fatalf("query sqlite schema objects: %v", err)
	}
	defer rows.Close()

	items := make([]string, 0)
	for rows.Next() {
		var item string
		if err := rows.Scan(&item); err != nil {
			t.Fatalf("scan sqlite schema object: %v", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate sqlite schema objects: %v", err)
	}
	return items
}

func seedGateSetItemForTest(t *testing.T, s *Store, gateSetID, gateID, kind string, required int) {
	t.Helper()

	ctx := context.Background()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES(?, ?, ?, ?, ?)
	`, gateSetID, gateID, kind, required, `{"ref":"test"}`)
	if err != nil {
		t.Fatalf("insert gate_set_item %s/%s: %v", gateSetID, gateID, err)
	}
}

func appendGateEvaluationEventForTest(
	t *testing.T,
	s *Store,
	issueID, gateSetID, gateID, result, actor, commandID string,
) {
	t.Helper()

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for gate evaluation event: %v", err)
	}
	defer tx.Rollback()

	var gateSetHash string
	if err := tx.QueryRowContext(ctx, `SELECT gate_set_hash FROM gate_sets WHERE gate_set_id = ?`, gateSetID).Scan(&gateSetHash); err != nil {
		t.Fatalf("lookup gate_set_hash for %s: %v", gateSetID, err)
	}

	payloadJSON := fmt.Sprintf(
		`{"issue_id":%q,"gate_set_id":%q,"gate_id":%q,"result":%q,"evidence_refs":["test://evidence"],"proof":{"verifier":"test-verifier","runner":"unit-test","runner_version":"1","exit_code":0,"gate_set_hash":%q},"evaluated_at":%q}`,
		issueID, gateSetID, gateID, result, gateSetHash, nowUTC(),
	)
	res, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeIssue,
		EntityID:            issueID,
		EventType:           eventTypeGateEval,
		PayloadJSON:         payloadJSON,
		Actor:               actor,
		CommandID:           commandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		t.Fatalf("append gate evaluation event: %v", err)
	}
	if res.AlreadyExists {
		t.Fatalf("expected non-idempotent append for unique command_id %q", commandID)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit gate evaluation event: %v", err)
	}
}

func appendGateEvaluationEventWithoutEvidenceForTest(
	t *testing.T,
	s *Store,
	issueID, gateSetID, gateID, result, actor, commandID string,
) {
	t.Helper()

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for gate evaluation event: %v", err)
	}
	defer tx.Rollback()

	payloadJSON := fmt.Sprintf(
		`{"issue_id":%q,"gate_set_id":%q,"gate_id":%q,"result":%q,"evaluated_at":%q}`,
		issueID, gateSetID, gateID, result, nowUTC(),
	)
	res, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeIssue,
		EntityID:            issueID,
		EventType:           eventTypeGateEval,
		PayloadJSON:         payloadJSON,
		Actor:               actor,
		CommandID:           commandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		t.Fatalf("append gate evaluation event: %v", err)
	}
	if res.AlreadyExists {
		t.Fatalf("expected non-idempotent append for unique command_id %q", commandID)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit gate evaluation event: %v", err)
	}
}

func appendGateEvaluationEventWithEvidenceNoProofForTest(
	t *testing.T,
	s *Store,
	issueID, gateSetID, gateID, result, actor, commandID string,
) {
	t.Helper()

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for gate evaluation event: %v", err)
	}
	defer tx.Rollback()

	payloadJSON := fmt.Sprintf(
		`{"issue_id":%q,"gate_set_id":%q,"gate_id":%q,"result":%q,"evidence_refs":["test://evidence"],"evaluated_at":%q}`,
		issueID, gateSetID, gateID, result, nowUTC(),
	)
	res, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeIssue,
		EntityID:            issueID,
		EventType:           eventTypeGateEval,
		PayloadJSON:         payloadJSON,
		Actor:               actor,
		CommandID:           commandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		t.Fatalf("append gate evaluation event: %v", err)
	}
	if res.AlreadyExists {
		t.Fatalf("expected non-idempotent append for unique command_id %q", commandID)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit gate evaluation event: %v", err)
	}
}

func assertIssueEqual(t *testing.T, expected, actual Issue) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("issue mismatch\nexpected: %#v\nactual:   %#v", expected, actual)
	}
}
