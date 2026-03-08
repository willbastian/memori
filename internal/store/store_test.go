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

	"memori/internal/dbschema"
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

func TestCreateGateTemplateVersioningAndListing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	firstDef := `{"gates":[{"id":"build","required":true}]}`
	created, idempotent, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "quality",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: firstDef,
		Actor:          "agent-1",
		CommandID:      "cmd-template-create-1",
	})
	if err != nil {
		t.Fatalf("create gate template v1: %v", err)
	}
	if idempotent {
		t.Fatalf("expected first template create to be non-idempotent")
	}
	if created.TemplateID != "quality" || created.Version != 1 {
		t.Fatalf("unexpected created template identity: %#v", created)
	}

	same, sameIdempotent, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "quality",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: firstDef,
		Actor:          "agent-1",
		CommandID:      "cmd-template-create-1",
	})
	if err != nil {
		t.Fatalf("idempotent template create retry: %v", err)
	}
	if !sameIdempotent {
		t.Fatalf("expected same template create to be idempotent")
	}
	if same.DefinitionHash != created.DefinitionHash {
		t.Fatalf("expected same hash on idempotent retry, got %s vs %s", same.DefinitionHash, created.DefinitionHash)
	}

	_, _, err = s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "quality",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","required":false}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-template-create-2",
	})
	if err == nil || !strings.Contains(err.Error(), "create a new version") {
		t.Fatalf("expected same-version mutation rejection, got: %v", err)
	}

	_, idempotent, err = s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "quality",
		Version:        2,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","required":false}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-template-create-3",
	})
	if err != nil {
		t.Fatalf("create gate template v2: %v", err)
	}
	if idempotent {
		t.Fatalf("expected v2 create to be non-idempotent")
	}

	templates, err := s.ListGateTemplates(ctx, ListGateTemplatesParams{IssueType: "task"})
	if err != nil {
		t.Fatalf("list gate templates by type: %v", err)
	}
	if len(templates) != 2 {
		t.Fatalf("expected 2 task templates, got %d", len(templates))
	}
}

func TestInstantiateAndLockGateSetFlow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-7878787"
	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Instantiate lock flow",
		Actor:     "agent-1",
		CommandID: "cmd-gset-flow-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	_, _, err = s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "quality",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}},{"id":"lint","kind":"check","required":false}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-gset-template-create-1",
	})
	if err != nil {
		t.Fatalf("create gate template: %v", err)
	}

	gateSet, idempotent, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"quality@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gset-instantiate-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if idempotent {
		t.Fatalf("expected first instantiate to be non-idempotent")
	}
	if gateSet.CycleNo != 1 {
		t.Fatalf("expected cycle 1, got %d", gateSet.CycleNo)
	}
	if len(gateSet.Items) != 2 {
		t.Fatalf("expected 2 gate items, got %d", len(gateSet.Items))
	}

	retrySet, retryIdempotent, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"quality@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gset-instantiate-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set retry: %v", err)
	}
	if !retryIdempotent {
		t.Fatalf("expected retry instantiate to be idempotent")
	}
	if retrySet.GateSetID != gateSet.GateSetID {
		t.Fatalf("expected same gate set id on retry, got %q vs %q", retrySet.GateSetID, gateSet.GateSetID)
	}

	locked, lockedNow, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-gset-lock-1",
	})
	if err != nil {
		t.Fatalf("lock gate set: %v", err)
	}
	if !lockedNow {
		t.Fatalf("expected first lock call to lock now")
	}
	if strings.TrimSpace(locked.LockedAt) == "" {
		t.Fatalf("expected locked_at timestamp to be set")
	}

	relock, relockNow, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-gset-lock-1",
	})
	if err != nil {
		t.Fatalf("lock gate set second time: %v", err)
	}
	if relockNow {
		t.Fatalf("expected second lock call to report already locked")
	}
	if relock.GateSetID != gateSet.GateSetID {
		t.Fatalf("expected same gate set on relock, got %q vs %q", relock.GateSetID, gateSet.GateSetID)
	}

	var activeGateSetID sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT active_gate_set_id FROM work_items WHERE id = ?`, issueID).Scan(&activeGateSetID); err != nil {
		t.Fatalf("read active_gate_set_id: %v", err)
	}
	if !activeGateSetID.Valid || activeGateSetID.String != gateSet.GateSetID {
		t.Fatalf("expected active_gate_set_id=%q, got %#v", gateSet.GateSetID, activeGateSetID)
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

	description := "Track rich context as first-class metadata"
	acceptance := "Issue show surfaces context fields"
	references := []string{"https://example.com/rfc", "adr-001.md"}
	updated, event, idempotent, err := s.UpdateIssue(ctx, UpdateIssueParams{
		IssueID:            "mem-1f1f1f1",
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

func TestUpdateIssueStatusRejectsInvalidTransitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-2222222",
		Type:      "task",
		Title:     "Invalid transition test",
		Actor:     "agent-1",
		CommandID: "cmd-update-create-2",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-2222222",
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-update-2",
	})
	if err == nil || !strings.Contains(err.Error(), "invalid status transition") {
		t.Fatalf("expected invalid transition error, got: %v", err)
	}

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-2222222",
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-update-3",
	})
	if err != nil {
		t.Fatalf("Todo->InProgress should be allowed: %v", err)
	}

	gateSetID := "gs_update_transitions_1"
	seedLockedGateSetForTest(t, s, "mem-2222222", gateSetID)
	seedGateSetItemForTest(t, s, gateSetID, "build", "check", 1)
	appendGateEvaluationEventForTest(t, s, "mem-2222222", gateSetID, "build", "PASS", "agent-1", "cmd-update-gate-pass-1")

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-2222222",
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-update-4",
	})
	if err != nil {
		t.Fatalf("InProgress->Done with passing required gates should be allowed: %v", err)
	}

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-2222222",
		Status:    "blocked",
		Actor:     "agent-1",
		CommandID: "cmd-update-5",
	})
	if err == nil || !strings.Contains(err.Error(), "invalid status transition") {
		t.Fatalf("expected invalid transition from Done error, got: %v", err)
	}
}

func TestUpdateIssueStatusWontDoRequiresHumanActor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-2727272"
	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Human-only WontDo",
		Actor:     "agent-1",
		CommandID: "cmd-wontdo-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "wont-do",
		Actor:     "agent-1",
		CommandID: "cmd-wontdo-llm-1",
	})
	if err == nil || !strings.Contains(err.Error(), "WontDo status requires a human actor") {
		t.Fatalf("expected human-only WontDo restriction, got: %v", err)
	}

	updated, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "won't do",
		Actor:     "human:alice",
		CommandID: "cmd-wontdo-human-1",
	})
	if err != nil {
		t.Fatalf("human Todo->WontDo should be allowed: %v", err)
	}
	if updated.Status != "WontDo" {
		t.Fatalf("expected status WontDo, got %s", updated.Status)
	}

	reopened, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "todo",
		Actor:     "agent-1",
		CommandID: "cmd-wontdo-reopen-1",
	})
	if err != nil {
		t.Fatalf("WontDo->Todo should be allowed: %v", err)
	}
	if reopened.Status != "Todo" {
		t.Fatalf("expected reopened status Todo, got %s", reopened.Status)
	}
}

func TestUpdateIssueStatusAllowsHumanTransitionToWontDoFromAnyStatus(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		issueID      string
		initial      string
		commandID    string
		expectStatus string
	}{
		{name: "todo", issueID: "mem-2711111", initial: "todo", commandID: "cmd-wontdo-any-todo-1", expectStatus: "WontDo"},
		{name: "inprogress", issueID: "mem-2722222", initial: "inprogress", commandID: "cmd-wontdo-any-inprogress-1", expectStatus: "WontDo"},
		{name: "blocked", issueID: "mem-2733333", initial: "blocked", commandID: "cmd-wontdo-any-blocked-1", expectStatus: "WontDo"},
		{name: "done", issueID: "mem-2744444", initial: "done", commandID: "cmd-wontdo-any-done-1", expectStatus: "WontDo"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			s := newTestStore(t)

			_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
				IssueID:   tc.issueID,
				Type:      "task",
				Title:     "WontDo transition case " + tc.name,
				Actor:     "agent-1",
				CommandID: "cmd-wontdo-any-create-" + tc.name,
			})
			if err != nil {
				t.Fatalf("create issue: %v", err)
			}

			switch tc.initial {
			case "inprogress":
				_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
					IssueID:   tc.issueID,
					Status:    "inprogress",
					Actor:     "agent-1",
					CommandID: "cmd-wontdo-any-prep-inprogress-" + tc.name,
				})
			case "blocked":
				_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
					IssueID:   tc.issueID,
					Status:    "blocked",
					Actor:     "agent-1",
					CommandID: "cmd-wontdo-any-prep-blocked-" + tc.name,
				})
			case "done":
				_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
					IssueID:   tc.issueID,
					Status:    "inprogress",
					Actor:     "agent-1",
					CommandID: "cmd-wontdo-any-prep-done-progress-" + tc.name,
				})
				if err == nil {
					gateSetID := "gs_wontdo_any_" + tc.name
					seedLockedGateSetForTest(t, s, tc.issueID, gateSetID)
					seedGateSetItemForTest(t, s, gateSetID, "build", "check", 1)
					appendGateEvaluationEventForTest(t, s, tc.issueID, gateSetID, "build", "PASS", "agent-1", "cmd-wontdo-any-prep-done-gate-"+tc.name)
					_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
						IssueID:   tc.issueID,
						Status:    "done",
						Actor:     "agent-1",
						CommandID: "cmd-wontdo-any-prep-done-" + tc.name,
					})
				}
			}
			if err != nil {
				t.Fatalf("prepare initial status %s: %v", tc.initial, err)
			}

			updated, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
				IssueID:   tc.issueID,
				Status:    "wontdo",
				Actor:     "human:alice",
				CommandID: tc.commandID,
			})
			if err != nil {
				t.Fatalf("transition %s -> WontDo: %v", tc.initial, err)
			}
			if updated.Status != tc.expectStatus {
				t.Fatalf("expected status %s, got %s", tc.expectStatus, updated.Status)
			}
		})
	}
}

func TestUpdateIssueStatusDoneRequiresLockedGateSet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-2323233"
	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Done requires locked gate set",
		Actor:     "agent-1",
		CommandID: "cmd-close-requires-lock-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-close-requires-lock-progress-1",
	})
	if err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-close-requires-lock-done-1",
	})
	if err == nil || !strings.Contains(err.Error(), "no locked gate set for current cycle") {
		t.Fatalf("expected locked gate set requirement error, got: %v", err)
	}
}

func TestUpdateIssueStatusDoneRequiresPassingLockedRequiredGates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-3434343"
	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Close validation test",
		Actor:     "agent-1",
		CommandID: "cmd-close-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-close-progress-1",
	})
	if err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}

	gateSetID := "gs_close_1"
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, gateSetID, issueID, 1, `["tmpl-default@1"]`, `{"gates":[{"id":"build"}]}`, "closehash1", nowUTC(), nowUTC(), "agent-1")
	if err != nil {
		t.Fatalf("insert locked gate set: %v", err)
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES(?, ?, ?, ?, ?)
	`, gateSetID, "build", "check", 1, `{"command":"go test ./..."}`)
	if err != nil {
		t.Fatalf("insert required gate item: %v", err)
	}

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-close-done-missing-1",
	})
	if err == nil || !strings.Contains(err.Error(), "required gates not PASS: build=MISSING") {
		t.Fatalf("expected missing gate failure, got: %v", err)
	}

	eventsAfterMissing, err := s.ListEventsForEntity(ctx, entityTypeIssue, issueID)
	if err != nil {
		t.Fatalf("list events after missing gate close attempt: %v", err)
	}
	if len(eventsAfterMissing) != 2 {
		t.Fatalf("expected no Done event appended on failed close, got %d events", len(eventsAfterMissing))
	}

	appendGateEvaluationEventForTest(t, s, issueID, gateSetID, "build", "FAIL", "agent-1", "cmd-close-gate-eval-fail-1")

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-close-done-fail-1",
	})
	if err == nil || !strings.Contains(err.Error(), "required gates not PASS: build=FAIL") {
		t.Fatalf("expected failing gate close rejection, got: %v", err)
	}

	appendGateEvaluationEventWithoutEvidenceForTest(t, s, issueID, gateSetID, "build", "PASS", "agent-1", "cmd-close-gate-eval-pass-no-proof-1")

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-close-done-pass-no-proof-1",
	})
	if err == nil || !strings.Contains(err.Error(), "required gates not PASS: build=PASS_NO_PROOF") {
		t.Fatalf("expected pass-no-proof rejection, got: %v", err)
	}

	appendGateEvaluationEventWithEvidenceNoProofForTest(t, s, issueID, gateSetID, "build", "PASS", "agent-1", "cmd-close-gate-eval-pass-unverified-1")

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-close-done-pass-unverified-1",
	})
	if err == nil || !strings.Contains(err.Error(), "required gates not PASS: build=PASS_UNVERIFIED") {
		t.Fatalf("expected pass-unverified rejection, got: %v", err)
	}

	manualIssueID := "mem-6767676"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   manualIssueID,
		Type:      "task",
		Title:     "Manual validation close test",
		Actor:     "agent-1",
		CommandID: "cmd-close-manual-create-1",
	}); err != nil {
		t.Fatalf("create manual validation issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   manualIssueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-close-manual-progress-1",
	}); err != nil {
		t.Fatalf("move manual validation issue to inprogress: %v", err)
	}
	manualGateSetID := "gs_close_manual_1"
	seedLockedGateSetForTest(t, s, manualIssueID, manualGateSetID)
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES(?, ?, ?, ?, ?)
	`, manualGateSetID, "validated", "check", 1, `{"ref":"manual-validation"}`); err != nil {
		t.Fatalf("insert manual-validation gate_set_item: %v", err)
	}
	appendGateEvaluationEventWithEvidenceNoProofForTest(t, s, manualIssueID, manualGateSetID, "validated", "PASS", "agent-1", "cmd-close-manual-pass-1")

	closedManual, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   manualIssueID,
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-close-manual-done-1",
	})
	if err != nil {
		t.Fatalf("expected manual-validation close to succeed: %v", err)
	}
	if closedManual.Status != "Done" {
		t.Fatalf("expected manual-validation issue status Done, got %s", closedManual.Status)
	}

	appendGateEvaluationEventForTest(t, s, issueID, gateSetID, "build", "PASS", "agent-1", "cmd-close-gate-eval-pass-1")

	closed, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-close-done-pass-1",
	})
	if err != nil {
		t.Fatalf("expected close to succeed once required gate passes: %v", err)
	}
	if closed.Status != "Done" {
		t.Fatalf("expected issue status Done, got %s", closed.Status)
	}

	events, err := s.ListEventsForEntity(ctx, entityTypeIssue, issueID)
	if err != nil {
		t.Fatalf("list issue events: %v", err)
	}
	lastEvent := events[len(events)-1]
	if lastEvent.EventType != eventTypeIssueUpdate {
		t.Fatalf("expected last event %q, got %q", eventTypeIssueUpdate, lastEvent.EventType)
	}
	var payload issueUpdatedPayload
	if err := json.Unmarshal([]byte(lastEvent.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode final issue.updated payload: %v", err)
	}
	if payload.CloseProof == nil {
		t.Fatalf("expected close proof on Done payload")
	}
	if payload.CloseProof.GateSetID != gateSetID {
		t.Fatalf("expected close proof gate_set_id %q, got %q", gateSetID, payload.CloseProof.GateSetID)
	}
	if payload.CloseProof.GateSetHash != "closehash1" {
		t.Fatalf("expected close proof gate_set_hash closehash1, got %q", payload.CloseProof.GateSetHash)
	}
	if len(payload.CloseProof.Gates) != 1 {
		t.Fatalf("expected one close-proof gate, got %d", len(payload.CloseProof.Gates))
	}
	gateProof := payload.CloseProof.Gates[0]
	if gateProof.GateID != "build" || gateProof.Result != "PASS" {
		t.Fatalf("unexpected close-proof gate payload: %#v", gateProof)
	}
	if gateProof.Proof == nil || gateProof.Proof.Runner != "unit-test" || gateProof.Proof.ExitCode != 0 {
		t.Fatalf("expected verifier proof on close payload, got %#v", gateProof.Proof)
	}
}

func TestUpdateIssueStatusDoneRequiresChildIssuesClosed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	parentID := "mem-5656565"
	childID := "mem-5656566"

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   parentID,
		Type:      "story",
		Title:     "Parent story",
		Actor:     "agent-1",
		CommandID: "cmd-close-children-parent-create-1",
	})
	if err != nil {
		t.Fatalf("create parent issue: %v", err)
	}
	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   childID,
		Type:      "task",
		Title:     "Child task",
		ParentID:  parentID,
		Actor:     "agent-1",
		CommandID: "cmd-close-children-child-create-1",
	})
	if err != nil {
		t.Fatalf("create child issue: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   parentID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-close-children-parent-progress-1",
	})
	if err != nil {
		t.Fatalf("move parent to inprogress: %v", err)
	}

	parentGateSetID := "gs_close_parent_1"
	seedLockedGateSetForTest(t, s, parentID, parentGateSetID)
	seedGateSetItemForTest(t, s, parentGateSetID, "review", "check", 1)
	appendGateEvaluationEventForTest(t, s, parentID, parentGateSetID, "review", "PASS", "agent-1", "cmd-close-children-parent-gate-pass-1")

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   parentID,
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-close-children-parent-done-1",
	})
	if err == nil || !strings.Contains(err.Error(), `child issues must be Done first: mem-5656566=Todo`) {
		t.Fatalf("expected child-issue close validation error, got: %v", err)
	}

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   childID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-close-children-child-progress-1",
	})
	if err != nil {
		t.Fatalf("move child to inprogress: %v", err)
	}
	childGateSetID := "gs_close_child_1"
	seedLockedGateSetForTest(t, s, childID, childGateSetID)
	seedGateSetItemForTest(t, s, childGateSetID, "build", "check", 1)
	appendGateEvaluationEventForTest(t, s, childID, childGateSetID, "build", "PASS", "agent-1", "cmd-close-children-child-gate-pass-1")

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   childID,
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-close-children-child-done-1",
	})
	if err != nil {
		t.Fatalf("close child issue: %v", err)
	}

	closedParent, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   parentID,
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-close-children-parent-done-2",
	})
	if err != nil {
		t.Fatalf("close parent issue after child closure: %v", err)
	}
	if closedParent.Status != "Done" {
		t.Fatalf("expected parent status Done, got %s", closedParent.Status)
	}
}

func TestUpdateIssueStatusDoneAllowsWontDoChildIssues(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	parentID := "mem-5757575"
	childID := "mem-5858585"

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   parentID,
		Type:      "story",
		Title:     "Parent with wontdo child",
		Actor:     "agent-1",
		CommandID: "cmd-parent-wontdo-create-1",
	})
	if err != nil {
		t.Fatalf("create parent issue: %v", err)
	}
	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   childID,
		Type:      "task",
		Title:     "Skipped child",
		ParentID:  parentID,
		Actor:     "agent-1",
		CommandID: "cmd-child-wontdo-create-1",
	})
	if err != nil {
		t.Fatalf("create child issue: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   childID,
		Status:    "wontdo",
		Actor:     "human:alice",
		CommandID: "cmd-child-wontdo-status-1",
	})
	if err != nil {
		t.Fatalf("mark child WontDo: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   parentID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-parent-wontdo-progress-1",
	})
	if err != nil {
		t.Fatalf("move parent to inprogress: %v", err)
	}

	parentGateSetID := "gs_close_parent_wontdo_1"
	seedLockedGateSetForTest(t, s, parentID, parentGateSetID)
	seedGateSetItemForTest(t, s, parentGateSetID, "review", "check", 1)
	appendGateEvaluationEventForTest(t, s, parentID, parentGateSetID, "review", "PASS", "agent-1", "cmd-parent-wontdo-gate-pass-1")

	closedParent, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   parentID,
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-parent-wontdo-done-1",
	})
	if err != nil {
		t.Fatalf("close parent issue with WontDo child: %v", err)
	}
	if closedParent.Status != "Done" {
		t.Fatalf("expected parent status Done, got %s", closedParent.Status)
	}
}

func TestUpdateIssueStatusReopenAdvancesCycleAndClearsActiveGateSet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-6767676"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Reopen cycle test",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "reopen",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-reopen-template-create-1",
	}); err != nil {
		t.Fatalf("create gate template: %v", err)
	}

	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"reopen@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-reopen-gset-create-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-reopen-gset-lock-1",
	}); err != nil {
		t.Fatalf("lock gate set: %v", err)
	}
	appendGateEvaluationEventForTest(t, s, issueID, gateSet.GateSetID, "build", "PASS", "agent-1", "cmd-reopen-gate-pass-1")

	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-done-1",
	}); err != nil {
		t.Fatalf("close issue before reopen: %v", err)
	}

	reopened, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-inprogress-2",
	})
	if err != nil {
		t.Fatalf("reopen issue: %v", err)
	}
	if reopened.Status != "InProgress" {
		t.Fatalf("expected reopened status InProgress, got %s", reopened.Status)
	}

	var (
		cycleNo         int
		activeGateSetID sql.NullString
	)
	if err := s.db.QueryRowContext(ctx, `
		SELECT current_cycle_no, active_gate_set_id
		FROM work_items
		WHERE id = ?
	`, issueID).Scan(&cycleNo, &activeGateSetID); err != nil {
		t.Fatalf("read reopened work item state: %v", err)
	}
	if cycleNo != 2 {
		t.Fatalf("expected reopened issue to advance to cycle 2, got %d", cycleNo)
	}
	if activeGateSetID.Valid {
		t.Fatalf("expected reopened issue to clear active_gate_set_id, got %#v", activeGateSetID)
	}
}

func TestReopenSupportsNewCycleGateSetAndReplay(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-6868686"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Reopen replay test",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-replay-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-replay-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "reopen-replay",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-reopen-replay-template-create-1",
	}); err != nil {
		t.Fatalf("create gate template: %v", err)
	}

	cycle1, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"reopen-replay@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-reopen-replay-gset-create-1",
	})
	if err != nil {
		t.Fatalf("instantiate cycle 1 gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-reopen-replay-gset-lock-1",
	}); err != nil {
		t.Fatalf("lock cycle 1 gate set: %v", err)
	}
	appendGateEvaluationEventForTest(t, s, issueID, cycle1.GateSetID, "build", "PASS", "agent-1", "cmd-reopen-replay-gate-pass-1")
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-replay-done-1",
	}); err != nil {
		t.Fatalf("close issue before reopen: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-replay-inprogress-2",
	}); err != nil {
		t.Fatalf("reopen issue: %v", err)
	}

	cycle2, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"reopen-replay@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-reopen-replay-gset-create-2",
	})
	if err != nil {
		t.Fatalf("instantiate cycle 2 gate set: %v", err)
	}
	if cycle2.CycleNo != 2 {
		t.Fatalf("expected cycle 2 gate set, got cycle %d", cycle2.CycleNo)
	}
	if cycle2.GateSetID == cycle1.GateSetID {
		t.Fatalf("expected new gate set id for reopened cycle, got %q", cycle2.GateSetID)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-reopen-replay-gset-lock-2",
	}); err != nil {
		t.Fatalf("lock cycle 2 gate set: %v", err)
	}
	evaluation, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      issueID,
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/reopen-replay-2"},
		Actor:        "agent-1",
		CommandID:    "cmd-reopen-replay-gate-fail-2",
	})
	if err != nil {
		t.Fatalf("evaluate cycle 2 gate: %v", err)
	}
	if evaluation.GateSetID != cycle2.GateSetID {
		t.Fatalf("expected cycle 2 evaluation to bind gate set %q, got %q", cycle2.GateSetID, evaluation.GateSetID)
	}

	if _, err := s.ReplayProjections(ctx); err != nil {
		t.Fatalf("replay projections: %v", err)
	}

	var (
		cycleNo         int
		activeGateSetID sql.NullString
	)
	if err := s.db.QueryRowContext(ctx, `
		SELECT current_cycle_no, active_gate_set_id
		FROM work_items
		WHERE id = ?
	`, issueID).Scan(&cycleNo, &activeGateSetID); err != nil {
		t.Fatalf("read replayed work item state: %v", err)
	}
	if cycleNo != 2 {
		t.Fatalf("expected replayed current_cycle_no 2, got %d", cycleNo)
	}
	if !activeGateSetID.Valid || activeGateSetID.String != cycle2.GateSetID {
		t.Fatalf("expected replay to restore active_gate_set_id %q, got %#v", cycle2.GateSetID, activeGateSetID)
	}

	status, err := s.GetGateStatus(ctx, issueID)
	if err != nil {
		t.Fatalf("get gate status after replay: %v", err)
	}
	if status.CycleNo != 2 {
		t.Fatalf("expected replayed gate status cycle 2, got %d", status.CycleNo)
	}
	if len(status.Gates) != 1 || status.Gates[0].GateID != "build" || status.Gates[0].Result != "FAIL" {
		t.Fatalf("unexpected replayed gate status: %#v", status.Gates)
	}

	templateEvents, err := s.ListEventsForEntity(ctx, "gate_template", "reopen-replay@1")
	if err != nil {
		t.Fatalf("list gate template events: %v", err)
	}
	if len(templateEvents) != 1 || templateEvents[0].EventType != "gate_template.created" {
		t.Fatalf("unexpected gate template events: %#v", templateEvents)
	}

	gateSetEvents, err := s.ListEventsForEntity(ctx, "gate_set", cycle2.GateSetID)
	if err != nil {
		t.Fatalf("list gate set events: %v", err)
	}
	if len(gateSetEvents) != 2 || gateSetEvents[0].EventType != "gate_set.instantiated" || gateSetEvents[1].EventType != "gate_set.locked" {
		t.Fatalf("unexpected gate set events: %#v", gateSetEvents)
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

func TestEvaluateGateAppendsEventAndUpdatesProjection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-4545454"
	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Gate evaluation projection test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-eval-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-eval-progress-1",
	})
	if err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}

	gateSetID := "gs_eval_1"
	seedLockedGateSetForTest(t, s, issueID, gateSetID)
	seedGateSetItemForTest(t, s, gateSetID, "build", "check", 1)
	seedGateSetItemForTest(t, s, gateSetID, "lint", "check", 0)

	evaluation, event, idempotent, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      issueID,
		GateID:       "build",
		Result:       "pass",
		EvidenceRefs: []string{"go test ./...", " go test ./... ", "ci://run/1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gate-eval-1",
	})
	if err != nil {
		t.Fatalf("evaluate gate: %v", err)
	}
	if idempotent {
		t.Fatalf("first gate evaluation should not be idempotent")
	}
	if event.EventType != eventTypeGateEval {
		t.Fatalf("expected gate.evaluated event, got %s", event.EventType)
	}
	if evaluation.Result != "PASS" {
		t.Fatalf("expected normalized PASS result, got %q", evaluation.Result)
	}
	if !reflect.DeepEqual(evaluation.EvidenceRefs, []string{"go test ./...", "ci://run/1"}) {
		t.Fatalf("unexpected normalized evidence refs: %#v", evaluation.EvidenceRefs)
	}

	status, err := s.GetGateStatus(ctx, issueID)
	if err != nil {
		t.Fatalf("get gate status: %v", err)
	}
	if status.GateSetID != gateSetID {
		t.Fatalf("expected gate_set_id %q, got %q", gateSetID, status.GateSetID)
	}
	if len(status.Gates) != 2 {
		t.Fatalf("expected 2 gate status rows, got %d", len(status.Gates))
	}
	if status.Gates[0].GateID != "build" || status.Gates[0].Result != "PASS" {
		t.Fatalf("expected build gate PASS, got %#v", status.Gates[0])
	}
	if status.Gates[1].GateID != "lint" || status.Gates[1].Result != "MISSING" {
		t.Fatalf("expected lint gate MISSING, got %#v", status.Gates[1])
	}
}

func TestGetGateStatusSupportsCycleSelection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5858585"
	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Gate status cycle selection test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-cycle-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-cycle-progress-1",
	})
	if err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "gs_cycle_1", issueID, 1, `["tmpl-default@1"]`, `{"gates":[{"id":"build"}]}`, "gs_cycle_hash_1", nowUTC(), nowUTC(), "agent-1")
	if err != nil {
		t.Fatalf("insert cycle 1 gate set: %v", err)
	}
	seedGateSetItemForTest(t, s, "gs_cycle_1", "build", "check", 1)

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "gs_cycle_2", issueID, 2, `["tmpl-default@2"]`, `{"gates":[{"id":"deploy"}]}`, "gs_cycle_hash_2", nowUTC(), nowUTC(), "agent-1")
	if err != nil {
		t.Fatalf("insert cycle 2 gate set: %v", err)
	}
	seedGateSetItemForTest(t, s, "gs_cycle_2", "deploy", "check", 1)

	defaultStatus, err := s.GetGateStatus(ctx, issueID)
	if err != nil {
		t.Fatalf("get default gate status: %v", err)
	}
	if defaultStatus.CycleNo != 1 || defaultStatus.GateSetID != "gs_cycle_1" {
		t.Fatalf("expected default gate status for current cycle 1, got cycle=%d gate_set_id=%q", defaultStatus.CycleNo, defaultStatus.GateSetID)
	}

	cycleTwo := 2
	historicalStatus, err := s.GetGateStatusForCycle(ctx, GetGateStatusParams{
		IssueID: issueID,
		CycleNo: &cycleTwo,
	})
	if err != nil {
		t.Fatalf("get historical gate status: %v", err)
	}
	if historicalStatus.CycleNo != 2 || historicalStatus.GateSetID != "gs_cycle_2" {
		t.Fatalf("expected cycle 2 gate status, got cycle=%d gate_set_id=%q", historicalStatus.CycleNo, historicalStatus.GateSetID)
	}
	if len(historicalStatus.Gates) != 1 || historicalStatus.Gates[0].GateID != "deploy" {
		t.Fatalf("expected deploy gate for cycle 2, got %#v", historicalStatus.Gates)
	}

	cycleThree := 3
	_, err = s.GetGateStatusForCycle(ctx, GetGateStatusParams{
		IssueID: issueID,
		CycleNo: &cycleThree,
	})
	if err == nil || !strings.Contains(err.Error(), `no locked gate set found for issue "mem-5858585" cycle 3`) {
		t.Fatalf("expected cycle not found error, got: %v", err)
	}
}

func TestEvaluateGateRequiresEvidenceAndKnownGate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5656565"
	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Gate validation test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-validate-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-validate-progress-1",
	})
	if err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	gateSetID := "gs_eval_2"
	seedLockedGateSetForTest(t, s, issueID, gateSetID)
	seedGateSetItemForTest(t, s, gateSetID, "build", "check", 1)

	_, _, _, err = s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:   issueID,
		GateID:    "build",
		Result:    "PASS",
		Actor:     "agent-1",
		CommandID: "cmd-gate-validate-no-evidence-1",
	})
	if err == nil || !strings.Contains(err.Error(), "--evidence is required") {
		t.Fatalf("expected missing evidence error, got: %v", err)
	}

	_, _, _, err = s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      issueID,
		GateID:       "not-defined",
		Result:       "PASS",
		EvidenceRefs: []string{"ci://run/2"},
		Actor:        "agent-1",
		CommandID:    "cmd-gate-validate-missing-gate-1",
	})
	if err == nil || !strings.Contains(err.Error(), `gate "not-defined" is not defined`) {
		t.Fatalf("expected missing gate definition error, got: %v", err)
	}
}

func TestLookupGateVerificationSpecReturnsLockedCriteriaCommand(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5757575"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Gate verify lookup test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-verify-spec-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-verify-spec-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	seedGateTemplateRowForTest(t, s, "tmpl-default", 1, []string{"Task"}, `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`, "human:alice")

	gateSetID := "gs_verify_spec_1"
	seedLockedGateSetForTest(t, s, issueID, gateSetID)
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES(?, ?, ?, ?, ?)
	`, gateSetID, "build", "check", 1, `{"command":"go test ./..."}`); err != nil {
		t.Fatalf("insert gate_set_item with command criteria: %v", err)
	}

	spec, err := s.LookupGateVerificationSpec(ctx, issueID, "build")
	if err != nil {
		t.Fatalf("lookup gate verification spec: %v", err)
	}
	if spec.GateSetID != gateSetID {
		t.Fatalf("expected gate_set_id %q, got %q", gateSetID, spec.GateSetID)
	}
	if spec.Command != "go test ./..." {
		t.Fatalf("expected verifier command, got %q", spec.Command)
	}
	if spec.GateSetHash == "" {
		t.Fatalf("expected non-empty gate_set_hash in verification spec")
	}
}

func TestInstantiateGateSetRejectsExecutableTemplateWithoutHumanGovernance(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5858585"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Unsafe template instantiate test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-unsafe-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-unsafe-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	seedGateTemplateRowForTest(t, s, "unsafe", 1, []string{"Task"}, `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`, "llm:openai:gpt-5")

	_, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"unsafe@1"},
		Actor:        "human:alice",
		CommandID:    "cmd-gate-unsafe-instantiate-1",
	})
	if err == nil || !strings.Contains(err.Error(), "pending human approval") {
		t.Fatalf("expected approval rejection, got: %v", err)
	}
}

func TestApproveGateTemplateAllowsAgentAuthoredExecutableTemplateAfterHumanApproval(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5868686"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Executable template approval workflow",
		Actor:     "agent-1",
		CommandID: "cmd-gate-approve-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-approve-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}

	template, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "agent-authored",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`,
		Actor:          "llm:openai:gpt-5",
		CommandID:      "cmd-gate-approve-template-1",
	})
	if err != nil {
		t.Fatalf("create executable template: %v", err)
	}
	if !template.Executable {
		t.Fatalf("expected executable template")
	}
	if template.ApprovedBy != "" {
		t.Fatalf("expected executable template to start unapproved, got approved_by=%q", template.ApprovedBy)
	}

	_, _, err = s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"agent-authored@1"},
		Actor:        "human:alice",
		CommandID:    "cmd-gate-approve-instantiate-pre-1",
	})
	if err == nil || !strings.Contains(err.Error(), "pending human approval") {
		t.Fatalf("expected pre-approval instantiate rejection, got: %v", err)
	}

	approved, idempotent, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: "agent-authored",
		Version:    1,
		Actor:      "human:alice",
		CommandID:  "cmd-gate-approve-template-approve-1",
	})
	if err != nil {
		t.Fatalf("approve executable template: %v", err)
	}
	if idempotent {
		t.Fatalf("expected first approval to be non-idempotent")
	}
	if approved.ApprovedBy != "human:alice" {
		t.Fatalf("expected approval actor recorded, got %q", approved.ApprovedBy)
	}

	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"agent-authored@1"},
		Actor:        "human:alice",
		CommandID:    "cmd-gate-approve-instantiate-post-1",
	})
	if err != nil {
		t.Fatalf("instantiate approved template: %v", err)
	}
	if gateSet.GateSetID == "" {
		t.Fatalf("expected instantiated gate set id")
	}
}

func TestInstantiateGateSetRejectsRequiredNonExecutableTemplate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5878787"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Manual required gate instantiate test",
		Actor:     "human:alice",
		CommandID: "cmd-gate-manual-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "human:alice",
		CommandID: "cmd-gate-manual-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "manual-required",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"review","kind":"check","required":true,"criteria":{"ref":"manual-review"}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-gate-manual-template-1",
	}); err != nil {
		t.Fatalf("create manual gate template: %v", err)
	}

	_, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"manual-required@1"},
		Actor:        "human:alice",
		CommandID:    "cmd-gate-manual-instantiate-1",
	})
	if err == nil || !strings.Contains(err.Error(), "required gate(s) lack executable criteria.command") {
		t.Fatalf("expected required non-executable gate rejection, got: %v", err)
	}
}

func TestInstantiateGateSetAllowsRequiredManualValidationTemplate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5879797"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Manual validation instantiate test",
		Actor:     "human:alice",
		CommandID: "cmd-gate-manual-validation-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "human:alice",
		CommandID: "cmd-gate-manual-validation-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "manual-validation-required",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"validated","kind":"check","required":true,"criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-gate-manual-validation-template-1",
	}); err != nil {
		t.Fatalf("create manual-validation gate template: %v", err)
	}

	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"manual-validation-required@1"},
		Actor:        "human:alice",
		CommandID:    "cmd-gate-manual-validation-instantiate-1",
	})
	if err != nil {
		t.Fatalf("expected required manual-validation gate set to instantiate: %v", err)
	}
	if gateSet.GateSetID == "" {
		t.Fatalf("expected gate set id")
	}
}

func TestLookupGateVerificationSpecRejectsExecutableCommandFromNonHumanTemplate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5959595"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Unsafe template verify test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-unsafe-verify-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-unsafe-verify-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	seedGateTemplateRowForTest(t, s, "unsafe", 1, []string{"Task"}, `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`, "llm:openai:gpt-5")

	gateSetID := "gs_verify_spec_unsafe"
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, gateSetID, issueID, 1, `["unsafe@1"]`, `{"templates":["unsafe@1"],"gates":[{"gate_id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`, gateSetID+"_hash", nowUTC(), nowUTC(), "llm:openai:gpt-5"); err != nil {
		t.Fatalf("insert unsafe locked gate set: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES(?, ?, ?, ?, ?)
	`, gateSetID, "build", "check", 1, `{"command":"go test ./..."}`); err != nil {
		t.Fatalf("insert gate_set_item with unsafe command criteria: %v", err)
	}

	_, err := s.LookupGateVerificationSpec(ctx, issueID, "build")
	if err == nil || !strings.Contains(err.Error(), "unapproved template") {
		t.Fatalf("expected executable governance rejection, got: %v", err)
	}
}

func TestReplayProjectionsRebuildsGateStatusProjection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-6767676"
	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Replay gate status test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-replay-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-replay-progress-1",
	})
	if err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	createLockedGateSetEventSourcedForTest(t, s, issueID, "gate-replay", "build", "cmd-gate-replay")

	if _, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      issueID,
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/3"},
		Actor:        "agent-1",
		CommandID:    "cmd-gate-replay-eval-1",
	}); err != nil {
		t.Fatalf("evaluate gate before replay: %v", err)
	}

	if _, err := s.db.ExecContext(ctx, `DELETE FROM gate_status_projection WHERE issue_id = ?`, issueID); err != nil {
		t.Fatalf("clear gate status projection manually: %v", err)
	}

	replay, err := s.ReplayProjections(ctx)
	if err != nil {
		t.Fatalf("replay projections: %v", err)
	}
	if replay.EventsApplied != 6 {
		t.Fatalf("expected replay to apply 6 events, got %d", replay.EventsApplied)
	}

	status, err := s.GetGateStatus(ctx, issueID)
	if err != nil {
		t.Fatalf("get gate status after replay: %v", err)
	}
	if len(status.Gates) != 1 || status.Gates[0].Result != "FAIL" {
		t.Fatalf("expected replayed gate status FAIL, got %#v", status.Gates)
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

func TestCreateIssueWithParentEnforcesTypeConstraints(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-4444444",
		Type:      "epic",
		Title:     "Parent epic",
		Actor:     "agent-1",
		CommandID: "cmd-parent-1",
	})
	if err != nil {
		t.Fatalf("create parent epic: %v", err)
	}

	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-5555555",
		Type:      "task",
		Title:     "Invalid child for epic",
		ParentID:  "mem-4444444",
		Actor:     "agent-1",
		CommandID: "cmd-parent-2",
	})
	if err == nil || !strings.Contains(err.Error(), "parent Epic requires child Story") {
		t.Fatalf("expected Epic child type constraint error, got: %v", err)
	}
}

func TestLinkIssueValidAndIdempotentRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-6666666",
		Type:      "epic",
		Title:     "Link parent epic",
		Actor:     "agent-1",
		CommandID: "cmd-link-1",
	})
	if err != nil {
		t.Fatalf("create parent epic: %v", err)
	}

	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-7777777",
		Type:      "story",
		Title:     "Unlinked story",
		Actor:     "agent-1",
		CommandID: "cmd-link-2",
	})
	if err != nil {
		t.Fatalf("create child story: %v", err)
	}

	linked, event, idempotent, err := s.LinkIssue(ctx, LinkIssueParams{
		ChildIssueID:  "mem-7777777",
		ParentIssueID: "mem-6666666",
		Actor:         "agent-1",
		CommandID:     "cmd-link-3",
	})
	if err != nil {
		t.Fatalf("link issue: %v", err)
	}
	if idempotent {
		t.Fatalf("first link should not be idempotent")
	}
	if linked.ParentID != "mem-6666666" {
		t.Fatalf("expected parent mem-6666666, got %q", linked.ParentID)
	}
	if event.EventType != "issue.linked" {
		t.Fatalf("expected issue.linked event, got %s", event.EventType)
	}

	retryLinked, retryEvent, retryIdempotent, err := s.LinkIssue(ctx, LinkIssueParams{
		ChildIssueID:  "mem-7777777",
		ParentIssueID: "mem-8888888",
		Actor:         "agent-1",
		CommandID:     "cmd-link-3",
	})
	if err != nil {
		t.Fatalf("idempotent link retry should succeed: %v", err)
	}
	if !retryIdempotent {
		t.Fatalf("expected idempotent retry")
	}
	if retryEvent.EventID != event.EventID {
		t.Fatalf("expected same event id on retry, got %s vs %s", retryEvent.EventID, event.EventID)
	}
	if retryLinked.ParentID != "mem-6666666" {
		t.Fatalf("idempotent retry should keep original parent, got %q", retryLinked.ParentID)
	}
}

func TestLinkIssueRejectsTypeConstraintViolations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-8888888",
		Type:      "epic",
		Title:     "Epic parent",
		Actor:     "agent-1",
		CommandID: "cmd-link-4",
	})
	if err != nil {
		t.Fatalf("create parent epic: %v", err)
	}
	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-9999999",
		Type:      "task",
		Title:     "Task child",
		Actor:     "agent-1",
		CommandID: "cmd-link-5",
	})
	if err != nil {
		t.Fatalf("create child task: %v", err)
	}

	_, _, _, err = s.LinkIssue(ctx, LinkIssueParams{
		ChildIssueID:  "mem-9999999",
		ParentIssueID: "mem-8888888",
		Actor:         "agent-1",
		CommandID:     "cmd-link-6",
	})
	if err == nil || !strings.Contains(err.Error(), "parent Epic requires child Story") {
		t.Fatalf("expected type constraint error, got: %v", err)
	}
}

func TestLinkIssueRejectsCycles(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-aaaaaaa",
		Type:      "story",
		Title:     "Story child",
		Actor:     "agent-1",
		CommandID: "cmd-link-7",
	})
	if err != nil {
		t.Fatalf("create story: %v", err)
	}
	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-bbbbbbb",
		Type:      "epic",
		Title:     "Epic parent",
		Actor:     "agent-1",
		CommandID: "cmd-link-8",
	})
	if err != nil {
		t.Fatalf("create epic: %v", err)
	}

	// Simulate corrupted/injected hierarchy so cycle check is exercised:
	// mem-bbbbbbb -> mem-aaaaaaa, then attempted mem-aaaaaaa -> mem-bbbbbbb.
	if _, err := s.db.ExecContext(ctx, `UPDATE work_items SET parent_id = ? WHERE id = ?`, "mem-aaaaaaa", "mem-bbbbbbb"); err != nil {
		t.Fatalf("inject parent relationship for cycle test: %v", err)
	}

	_, _, _, err = s.LinkIssue(ctx, LinkIssueParams{
		ChildIssueID:  "mem-aaaaaaa",
		ParentIssueID: "mem-bbbbbbb",
		Actor:         "agent-1",
		CommandID:     "cmd-link-9",
	})
	if err == nil || !strings.Contains(err.Error(), "cycle detected") {
		t.Fatalf("expected cycle detection error, got: %v", err)
	}
}

func TestReplayProjectionsAppliesIssueLinkedEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-ccccccc",
		Type:      "epic",
		Title:     "Replay parent",
		Actor:     "agent-1",
		CommandID: "cmd-link-replay-1",
	})
	if err != nil {
		t.Fatalf("create parent issue: %v", err)
	}
	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-ddddddd",
		Type:      "story",
		Title:     "Replay child",
		Actor:     "agent-1",
		CommandID: "cmd-link-replay-2",
	})
	if err != nil {
		t.Fatalf("create child issue: %v", err)
	}
	_, _, _, err = s.LinkIssue(ctx, LinkIssueParams{
		ChildIssueID:  "mem-ddddddd",
		ParentIssueID: "mem-ccccccc",
		Actor:         "agent-1",
		CommandID:     "cmd-link-replay-3",
	})
	if err != nil {
		t.Fatalf("link issues: %v", err)
	}

	replay, err := s.ReplayProjections(ctx)
	if err != nil {
		t.Fatalf("replay projections: %v", err)
	}
	if replay.EventsApplied != 3 {
		t.Fatalf("expected 3 events applied in replay, got %d", replay.EventsApplied)
	}

	issue, err := s.GetIssue(ctx, "mem-ddddddd")
	if err != nil {
		t.Fatalf("get issue after replay: %v", err)
	}
	if issue.ParentID != "mem-ccccccc" {
		t.Fatalf("expected replayed parent mem-ccccccc, got %q", issue.ParentID)
	}
}

func TestNextIssuePrefersContinuitySignalsForAgentResume(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	baselineIssueID := "mem-4343434"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   baselineIssueID,
		Type:      "task",
		Title:     "Baseline in-progress task",
		Actor:     "agent-1",
		CommandID: "cmd-next-create-baseline-1",
	}); err != nil {
		t.Fatalf("create baseline issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   baselineIssueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-next-progress-baseline-1",
	}); err != nil {
		t.Fatalf("move baseline issue to inprogress: %v", err)
	}
	priority := "p0"
	if _, _, _, err := s.UpdateIssue(ctx, UpdateIssueParams{
		IssueID:   baselineIssueID,
		Priority:  &priority,
		Actor:     "agent-1",
		CommandID: "cmd-next-priority-baseline-1",
	}); err != nil {
		t.Fatalf("set baseline issue priority: %v", err)
	}

	continuityIssueID := "mem-4545454"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   continuityIssueID,
		Type:      "task",
		Title:     "Continuity-heavy resume task",
		Actor:     "agent-1",
		CommandID: "cmd-next-create-continuity-1",
	}); err != nil {
		t.Fatalf("create continuity issue: %v", err)
	}
	definition := `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"echo continuity"}}]}`
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "next-continuity",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: definition,
		Actor:          "human:alice",
		CommandID:      "cmd-next-template-1",
	}); err != nil {
		t.Fatalf("create continuity gate template: %v", err)
	}
	if _, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      continuityIssueID,
		TemplateRefs: []string{"next-continuity@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-next-instantiate-1",
	}); err != nil {
		t.Fatalf("instantiate continuity gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   continuityIssueID,
		Actor:     "agent-1",
		CommandID: "cmd-next-lock-1",
	}); err != nil {
		t.Fatalf("lock continuity gate set: %v", err)
	}
	packet, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "issue",
		ScopeID:   continuityIssueID,
		Actor:     "agent-1",
		CommandID: "cmd-next-packet-build-1",
	})
	if err != nil {
		t.Fatalf("build continuity packet: %v", err)
	}
	if _, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      continuityIssueID,
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/next-continuity-1"},
		Actor:        "agent-1",
		CommandID:    "cmd-next-gate-fail-1",
	}); err != nil {
		t.Fatalf("evaluate continuity gate fail: %v", err)
	}
	if _, _, _, err := s.UseRehydratePacket(ctx, UsePacketParams{
		AgentID:   "agent-next-1",
		PacketID:  packet.PacketID,
		Actor:     "agent-1",
		CommandID: "cmd-next-packet-use-1",
	}); err != nil {
		t.Fatalf("use continuity packet: %v", err)
	}

	baselineNext, err := s.NextIssue(ctx, "")
	if err != nil {
		t.Fatalf("next issue without continuity agent: %v", err)
	}
	if baselineNext.Candidate.Issue.ID != baselineIssueID {
		t.Fatalf("expected baseline issue %q without continuity agent, got %q", baselineIssueID, baselineNext.Candidate.Issue.ID)
	}

	resumeNext, err := s.NextIssue(ctx, "agent-next-1")
	if err != nil {
		t.Fatalf("next issue for continuity agent: %v", err)
	}
	if resumeNext.Candidate.Issue.ID != continuityIssueID {
		t.Fatalf("expected continuity issue %q, got %q", continuityIssueID, resumeNext.Candidate.Issue.ID)
	}
	if resumeNext.Considered != 2 {
		t.Fatalf("expected 2 considered issues, got %d", resumeNext.Considered)
	}

	reasonText := strings.Join(resumeNext.Candidate.Reasons, "\n")
	for _, expected := range []string{
		"matches the agent's active focus for resume",
		"agent already holds the latest recovery packet",
		"has 1 open loop(s) that need continuity",
		"1 required gate(s) are failing",
		"available issue packet is stale",
	} {
		if !strings.Contains(reasonText, expected) {
			t.Fatalf("expected next issue reasons to contain %q, got %q", expected, reasonText)
		}
	}
}

func TestSessionCheckpointPacketAndRehydrateFlow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-9898989"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Context packet test issue",
		Actor:     "agent-1",
		CommandID: "cmd-context-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-context-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	createLockedGateSetEventSourcedForTest(t, s, issueID, "context-gate", "build", "cmd-context-gset")
	if _, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      issueID,
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/context-1"},
		Actor:        "agent-1",
		CommandID:    "cmd-context-gate-fail-1",
	}); err != nil {
		t.Fatalf("evaluate gate fail for context packet: %v", err)
	}

	session, created, err := s.CheckpointSession(ctx, CheckpointSessionParams{
		SessionID: "sess-1",
		Trigger:   "manual",
		Actor:     "agent-1",
		CommandID: "cmd-context-checkpoint-1",
	})
	if err != nil {
		t.Fatalf("checkpoint session: %v", err)
	}
	if !created {
		t.Fatalf("expected first checkpoint to create session")
	}
	if session.SessionID != "sess-1" {
		t.Fatalf("expected session id sess-1, got %q", session.SessionID)
	}
	var chunkCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM context_chunks WHERE session_id = ?`, "sess-1").Scan(&chunkCount); err != nil {
		t.Fatalf("count context chunks: %v", err)
	}
	if chunkCount == 0 {
		t.Fatalf("expected checkpoint to persist context_chunks rows")
	}

	issuePacket, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "issue",
		ScopeID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-context-packet-issue-1",
	})
	if err != nil {
		t.Fatalf("build issue packet: %v", err)
	}
	if issuePacket.PacketID == "" || issuePacket.Scope != "issue" {
		t.Fatalf("expected issue packet metadata, got %#v", issuePacket)
	}
	stateRaw, ok := issuePacket.Packet["state"].(map[string]any)
	if !ok {
		t.Fatalf("expected issue packet state map, got %#v", issuePacket.Packet["state"])
	}
	if cycleNo, ok := stateRaw["cycle_no"].(float64); !ok || int(cycleNo) != 1 {
		t.Fatalf("expected issue packet cycle_no 1, got %#v", stateRaw["cycle_no"])
	}
	if builtFrom, ok := issuePacket.Packet["built_from_event_id"].(string); !ok || builtFrom == "" {
		t.Fatalf("expected issue packet built_from_event_id, got %#v", issuePacket.Packet["built_from_event_id"])
	}
	provenanceRaw, ok := issuePacket.Packet["provenance"].(map[string]any)
	if !ok {
		t.Fatalf("expected packet provenance map, got %#v", issuePacket.Packet["provenance"])
	}
	if provenanceCycle, ok := provenanceRaw["issue_cycle_no"].(float64); !ok || int(provenanceCycle) != 1 {
		t.Fatalf("expected packet provenance issue_cycle_no 1, got %#v", provenanceRaw["issue_cycle_no"])
	}
	gatesRaw, ok := issuePacket.Packet["gates"].([]any)
	if !ok || len(gatesRaw) == 0 {
		t.Fatalf("expected issue packet to include gate health, got %#v", issuePacket.Packet["gates"])
	}
	openLoopsRaw, ok := issuePacket.Packet["open_loops"].([]any)
	if !ok || len(openLoopsRaw) == 0 {
		t.Fatalf("expected issue packet to include open loops, got %#v", issuePacket.Packet["open_loops"])
	}
	loops, err := s.ListOpenLoops(ctx, ListOpenLoopsParams{IssueID: issueID})
	if err != nil {
		t.Fatalf("list open loops: %v", err)
	}
	if len(loops) == 0 {
		t.Fatalf("expected persisted open loops for issue %s", issueID)
	}
	var summaryCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM issue_summaries WHERE issue_id = ?`, issueID).Scan(&summaryCount); err != nil {
		t.Fatalf("count issue summaries: %v", err)
	}
	if summaryCount == 0 {
		t.Fatalf("expected issue summaries to persist after packet build")
	}

	storedIssuePacket, err := s.GetRehydratePacket(ctx, GetPacketParams{PacketID: issuePacket.PacketID})
	if err != nil {
		t.Fatalf("get stored issue packet: %v", err)
	}
	if storedIssuePacket.PacketID != issuePacket.PacketID {
		t.Fatalf("expected stored packet id %q, got %q", issuePacket.PacketID, storedIssuePacket.PacketID)
	}

	focus, usedPacket, idempotent, err := s.UseRehydratePacket(ctx, UsePacketParams{
		AgentID:   "agent-ctx-1",
		PacketID:  issuePacket.PacketID,
		Actor:     "agent-1",
		CommandID: "cmd-context-packet-use-1",
	})
	if err != nil {
		t.Fatalf("use issue packet: %v", err)
	}
	if idempotent {
		t.Fatalf("expected first packet use to be non-idempotent")
	}
	if focus.AgentID != "agent-ctx-1" || focus.LastPacketID != issuePacket.PacketID {
		t.Fatalf("unexpected agent focus after packet use: %#v", focus)
	}
	if focus.ActiveIssueID != issueID {
		t.Fatalf("expected active issue %q, got %q", issueID, focus.ActiveIssueID)
	}
	if usedPacket.PacketID != issuePacket.PacketID {
		t.Fatalf("expected used packet id %q, got %q", issuePacket.PacketID, usedPacket.PacketID)
	}
	focusEvents, err := s.ListEventsForEntity(ctx, "focus", "agent-ctx-1")
	if err != nil {
		t.Fatalf("list focus events: %v", err)
	}
	if len(focusEvents) != 1 || focusEvents[0].EventType != "focus.used" {
		t.Fatalf("expected one focus.used event, got %#v", focusEvents)
	}
	replayedFocus, _, idempotent, err := s.UseRehydratePacket(ctx, UsePacketParams{
		AgentID:   "agent-ctx-1",
		PacketID:  issuePacket.PacketID,
		Actor:     "agent-1",
		CommandID: "cmd-context-packet-use-1",
	})
	if err != nil {
		t.Fatalf("replay use issue packet: %v", err)
	}
	if !idempotent {
		t.Fatalf("expected replayed packet use to be idempotent")
	}
	if replayedFocus.LastPacketID != issuePacket.PacketID {
		t.Fatalf("expected replayed focus to keep last packet %q, got %q", issuePacket.PacketID, replayedFocus.LastPacketID)
	}

	rehydratedFallback, err := s.RehydrateSession(ctx, RehydrateSessionParams{SessionID: "sess-1"})
	if err != nil {
		t.Fatalf("rehydrate session (fallback): %v", err)
	}
	if rehydratedFallback.Source != "raw-events-fallback" {
		t.Fatalf("expected fallback source, got %q", rehydratedFallback.Source)
	}

	sessionPacket, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "session",
		ScopeID:   "sess-1",
		Actor:     "agent-1",
		CommandID: "cmd-context-packet-session-1",
	})
	if err != nil {
		t.Fatalf("build session packet: %v", err)
	}
	if sessionPacket.Scope != "session" {
		t.Fatalf("expected session scope packet, got %#v", sessionPacket)
	}

	if _, err := s.ReplayProjections(ctx); err != nil {
		t.Fatalf("replay projections: %v", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM sessions WHERE session_id = ?`, "sess-1").Scan(&chunkCount); err != nil {
		t.Fatalf("count replayed sessions: %v", err)
	}
	if chunkCount != 1 {
		t.Fatalf("expected replay to rebuild session row, got %d", chunkCount)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM context_chunks WHERE session_id = ?`, "sess-1").Scan(&chunkCount); err != nil {
		t.Fatalf("count replayed context chunks: %v", err)
	}
	if chunkCount == 0 {
		t.Fatalf("expected replay to rebuild context chunks")
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM agent_focus WHERE agent_id = ?`, "agent-ctx-1").Scan(&chunkCount); err != nil {
		t.Fatalf("count replayed agent focus rows: %v", err)
	}
	if chunkCount != 1 {
		t.Fatalf("expected replay to rebuild agent focus row, got %d", chunkCount)
	}

	rehydratedPacket, err := s.RehydrateSession(ctx, RehydrateSessionParams{SessionID: "sess-1"})
	if err != nil {
		t.Fatalf("rehydrate session (packet-first): %v", err)
	}
	if rehydratedPacket.Source != "packet" {
		t.Fatalf("expected packet source, got %q", rehydratedPacket.Source)
	}
	if rehydratedPacket.Packet.PacketID != sessionPacket.PacketID {
		t.Fatalf("expected latest session packet %q, got %q", sessionPacket.PacketID, rehydratedPacket.Packet.PacketID)
	}
}

func TestReplayRebuildsEventSourcedPacketsAndIssueSummaries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-9191919"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Replay packet test issue",
		Actor:     "agent-1",
		CommandID: "cmd-replay-packet-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-replay-packet-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	createLockedGateSetEventSourcedForTest(t, s, issueID, "replay-packet-gate", "build", "cmd-replay-packet-gset")
	_, gateEvent, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      issueID,
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/replay-packet-1"},
		Actor:        "agent-1",
		CommandID:    "cmd-replay-packet-gate-1",
	})
	if err != nil {
		t.Fatalf("evaluate gate for replay packet test: %v", err)
	}

	packet, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "issue",
		ScopeID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-replay-packet-build-1",
	})
	if err != nil {
		t.Fatalf("build issue packet: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO rehydrate_packets(packet_id, scope, packet_json, packet_schema_version, built_from_event_id, created_at)
		VALUES(?, ?, ?, ?, ?, ?)
	`, "pkt_stale_replay", "issue", `{"scope":"issue","scope_id":"mem-stale"}`, 1, "evt_stale", nowUTC()); err != nil {
		t.Fatalf("insert stale packet row: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO agent_focus(agent_id, active_issue_id, active_cycle_no, last_packet_id, updated_at)
		VALUES(?, ?, ?, ?, ?)
	`, "agent-stale-replay", issueID, 1, "pkt_stale_replay", nowUTC()); err != nil {
		t.Fatalf("insert stale agent focus: %v", err)
	}

	if _, err := s.db.ExecContext(ctx, `DELETE FROM issue_summaries WHERE summary_level = 'packet'`); err != nil {
		t.Fatalf("delete packet issue summaries: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM rehydrate_packets WHERE packet_id = ?`, packet.PacketID); err != nil {
		t.Fatalf("delete packet row: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO issue_summaries(
			summary_id, issue_id, cycle_no, summary_level, summary_json,
			from_entity_seq, to_entity_seq, parent_summary_id, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, NULL, ?)
	`, "sum_stale_replay", issueID, 1, "packet", `{"stale":true}`, 1, 1, nowUTC()); err != nil {
		t.Fatalf("insert stale issue summary: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM open_loops WHERE issue_id = ?`, issueID); err != nil {
		t.Fatalf("delete open loops: %v", err)
	}

	if _, err := s.ReplayProjections(ctx); err != nil {
		t.Fatalf("replay projections: %v", err)
	}

	var packetCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM rehydrate_packets WHERE packet_id = ?`, packet.PacketID).Scan(&packetCount); err != nil {
		t.Fatalf("count replayed packet row: %v", err)
	}
	if packetCount != 1 {
		t.Fatalf("expected replay to rebuild packet row, got %d", packetCount)
	}

	var summaryCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM issue_summaries WHERE summary_level = 'packet' AND issue_id = ?`, issueID).Scan(&summaryCount); err != nil {
		t.Fatalf("count replayed packet issue summaries: %v", err)
	}
	if summaryCount == 0 {
		t.Fatalf("expected replay to rebuild packet-derived issue summaries")
	}

	var loopCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM open_loops WHERE issue_id = ?`, issueID).Scan(&loopCount); err != nil {
		t.Fatalf("count replayed open loops: %v", err)
	}
	if loopCount == 0 {
		t.Fatalf("expected replay to rebuild open loops")
	}
	var stalePacketCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM rehydrate_packets WHERE packet_id = ?`, "pkt_stale_replay").Scan(&stalePacketCount); err != nil {
		t.Fatalf("count stale packet rows after replay: %v", err)
	}
	if stalePacketCount != 0 {
		t.Fatalf("expected replay to clear stale packet rows, got %d", stalePacketCount)
	}
	var staleSummaryCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM issue_summaries WHERE summary_id = ?`, "sum_stale_replay").Scan(&staleSummaryCount); err != nil {
		t.Fatalf("count stale summary rows after replay: %v", err)
	}
	if staleSummaryCount != 0 {
		t.Fatalf("expected replay to clear stale issue summaries, got %d", staleSummaryCount)
	}
	var staleFocusCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM agent_focus WHERE agent_id = ?`, "agent-stale-replay").Scan(&staleFocusCount); err != nil {
		t.Fatalf("count stale agent focus rows after replay: %v", err)
	}
	if staleFocusCount != 0 {
		t.Fatalf("expected replay to clear stale agent focus rows, got %d", staleFocusCount)
	}
	var loopSourceEventID string
	if err := s.db.QueryRowContext(ctx, `
		SELECT source_event_id
		FROM open_loops
		WHERE issue_id = ? AND loop_type = 'gate' AND status = 'Open'
		LIMIT 1
	`, issueID).Scan(&loopSourceEventID); err != nil {
		t.Fatalf("read replayed open loop source_event_id: %v", err)
	}
	if loopSourceEventID != gateEvent.EventID {
		t.Fatalf("expected replayed loop source_event_id %q, got %q", gateEvent.EventID, loopSourceEventID)
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

	ctx := context.Background()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, gateSetID, issueID, 1, `["tmpl-default@1"]`, `{"gates":[{"id":"build"}]}`, gateSetID+"_hash", nowUTC(), nowUTC(), "agent-1")
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
