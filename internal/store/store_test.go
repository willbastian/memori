package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
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
	`, "evt_test_entity_type", 1, "session", "mem-d4d4d4d", 1, "issue.created", "{}", "agent-1", "cmd-enum-1", nowUTC(), "hash_test_1", 1)
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
	})
	if err != nil {
		t.Fatalf("create gate template: %v", err)
	}

	gateSet, idempotent, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"quality@1"},
		Actor:        "agent-1",
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

	locked, lockedNow, err := s.LockGateSet(ctx, LockGateSetParams{IssueID: issueID})
	if err != nil {
		t.Fatalf("lock gate set: %v", err)
	}
	if !lockedNow {
		t.Fatalf("expected first lock call to lock now")
	}
	if strings.TrimSpace(locked.LockedAt) == "" {
		t.Fatalf("expected locked_at timestamp to be set")
	}

	relock, relockNow, err := s.LockGateSet(ctx, LockGateSetParams{IssueID: issueID})
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

	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-2222222",
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-update-4",
	})
	if err != nil {
		t.Fatalf("InProgress->Done should be allowed: %v", err)
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
	gateSetID := "gs_eval_3"
	seedLockedGateSetForTest(t, s, issueID, gateSetID)
	seedGateSetItemForTest(t, s, gateSetID, "build", "check", 1)

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
	if replay.EventsApplied != 3 {
		t.Fatalf("expected replay to apply 3 events, got %d", replay.EventsApplied)
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
