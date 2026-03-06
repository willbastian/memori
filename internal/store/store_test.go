package store

import (
	"context"
	"path/filepath"
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

func assertIssueEqual(t *testing.T, expected, actual Issue) {
	t.Helper()
	if expected != actual {
		t.Fatalf("issue mismatch\nexpected: %#v\nactual:   %#v", expected, actual)
	}
}
