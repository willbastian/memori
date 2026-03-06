package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateIssueIdempotencyByActorAndCommandID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	firstIssue, firstEvent, firstIdempotent, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "task_alpha",
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
		IssueID:   "task_beta",
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

	alphaEvents, err := s.ListEventsForEntity(ctx, "issue", "task_alpha")
	if err != nil {
		t.Fatalf("list events for task_alpha: %v", err)
	}
	if len(alphaEvents) != 1 {
		t.Fatalf("expected 1 event for task_alpha, got %d", len(alphaEvents))
	}

	betaEvents, err := s.ListEventsForEntity(ctx, "issue", "task_beta")
	if err != nil {
		t.Fatalf("list events for task_beta: %v", err)
	}
	if len(betaEvents) != 0 {
		t.Fatalf("expected 0 events for task_beta, got %d", len(betaEvents))
	}

	storedIssue, err := s.GetIssue(ctx, "task_alpha")
	if err != nil {
		t.Fatalf("get issue task_alpha: %v", err)
	}
	if storedIssue.Title != "First title" {
		t.Fatalf("idempotent retry should not mutate title, got %q", storedIssue.Title)
	}
}

func TestReplayProjectionsDeterministicAcrossRuns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "epic_1",
		Type:      "epic",
		Title:     "Epic one",
		Actor:     "agent-1",
		CommandID: "cmd-epic-1",
	})
	if err != nil {
		t.Fatalf("create epic_1: %v", err)
	}
	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "task_1",
		Type:      "task",
		Title:     "Task one",
		ParentID:  "epic_1",
		Actor:     "agent-1",
		CommandID: "cmd-task-1",
	})
	if err != nil {
		t.Fatalf("create task_1: %v", err)
	}

	beforeEpic, err := s.GetIssue(ctx, "epic_1")
	if err != nil {
		t.Fatalf("get epic_1 before replay: %v", err)
	}
	beforeTask, err := s.GetIssue(ctx, "task_1")
	if err != nil {
		t.Fatalf("get task_1 before replay: %v", err)
	}

	replay1, err := s.ReplayProjections(ctx)
	if err != nil {
		t.Fatalf("first replay: %v", err)
	}
	if replay1.EventsApplied != 2 {
		t.Fatalf("expected first replay to apply 2 events, got %d", replay1.EventsApplied)
	}

	afterFirstEpic, err := s.GetIssue(ctx, "epic_1")
	if err != nil {
		t.Fatalf("get epic_1 after first replay: %v", err)
	}
	afterFirstTask, err := s.GetIssue(ctx, "task_1")
	if err != nil {
		t.Fatalf("get task_1 after first replay: %v", err)
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

	afterSecondEpic, err := s.GetIssue(ctx, "epic_1")
	if err != nil {
		t.Fatalf("get epic_1 after second replay: %v", err)
	}
	afterSecondTask, err := s.GetIssue(ctx, "task_1")
	if err != nil {
		t.Fatalf("get task_1 after second replay: %v", err)
	}
	assertIssueEqual(t, afterFirstEpic, afterSecondEpic)
	assertIssueEqual(t, afterFirstTask, afterSecondTask)
}

func TestEventsTableAppendOnlyTriggersBlockMutation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, event, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "task_trigger",
		Type:      "task",
		Title:     "Trigger test",
		Actor:     "agent-1",
		CommandID: "cmd-trigger",
	})
	if err != nil {
		t.Fatalf("create task_trigger: %v", err)
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

func newTestStore(t *testing.T) *Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "memori-test.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open test store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	if err := s.Initialize(context.Background()); err != nil {
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
