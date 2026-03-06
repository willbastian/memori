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
		Type:      "task",
		Title:     "Task one",
		ParentID:  "mem-a1b2c3d",
		Actor:     "agent-1",
		CommandID: "cmd-task-1",
	})
	if err != nil {
		t.Fatalf("create mem-b2c3d4e: %v", err)
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
