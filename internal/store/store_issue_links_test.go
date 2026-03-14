package store

import (
	"context"
	"strings"
	"testing"
)

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

func TestCreateIssueRejectsMissingParent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-c1c1c1c",
		Type:      "story",
		Title:     "Self-parented story",
		ParentID:  "mem-c1c1c1c",
		Actor:     "agent-1",
		CommandID: "cmd-link-self-parent-1",
	})
	if err == nil || !strings.Contains(err.Error(), `issue "mem-c1c1c1c" not found`) {
		t.Fatalf("expected missing parent lookup error, got: %v", err)
	}
}

func TestLinkIssueRejectsAlreadyLinkedParentAndSelfLink(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-d1d1d1d",
		Type:      "epic",
		Title:     "Epic parent",
		Actor:     "agent-1",
		CommandID: "cmd-link-repeat-parent-1",
	})
	if err != nil {
		t.Fatalf("create parent epic: %v", err)
	}
	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-e1e1e1e",
		Type:      "story",
		Title:     "Child story",
		Actor:     "agent-1",
		CommandID: "cmd-link-repeat-child-1",
	})
	if err != nil {
		t.Fatalf("create child story: %v", err)
	}

	_, _, _, err = s.LinkIssue(ctx, LinkIssueParams{
		ChildIssueID:  "mem-e1e1e1e",
		ParentIssueID: "mem-d1d1d1d",
		Actor:         "agent-1",
		CommandID:     "cmd-link-repeat-link-1",
	})
	if err != nil {
		t.Fatalf("link child story: %v", err)
	}

	_, _, _, err = s.LinkIssue(ctx, LinkIssueParams{
		ChildIssueID:  "mem-e1e1e1e",
		ParentIssueID: "mem-d1d1d1d",
		Actor:         "agent-1",
		CommandID:     "cmd-link-repeat-link-2",
	})
	if err == nil || !strings.Contains(err.Error(), `already linked to parent "mem-d1d1d1d"`) {
		t.Fatalf("expected already-linked validation error, got: %v", err)
	}

	_, _, _, err = s.LinkIssue(ctx, LinkIssueParams{
		ChildIssueID:  "mem-d1d1d1d",
		ParentIssueID: "mem-d1d1d1d",
		Actor:         "agent-1",
		CommandID:     "cmd-link-repeat-self-1",
	})
	if err == nil || !strings.Contains(err.Error(), "issue cannot be its own parent") {
		t.Fatalf("expected self-link validation error, got: %v", err)
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
