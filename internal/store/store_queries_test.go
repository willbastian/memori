package store

import (
	"context"
	"testing"
)

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
