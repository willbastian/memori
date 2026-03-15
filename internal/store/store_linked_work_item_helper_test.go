package store

import (
	"context"
	"testing"
)

func TestIssueToLinkedWorkItemAndLinkedWorkItemListing(t *testing.T) {
	t.Parallel()

	withParent := issueToLinkedWorkItem(Issue{
		ID:       "mem-child01",
		Type:     "Task",
		Title:    "Child work item",
		Status:   "Todo",
		ParentID: "mem-parent1",
	}, "child")
	if withParent["parent_id"] != "mem-parent1" {
		t.Fatalf("expected issueToLinkedWorkItem to include parent_id, got %#v", withParent)
	}
	withoutParent := issueToLinkedWorkItem(Issue{
		ID:     "mem-parent1",
		Type:   "Epic",
		Title:  "Parent work item",
		Status: "InProgress",
	}, "parent")
	if _, ok := withoutParent["parent_id"]; ok {
		t.Fatalf("did not expect parent_id for top-level linked item, got %#v", withoutParent)
	}

	ctx := context.Background()
	s := newTestStore(t)

	parentID := "mem-111aaaa"
	issueID := "mem-222bbbb"
	openChildID := "mem-333cccc"
	doneChildID := "mem-444dddd"

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   parentID,
		Type:      "epic",
		Title:     "Parent",
		Actor:     "agent-1",
		CommandID: "cmd-linked-parent-1",
	}); err != nil {
		t.Fatalf("create parent issue: %v", err)
	}
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "story",
		Title:     "Current issue",
		ParentID:  parentID,
		Actor:     "agent-1",
		CommandID: "cmd-linked-current-1",
	}); err != nil {
		t.Fatalf("create current issue: %v", err)
	}
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   openChildID,
		Type:      "task",
		Title:     "Open child",
		ParentID:  issueID,
		Actor:     "agent-1",
		CommandID: "cmd-linked-open-child-1",
	}); err != nil {
		t.Fatalf("create open child issue: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO work_items(
			id, type, title, parent_id, status, priority, labels_json, current_cycle_no,
			active_gate_set_id, created_at, updated_at, last_event_id, description,
			acceptance_criteria, references_json
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, doneChildID, "Task", "Done child", issueID, "Done", nil, "[]", 1, nil, nowUTC(), nowUTC(), "evt_done_child", "", "", "[]"); err != nil {
		t.Fatalf("insert done child issue: %v", err)
	}

	issue, err := s.GetIssue(ctx, issueID)
	if err != nil {
		t.Fatalf("get current issue: %v", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for linked work items: %v", err)
	}
	defer tx.Rollback()

	items, err := listLinkedWorkItemsForIssueTx(ctx, tx, issue)
	if err != nil {
		t.Fatalf("list linked work items: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected parent plus one open child, got %#v", items)
	}

	parentItem := findLinkedWorkItem(t, items, parentID)
	if parentItem["relation"] != "parent" || parentItem["title"] != "Parent" || parentItem["status"] != "Todo" {
		t.Fatalf("unexpected parent linked work item: %#v", parentItem)
	}

	childItem := findLinkedWorkItem(t, items, openChildID)
	if childItem["relation"] != "child" || childItem["title"] != "Open child" || childItem["status"] != "Todo" {
		t.Fatalf("unexpected child linked work item: %#v", childItem)
	}
	if _, ok := childItem["parent_id"]; ok {
		t.Fatalf("did not expect child row map to include parent_id, got %#v", childItem)
	}
}

func findLinkedWorkItem(t *testing.T, items []any, issueID string) map[string]any {
	t.Helper()

	for _, item := range items {
		typed, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if typed["issue_id"] == issueID {
			return typed
		}
	}
	t.Fatalf("linked work item %q not found in %#v", issueID, items)
	return nil
}
