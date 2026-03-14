package store

import (
	"context"
	"strings"
	"testing"
)

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
