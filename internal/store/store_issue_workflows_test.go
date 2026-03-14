package store

import (
	"context"
	"strings"
	"testing"
)

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
