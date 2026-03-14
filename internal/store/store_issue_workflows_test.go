package store

import (
	"context"
	"database/sql"
	"encoding/json"
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
