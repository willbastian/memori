package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

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
