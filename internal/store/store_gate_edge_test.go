package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitializeAllowsPrefixChangeBeforeEventsAndRejectsAfterEvents(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-store-init-edge.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Initialize(ctx, InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		t.Fatalf("initialize mem prefix: %v", err)
	}
	if err := s.Initialize(ctx, InitializeParams{IssueKeyPrefix: "ops"}); err != nil {
		t.Fatalf("initialize ops prefix before events: %v", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	prefix, err := s.projectIssueKeyPrefixTx(ctx, tx)
	if err != nil {
		t.Fatalf("project issue key prefix: %v", err)
	}
	_ = tx.Rollback()
	if prefix != "ops" {
		t.Fatalf("expected updated prefix %q, got %q", "ops", prefix)
	}

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "ops-a1b2c3d",
		Type:      "task",
		Title:     "Freeze prefix",
		Actor:     "agent-1",
		CommandID: "cmd-init-prefix-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	if err := s.Initialize(ctx, InitializeParams{IssueKeyPrefix: "wrk"}); err == nil || !strings.Contains(err.Error(), "cannot change issue key prefix") {
		t.Fatalf("expected prefix change rejection after events, got %v", err)
	}
}

func TestGateTemplateAndGateSetEdgeCases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "release-checks",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[]}`,
		Actor:          "agent-1",
	}); err == nil || !strings.Contains(err.Error(), "--command-id is required") {
		t.Fatalf("expected missing command id error, got %v", err)
	}

	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "release-checks",
		Version:        0,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-gate-template-invalid-version-1",
	}); err == nil || !strings.Contains(err.Error(), "--version must be > 0") {
		t.Fatalf("expected invalid version error, got %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "release-checks",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: ` `,
		Actor:          "agent-1",
		CommandID:      "cmd-gate-template-invalid-json-1",
	}); err == nil || !strings.Contains(err.Error(), "--file must contain JSON") {
		t.Fatalf("expected invalid definition json error, got %v", err)
	}

	manualTemplate, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "manual-close",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"docs","kind":"check","required":true,"criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-gate-template-manual-1",
	})
	if err != nil {
		t.Fatalf("create manual gate template: %v", err)
	}

	if _, _, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: manualTemplate.TemplateID,
		Version:    manualTemplate.Version,
		Actor:      "human:alice",
		CommandID:  "cmd-gate-template-approve-manual-1",
	}); err == nil || !strings.Contains(err.Error(), "does not require approval") {
		t.Fatalf("expected non-executable approval rejection, got %v", err)
	}

	if _, _, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: "missing-template",
		Version:    1,
		Actor:      "human:alice",
		CommandID:  "cmd-gate-template-approve-missing-1",
	}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected missing template approval error, got %v", err)
	}
	if _, _, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: manualTemplate.TemplateID,
		Version:    0,
		Actor:      "human:alice",
		CommandID:  "cmd-gate-template-approve-invalid-version-1",
	}); err == nil || !strings.Contains(err.Error(), "--version must be > 0") {
		t.Fatalf("expected invalid approval version error, got %v", err)
	}
	if _, _, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: manualTemplate.TemplateID,
		Version:    manualTemplate.Version,
		Actor:      "llm:openai:gpt-5",
		CommandID:  "cmd-gate-template-approve-nonhuman-1",
	}); err == nil || !strings.Contains(err.Error(), "human-governed actor") {
		t.Fatalf("expected non-human approval rejection, got %v", err)
	}

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-c1d2e3f",
		Type:      "task",
		Title:     "Gate edges",
		Actor:     "agent-1",
		CommandID: "cmd-gate-issue-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-c1d2e3f",
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-issue-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}

	executableOne, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "exec-one",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-gate-template-exec-one-1",
	})
	if err != nil {
		t.Fatalf("create executable template one: %v", err)
	}
	executableTwo, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "exec-two",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"lint","kind":"check","required":true,"criteria":{"command":"go test ./internal/cli"}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-gate-template-exec-two-1",
	})
	if err != nil {
		t.Fatalf("create executable template two: %v", err)
	}

	gateSet, idempotent, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-c1d2e3f",
		TemplateRefs: []string{executableOne.TemplateID + "@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gate-set-instantiate-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if idempotent {
		t.Fatal("expected first gate set instantiation not to be idempotent")
	}

	idempotentSet, idempotent, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-c1d2e3f",
		TemplateRefs: []string{executableOne.TemplateID + "@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gate-set-instantiate-2",
	})
	if err != nil {
		t.Fatalf("instantiate same gate set idempotently: %v", err)
	}
	if !idempotent || idempotentSet.GateSetID != gateSet.GateSetID {
		t.Fatalf("expected existing gate set idempotent result, got %#v idempotent=%v", idempotentSet, idempotent)
	}

	if _, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-c1d2e3f",
		TemplateRefs: []string{executableTwo.TemplateID + "@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gate-set-instantiate-3",
	}); err == nil || !strings.Contains(err.Error(), "gate set already exists") {
		t.Fatalf("expected conflicting gate set error, got %v", err)
	}
	if _, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-c1d2e3f",
		TemplateRefs: []string{executableOne.TemplateID + "@1"},
		Actor:        "agent-1",
	}); err == nil || !strings.Contains(err.Error(), "--command-id is required") {
		t.Fatalf("expected missing instantiate command id error, got %v", err)
	}
	if _, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-a9b8c7d",
		TemplateRefs: []string{executableOne.TemplateID + "@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gate-set-instantiate-missing-issue-1",
	}); err == nil || !strings.Contains(err.Error(), `issue "mem-a9b8c7d" not found`) {
		t.Fatalf("expected missing issue instantiate error, got %v", err)
	}

	zeroCycle := 0
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-c1d2e3f",
		CycleNo:   &zeroCycle,
		Actor:     "agent-1",
		CommandID: "cmd-gate-set-lock-invalid-cycle-1",
	}); err == nil || !strings.Contains(err.Error(), "--cycle must be > 0") {
		t.Fatalf("expected invalid cycle error, got %v", err)
	}

	locked, lockedNow, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-c1d2e3f",
		Actor:     "agent-1",
		CommandID: "cmd-gate-set-lock-1",
	})
	if err != nil {
		t.Fatalf("lock gate set: %v", err)
	}
	if !lockedNow || strings.TrimSpace(locked.LockedAt) == "" {
		t.Fatalf("expected newly locked gate set, got %#v lockedNow=%v", locked, lockedNow)
	}

	lockedAgain, lockedNow, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-c1d2e3f",
		Actor:     "agent-1",
		CommandID: "cmd-gate-set-lock-2",
	})
	if err != nil {
		t.Fatalf("lock already locked gate set: %v", err)
	}
	if lockedNow || lockedAgain.GateSetID != gateSet.GateSetID {
		t.Fatalf("expected existing locked gate set, got %#v lockedNow=%v", lockedAgain, lockedNow)
	}

	approvedAgain, idempotent, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: executableOne.TemplateID,
		Version:    executableOne.Version,
		Actor:      "human:bob",
		CommandID:  "cmd-gate-template-approve-already-approved-1",
	})
	if err != nil {
		t.Fatalf("approve already-approved template: %v", err)
	}
	if !idempotent || approvedAgain.ApprovedBy != "human:alice" {
		t.Fatalf("expected already-approved template to return idempotent existing approval, got %#v idempotent=%v", approvedAgain, idempotent)
	}

	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-c1d2e3f",
		Actor:     "agent-1",
	}); err == nil || !strings.Contains(err.Error(), "--command-id is required") {
		t.Fatalf("expected missing lock command id error, got %v", err)
	}

	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-a9b8c7d",
		Actor:     "agent-1",
		CommandID: "cmd-gate-set-lock-missing-issue-1",
	}); err == nil || !strings.Contains(err.Error(), `issue "mem-a9b8c7d" not found`) {
		t.Fatalf("expected missing issue lock error, got %v", err)
	}

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-e4f5a6b",
		Type:      "task",
		Title:     "Lock validation issue",
		Actor:     "agent-1",
		CommandID: "cmd-gate-lock-validation-issue-1",
	}); err != nil {
		t.Fatalf("create lock validation issue: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-e4f5a6b",
		Actor:     "agent-1",
		CommandID: "cmd-gate-set-lock-no-set-1",
	}); err == nil || !strings.Contains(err.Error(), "no gate set found") {
		t.Fatalf("expected missing gate set lock error, got %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, NULL, ?, ?)
	`, "gset-empty", "mem-e4f5a6b", 1, `["manual-close@1"]`, `{"templates":["manual-close@1"],"gates":[]}`, "gset-empty-hash", nowUTC(), "agent-1"); err != nil {
		t.Fatalf("insert empty gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-e4f5a6b",
		Actor:     "agent-1",
		CommandID: "cmd-gate-set-lock-empty-1",
	}); err == nil || !strings.Contains(err.Error(), "no gate items defined") {
		t.Fatalf("expected empty gate set lock error, got %v", err)
	}
}

func TestGateWorkflowReplaysMissingProjectionsOnIdempotentRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-f1a2b3c",
		Type:      "task",
		Title:     "Replay missing gate projections",
		Actor:     "agent-1",
		CommandID: "cmd-gate-replay-issue-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	template, idempotent, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "replay-template",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-gate-replay-template-1",
	})
	if err != nil {
		t.Fatalf("create template: %v", err)
	}
	if idempotent {
		t.Fatal("expected first template create to be non-idempotent")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin template replay tx: %v", err)
	}
	if err := dropReplayProjectionDeleteTriggersTx(ctx, tx); err != nil {
		t.Fatalf("drop template replay triggers: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_template_approvals WHERE template_id = ? AND version = ?`, template.TemplateID, template.Version); err != nil {
		t.Fatalf("delete gate template approvals: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_templates WHERE template_id = ? AND version = ?`, template.TemplateID, template.Version); err != nil {
		t.Fatalf("delete gate templates: %v", err)
	}
	if err := restoreReplayProjectionDeleteTriggersTx(ctx, tx); err != nil {
		t.Fatalf("restore template replay triggers: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit template replay tx: %v", err)
	}

	replayedTemplate, idempotent, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "replay-template",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-gate-replay-template-1",
	})
	if err != nil {
		t.Fatalf("replay template via idempotent retry: %v", err)
	}
	if !idempotent {
		t.Fatal("expected template retry to report idempotent replay")
	}
	if replayedTemplate.TemplateID != template.TemplateID || replayedTemplate.ApprovedBy != "human:alice" {
		t.Fatalf("expected replayed template with preserved approval, got %#v", replayedTemplate)
	}

	pendingTemplate, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "pending-template",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"lint","kind":"check","required":true,"criteria":{"command":"go test ./internal/cli"}}]}`,
		Actor:          "llm:openai:gpt-5",
		CommandID:      "cmd-gate-replay-pending-template-1",
	})
	if err != nil {
		t.Fatalf("create pending template: %v", err)
	}

	approvedTemplate, idempotent, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: pendingTemplate.TemplateID,
		Version:    pendingTemplate.Version,
		Actor:      "human:alice",
		CommandID:  "cmd-gate-replay-approve-1",
	})
	if err != nil {
		t.Fatalf("approve pending template: %v", err)
	}
	if idempotent {
		t.Fatal("expected first approval to be non-idempotent")
	}

	if _, err := s.db.ExecContext(ctx, `DELETE FROM gate_template_approvals WHERE template_id = ? AND version = ?`, pendingTemplate.TemplateID, pendingTemplate.Version); err != nil {
		t.Fatalf("delete replay approval row: %v", err)
	}

	replayedApproval, idempotent, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: pendingTemplate.TemplateID,
		Version:    pendingTemplate.Version,
		Actor:      "human:alice",
		CommandID:  "cmd-gate-replay-approve-1",
	})
	if err != nil {
		t.Fatalf("replay approval via idempotent retry: %v", err)
	}
	if !idempotent {
		t.Fatal("expected approval retry to report idempotent replay")
	}
	if replayedApproval.ApprovedBy != approvedTemplate.ApprovedBy || replayedApproval.ApprovedAt == "" {
		t.Fatalf("expected replayed approval metadata, got %#v", replayedApproval)
	}

	gateSet, idempotent, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-f1a2b3c",
		TemplateRefs: []string{"replay-template@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gate-replay-set-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if idempotent {
		t.Fatal("expected first gate set instantiate to be non-idempotent")
	}

	tx, err = s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin gate set replay tx: %v", err)
	}
	if err := dropReplayProjectionDeleteTriggersTx(ctx, tx); err != nil {
		t.Fatalf("drop gate set replay triggers: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_set_items WHERE gate_set_id = ?`, gateSet.GateSetID); err != nil {
		t.Fatalf("delete gate set items: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_sets WHERE gate_set_id = ?`, gateSet.GateSetID); err != nil {
		t.Fatalf("delete gate sets: %v", err)
	}
	if err := restoreReplayProjectionDeleteTriggersTx(ctx, tx); err != nil {
		t.Fatalf("restore gate set replay triggers: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit gate set replay tx: %v", err)
	}

	replayedSet, idempotent, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-f1a2b3c",
		TemplateRefs: []string{"replay-template@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gate-replay-set-1",
	})
	if err != nil {
		t.Fatalf("replay gate set via idempotent retry: %v", err)
	}
	if !idempotent {
		t.Fatal("expected gate set retry to report idempotent replay")
	}
	if replayedSet.GateSetID != gateSet.GateSetID || len(replayedSet.Items) != 1 || replayedSet.Items[0].GateID != "build" {
		t.Fatalf("expected replayed gate set contents, got %#v", replayedSet)
	}
}

func TestGateCommandsRejectCommandIDsUsedByOtherEventTypes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-a4b5c6d",
		Type:      "task",
		Title:     "Agent collision issue",
		Actor:     "agent-1",
		CommandID: "cmd-gate-collision-agent-1",
	}); err != nil {
		t.Fatalf("create agent collision issue: %v", err)
	}
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-b5c6d7e",
		Type:      "task",
		Title:     "Human collision issue",
		Actor:     "human:alice",
		CommandID: "cmd-gate-collision-human-1",
	}); err != nil {
		t.Fatalf("create human collision issue: %v", err)
	}

	agentCases := []struct {
		name string
		call func() error
	}{
		{
			name: "create gate template",
			call: func() error {
				_, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
					TemplateID:     "collision-template",
					Version:        1,
					AppliesTo:      []string{"task"},
					DefinitionJSON: `{"gates":[]}`,
					Actor:          "agent-1",
					CommandID:      "cmd-gate-collision-agent-1",
				})
				return err
			},
		},
		{
			name: "instantiate gate set",
			call: func() error {
				_, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
					IssueID:      "mem-a4b5c6d",
					TemplateRefs: []string{"collision-template@1"},
					Actor:        "agent-1",
					CommandID:    "cmd-gate-collision-agent-1",
				})
				return err
			},
		},
		{
			name: "lock gate set",
			call: func() error {
				_, _, err := s.LockGateSet(ctx, LockGateSetParams{
					IssueID:   "mem-a4b5c6d",
					Actor:     "agent-1",
					CommandID: "cmd-gate-collision-agent-1",
				})
				return err
			},
		},
		{
			name: "evaluate gate",
			call: func() error {
				_, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
					IssueID:      "mem-a4b5c6d",
					GateID:       "build",
					Result:       "fail",
					EvidenceRefs: []string{"ci://run/42"},
					Actor:        "agent-1",
					CommandID:    "cmd-gate-collision-agent-1",
				})
				return err
			},
		},
	}

	for _, tc := range agentCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil || !strings.Contains(err.Error(), `command id already used by "issue.created"`) {
				t.Fatalf("expected command collision error, got %v", err)
			}
		})
	}

	if _, _, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: "missing-template",
		Version:    1,
		Actor:      "human:alice",
		CommandID:  "cmd-gate-collision-human-1",
	}); err == nil || !strings.Contains(err.Error(), `command id already used by "issue.created"`) {
		t.Fatalf("expected approval command collision error, got %v", err)
	}
}

func TestEvaluateGateAndVerificationSpecEdgeCases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-d1e2f3a",
		Type:      "task",
		Title:     "Manual verification edges",
		Actor:     "agent-1",
		CommandID: "cmd-eval-edge-issue-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-d1e2f3a",
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-eval-edge-issue-progress-1",
	}); err != nil {
		t.Fatalf("progress issue: %v", err)
	}

	if _, err := s.LookupGateVerificationSpec(ctx, "mem-d1e2f3a", ""); err == nil || !strings.Contains(err.Error(), "--gate is required") {
		t.Fatalf("expected missing gate id error, got %v", err)
	}
	if _, err := s.LookupGateVerificationSpec(ctx, "mem-d1e2f3a", "build"); err == nil || !strings.Contains(err.Error(), "no locked gate set found") {
		t.Fatalf("expected no locked gate set error, got %v", err)
	}

	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "manual-only",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"docs","kind":"check","required":true,"criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-eval-edge-template-1",
	}); err != nil {
		t.Fatalf("create manual-only template: %v", err)
	}
	if _, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-d1e2f3a",
		TemplateRefs: []string{"manual-only@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-eval-edge-instantiate-1",
	}); err != nil {
		t.Fatalf("instantiate manual-only gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-d1e2f3a",
		Actor:     "agent-1",
		CommandID: "cmd-eval-edge-lock-1",
	}); err != nil {
		t.Fatalf("lock manual-only gate set: %v", err)
	}

	if _, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      "mem-d1e2f3a",
		GateID:       "docs",
		Result:       "PASS",
		EvidenceRefs: []string{"docs://review"},
		Proof: &GateEvaluationProof{
			Verifier: "human:alice",
		},
		Actor:     "agent-1",
		CommandID: "cmd-eval-edge-proof-1",
	}); err == nil || !strings.Contains(err.Error(), "cannot accept verifier proof") {
		t.Fatalf("expected proof rejection for manual gate, got %v", err)
	}

	if _, err := s.LookupGateVerificationSpec(ctx, "mem-d1e2f3a", "docs"); err == nil || !strings.Contains(err.Error(), "no executable verifier command") {
		t.Fatalf("expected manual gate verifier command error, got %v", err)
	}
	if _, err := s.LookupGateVerificationSpec(ctx, "mem-d1e2f3a", "missing"); err == nil || !strings.Contains(err.Error(), "is not defined") {
		t.Fatalf("expected missing gate verification spec error, got %v", err)
	}
}

func TestEvaluateGateExecutablePassAndIdempotentRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-f2a3b4c",
		Type:      "task",
		Title:     "Evaluate gate retry edges",
		Actor:     "agent-1",
		CommandID: "cmd-eval-retry-issue-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-f2a3b4c",
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-eval-retry-issue-progress-1",
	}); err != nil {
		t.Fatalf("progress issue: %v", err)
	}

	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "eval-retry-template",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}},{"id":"lint","kind":"check","required":false,"criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-eval-retry-template-1",
	}); err != nil {
		t.Fatalf("create evaluate retry template: %v", err)
	}
	if _, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-f2a3b4c",
		TemplateRefs: []string{"eval-retry-template@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-eval-retry-instantiate-1",
	}); err != nil {
		t.Fatalf("instantiate evaluate retry gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-f2a3b4c",
		Actor:     "agent-1",
		CommandID: "cmd-eval-retry-lock-1",
	}); err != nil {
		t.Fatalf("lock evaluate retry gate set: %v", err)
	}

	if _, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      "mem-f2a3b4c",
		GateID:       "build",
		Result:       "PASS",
		EvidenceRefs: []string{"ci://run/77"},
		Actor:        "agent-1",
		CommandID:    "cmd-eval-retry-build-pass-1",
	}); err == nil || !strings.Contains(err.Error(), "use memori gate verify") {
		t.Fatalf("expected executable PASS guidance error, got %v", err)
	}

	evaluation, event, idempotent, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      "mem-f2a3b4c",
		GateID:       "lint",
		Result:       "PASS",
		EvidenceRefs: []string{"docs://review"},
		Actor:        "agent-1",
		CommandID:    "cmd-eval-retry-lint-pass-1",
	})
	if err != nil {
		t.Fatalf("evaluate optional gate: %v", err)
	}
	if idempotent {
		t.Fatal("expected first optional gate evaluation to be non-idempotent")
	}
	if evaluation.GateID != "lint" || evaluation.Result != "PASS" || event.EventType != eventTypeGateEval {
		t.Fatalf("unexpected optional gate evaluation result: evaluation=%#v event=%#v", evaluation, event)
	}

	retryEvaluation, retryEvent, idempotent, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      "mem-f2a3b4c",
		GateID:       "lint",
		Result:       "PASS",
		EvidenceRefs: []string{"docs://review"},
		Actor:        "agent-1",
		CommandID:    "cmd-eval-retry-lint-pass-1",
	})
	if err != nil {
		t.Fatalf("retry optional gate evaluation: %v", err)
	}
	if !idempotent {
		t.Fatal("expected optional gate retry to be idempotent")
	}
	if retryEvent.EventID != event.EventID || retryEvaluation.GateID != evaluation.GateID || retryEvaluation.EvaluatedAt != evaluation.EvaluatedAt {
		t.Fatalf("expected retry to replay original evaluation, got evaluation=%#v event=%#v", retryEvaluation, retryEvent)
	}
}

func TestProjectionFunctionsRejectMissingOrConflictingState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-e1f2a3b",
		Type:      "task",
		Title:     "Projection lock conflict",
		Actor:     "agent-1",
		CommandID: "cmd-projection-lock-issue-1",
	}); err != nil {
		t.Fatalf("create projection issue: %v", err)
	}
	seedGateTemplateRowForTest(t, s, "tmpl-hash", 1, []string{"task"}, `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`, "human:alice")

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	summarizedPayload, _ := json.Marshal(sessionSummarizedPayload{
		SessionID:           "sess-missing",
		Summary:             map[string]any{"status": "done"},
		SummarizedAt:        "2026-03-08T00:00:00Z",
		ContextChunkID:      "chunk-summary",
		ContextChunkKind:    "summary",
		ContextChunkContent: "summary",
		ContextChunkMeta:    map[string]any{"kind": "summary"},
	})
	if err := applySessionSummarizedProjectionTx(ctx, tx, Event{
		EventID:     "evt_session_summary_missing",
		EventType:   eventTypeSessionSummarized,
		PayloadJSON: string(summarizedPayload),
	}); err == nil || !strings.Contains(err.Error(), `session "sess-missing" not found`) {
		t.Fatalf("expected missing session summary error, got %v", err)
	}

	closedPayload, _ := json.Marshal(sessionClosedPayload{
		SessionID:           "sess-missing",
		EndedAt:             "2026-03-08T00:00:01Z",
		ClosedAt:            "2026-03-08T00:00:01Z",
		ContextChunkID:      "chunk-close",
		ContextChunkKind:    "close",
		ContextChunkContent: "close",
		ContextChunkMeta:    map[string]any{"kind": "close"},
	})
	if err := applySessionClosedProjectionTx(ctx, tx, Event{
		EventID:     "evt_session_close_missing",
		EventType:   eventTypeSessionClosed,
		PayloadJSON: string(closedPayload),
	}); err == nil || !strings.Contains(err.Error(), `session "sess-missing" not found`) {
		t.Fatalf("expected missing session close error, got %v", err)
	}

	focusPayload, _ := json.Marshal(focusUsedPayload{
		AgentID:      "agent-1",
		LastPacketID: "pkt-missing",
		FocusedAt:    "2026-03-08T00:00:02Z",
	})
	if err := applyFocusUsedProjectionTx(ctx, tx, Event{
		EventID:     "evt_focus_missing_packet",
		EventType:   eventTypeFocusUsed,
		PayloadJSON: string(focusPayload),
	}); err == nil || !strings.Contains(err.Error(), `packet "pkt-missing" not found`) {
		t.Fatalf("expected missing packet focus error, got %v", err)
	}

	approvalPayload, _ := json.Marshal(gateTemplateApprovedPayload{
		TemplateID:     "tmpl-missing",
		Version:        1,
		DefinitionHash: "hash",
		ApprovedAt:     "2026-03-08T00:00:03Z",
		ApprovedBy:     "human:alice",
	})
	if err := applyGateTemplateApprovedProjectionTx(ctx, tx, Event{
		EventID:     "evt_gate_template_approve_missing",
		EventType:   eventTypeGateTemplateApprove,
		PayloadJSON: string(approvalPayload),
	}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected missing gate template approval error, got %v", err)
	}

	hashMismatchPayload, _ := json.Marshal(gateTemplateApprovedPayload{
		TemplateID:     "tmpl-hash",
		Version:        1,
		DefinitionHash: "wrong-hash",
		ApprovedAt:     "2026-03-08T00:00:04Z",
		ApprovedBy:     "human:alice",
	})
	if err := applyGateTemplateApprovedProjectionTx(ctx, tx, Event{
		EventID:     "evt_gate_template_approve_hash",
		EventType:   eventTypeGateTemplateApprove,
		PayloadJSON: string(hashMismatchPayload),
	}); err == nil || !strings.Contains(err.Error(), "definition hash mismatch") {
		t.Fatalf("expected gate template hash mismatch error, got %v", err)
	}

	lockPayload, _ := json.Marshal(gateSetLockedPayload{
		GateSetID: "gset-missing",
		IssueID:   "mem-d1e2f3a",
		CycleNo:   1,
		LockedAt:  "2026-03-08T00:00:05Z",
	})
	if err := applyGateSetLockedProjectionTx(ctx, tx, Event{
		EventID:     "evt_gate_set_lock_missing",
		EventType:   eventTypeGateSetLock,
		PayloadJSON: string(lockPayload),
	}); err == nil || !strings.Contains(err.Error(), `gate set "gset-missing" not found`) {
		t.Fatalf("expected missing gate set lock error, got %v", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "gset-conflict", "mem-e1f2a3b", 1, `["tmpl-hash@1"]`, `{"gates":[{"id":"build"}]}`, "hash", "2026-03-08T00:00:06Z", nowUTC(), "agent-1"); err != nil {
		t.Fatalf("insert conflicting gate set: %v", err)
	}
	conflictPayload, _ := json.Marshal(gateSetLockedPayload{
		GateSetID: "gset-conflict",
		IssueID:   "mem-e1f2a3b",
		CycleNo:   1,
		LockedAt:  "2026-03-08T00:00:07Z",
	})
	if err := applyGateSetLockedProjectionTx(ctx, tx, Event{
		EventID:     "evt_gate_set_lock_conflict",
		EventType:   eventTypeGateSetLock,
		PayloadJSON: string(conflictPayload),
	}); err == nil || !strings.Contains(err.Error(), "already locked at") {
		t.Fatalf("expected gate set already locked error, got %v", err)
	}
}

func TestReplayProjectionsRebuildsGateTemplatesAndGateSets(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-a7b8c9d",
		Type:      "task",
		Title:     "Replay gate projections",
		Actor:     "agent-1",
		CommandID: "cmd-replay-gate-projections-issue-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	template, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "replay-gates",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-replay-gate-projections-template-1",
	})
	if err != nil {
		t.Fatalf("create gate template: %v", err)
	}

	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-a7b8c9d",
		TemplateRefs: []string{"replay-gates@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-replay-gate-projections-set-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-a7b8c9d",
		Actor:     "agent-1",
		CommandID: "cmd-replay-gate-projections-lock-1",
	}); err != nil {
		t.Fatalf("lock gate set: %v", err)
	}

	replay, err := s.ReplayProjections(ctx)
	if err != nil {
		t.Fatalf("replay projections: %v", err)
	}
	if replay.EventsApplied < 4 {
		t.Fatalf("expected replay to apply gate-related events, got %d", replay.EventsApplied)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	replayedTemplate, found, err := gateTemplateByIDVersionTx(ctx, tx, template.TemplateID, template.Version)
	if err != nil {
		t.Fatalf("lookup replayed template: %v", err)
	}
	if !found {
		t.Fatalf("expected replayed template %s@%d", template.TemplateID, template.Version)
	}
	if replayedTemplate.ApprovedBy != "human:alice" || !replayedTemplate.Executable {
		t.Fatalf("expected replayed approved executable template, got %#v", replayedTemplate)
	}

	replayedGateSet, found, err := gateSetForIssueCycleTx(ctx, tx, "mem-a7b8c9d", 1)
	if err != nil {
		t.Fatalf("lookup replayed gate set: %v", err)
	}
	if !found {
		t.Fatal("expected replayed gate set for issue cycle 1")
	}
	if replayedGateSet.GateSetID != gateSet.GateSetID || strings.TrimSpace(replayedGateSet.LockedAt) == "" || len(replayedGateSet.Items) != 1 {
		t.Fatalf("unexpected replayed gate set: %#v", replayedGateSet)
	}

	var activeGateSetID string
	if err := tx.QueryRowContext(ctx, `SELECT active_gate_set_id FROM work_items WHERE id = ?`, "mem-a7b8c9d").Scan(&activeGateSetID); err != nil {
		t.Fatalf("read active_gate_set_id after replay: %v", err)
	}
	if activeGateSetID != gateSet.GateSetID {
		t.Fatalf("expected active_gate_set_id %q after replay, got %q", gateSet.GateSetID, activeGateSetID)
	}
}

func TestGetHumanAuthCredentialTxReturnsStoredCredential(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for missing credential lookup: %v", err)
	}
	credential, found, err := getHumanAuthCredentialTx(ctx, tx)
	if err != nil {
		t.Fatalf("get missing human auth credential tx: %v", err)
	}
	if found || credential.Algorithm != "" {
		t.Fatalf("expected no stored credential, got %#v found=%v", credential, found)
	}
	_ = tx.Rollback()

	if _, _, err := s.UpsertHumanAuthCredential(ctx, UpsertHumanAuthCredentialParams{
		Algorithm:  "pbkdf2-sha256",
		Iterations: 600000,
		SaltHex:    strings.Repeat("a", 32),
		HashHex:    strings.Repeat("b", 64),
		Actor:      "human:alice",
	}); err != nil {
		t.Fatalf("upsert human auth credential: %v", err)
	}

	tx, err = s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	credential, found, err = getHumanAuthCredentialTx(ctx, tx)
	if err != nil {
		t.Fatalf("get human auth credential tx: %v", err)
	}
	if !found || credential.RotatedBy != "human:alice" {
		t.Fatalf("expected stored credential, got %#v found=%v", credential, found)
	}
}

func TestGateCommandRetriesReapplyMissingProjectionsFromEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-f1a2b3c",
		Type:      "task",
		Title:     "Retry projection restoration",
		Actor:     "agent-1",
		CommandID: "cmd-gate-replay-issue-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-f1a2b3c",
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-replay-issue-2",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}

	templateDefinition := `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`
	templateID, err := normalizeGateTemplateID("retry-template")
	if err != nil {
		t.Fatalf("normalize template id: %v", err)
	}
	appliesTo, err := normalizeGateAppliesTo([]string{"task"})
	if err != nil {
		t.Fatalf("normalize applies_to: %v", err)
	}
	definitionJSON, definitionHash, err := canonicalizeGateDefinition(templateDefinition)
	if err != nil {
		t.Fatalf("canonicalize gate definition: %v", err)
	}

	createPayload := gateTemplateCreatedPayload{
		TemplateID:     templateID,
		Version:        1,
		AppliesTo:      appliesTo,
		DefinitionJSON: definitionJSON,
		DefinitionHash: definitionHash,
		CreatedAt:      nowUTC(),
		CreatedBy:      "human:alice",
	}
	appendStoreEventForTest(t, s, entityTypeGateTemplate, gateTemplateEntityID(templateID, 1), eventTypeGateTemplateCreate, createPayload, "human:alice", "cmd-gate-replay-template-1", gateTemplateCorrelationID(templateID, 1))

	template, idempotent, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     templateID,
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: templateDefinition,
		Actor:          "human:alice",
		CommandID:      "cmd-gate-replay-template-1",
	})
	if err != nil {
		t.Fatalf("retry create gate template: %v", err)
	}
	if !idempotent || template.TemplateID != templateID || template.DefinitionHash != definitionHash {
		t.Fatalf("expected idempotent template replay result, got %#v idempotent=%v", template, idempotent)
	}

	approvePayload := gateTemplateApprovedPayload{
		TemplateID:     templateID,
		Version:        1,
		DefinitionHash: definitionHash,
		ApprovedAt:     nowUTC(),
		ApprovedBy:     "human:alice",
	}
	appendStoreEventForTest(t, s, entityTypeGateTemplate, gateTemplateEntityID(templateID, 1), eventTypeGateTemplateApprove, approvePayload, "human:alice", "cmd-gate-replay-approve-1", gateTemplateCorrelationID(templateID, 1))

	approved, approvedIdempotent, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: templateID,
		Version:    1,
		Actor:      "human:alice",
		CommandID:  "cmd-gate-replay-approve-1",
	})
	if err != nil {
		t.Fatalf("retry approve gate template: %v", err)
	}
	if !approvedIdempotent || approved.ApprovedBy != "human:alice" {
		t.Fatalf("expected idempotent approval replay result, got %#v idempotent=%v", approved, approvedIdempotent)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for gate set payload: %v", err)
	}
	issue, err := getIssueTx(ctx, tx, "mem-f1a2b3c")
	if err != nil {
		t.Fatalf("lookup issue for gate set payload: %v", err)
	}
	gates, err := buildGateSetDefinitionsTx(ctx, tx, issue.Type, []gateTemplateRef{{TemplateID: templateID, Version: 1}})
	if err != nil {
		t.Fatalf("build gate set definitions: %v", err)
	}
	frozenJSON, _, err := buildFrozenGateDefinition([]string{templateID + "@1"}, gates)
	if err != nil {
		t.Fatalf("build frozen gate definition: %v", err)
	}
	var frozenDefinition map[string]any
	if err := json.Unmarshal([]byte(frozenJSON), &frozenDefinition); err != nil {
		t.Fatalf("decode frozen definition: %v", err)
	}
	frozenHash := sha256.Sum256([]byte(frozenJSON))
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback payload tx: %v", err)
	}

	gateSetID := "gset_retry_projection"
	gateSetPayload := gateSetInstantiatedPayload{
		GateSetID:        gateSetID,
		IssueID:          "mem-f1a2b3c",
		CycleNo:          1,
		TemplateRefs:     []string{templateID + "@1"},
		FrozenDefinition: frozenDefinition,
		GateSetHash:      hex.EncodeToString(frozenHash[:]),
		CreatedAt:        nowUTC(),
		CreatedBy:        "agent-1",
		Items:            gates,
	}
	appendStoreEventForTest(t, s, entityTypeGateSet, gateSetID, eventTypeGateSetCreate, gateSetPayload, "agent-1", "cmd-gate-replay-instantiate-1", gateCycleCorrelationID("mem-f1a2b3c", 1))

	gateSet, setIdempotent, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-f1a2b3c",
		TemplateRefs: []string{templateID + "@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gate-replay-instantiate-1",
	})
	if err != nil {
		t.Fatalf("retry instantiate gate set: %v", err)
	}
	if !setIdempotent || gateSet.GateSetID != gateSetID || len(gateSet.Items) != 1 {
		t.Fatalf("expected idempotent gate set replay result, got %#v idempotent=%v", gateSet, setIdempotent)
	}

	lockPayload := gateSetLockedPayload{
		GateSetID: gateSetID,
		IssueID:   "mem-f1a2b3c",
		CycleNo:   1,
		LockedAt:  nowUTC(),
	}
	appendStoreEventForTest(t, s, entityTypeGateSet, gateSetID, eventTypeGateSetLock, lockPayload, "agent-1", "cmd-gate-replay-lock-1", gateCycleCorrelationID("mem-f1a2b3c", 1))

	locked, lockedNow, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-f1a2b3c",
		Actor:     "agent-1",
		CommandID: "cmd-gate-replay-lock-1",
	})
	if err != nil {
		t.Fatalf("retry lock gate set: %v", err)
	}
	if lockedNow || strings.TrimSpace(locked.LockedAt) != "" || locked.GateSetID != gateSetID {
		t.Fatalf("expected existing-event retry to return unlocked projected gate set, got %#v lockedNow=%v", locked, lockedNow)
	}
}

func appendStoreEventForTest(t *testing.T, s *Store, entityType, entityID, eventType string, payload any, actor, commandID, correlationID string) {
	t.Helper()

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s payload: %v", eventType, err)
	}

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for %s append: %v", eventType, err)
	}
	defer tx.Rollback()

	res, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityType,
		EntityID:            entityID,
		EventType:           eventType,
		PayloadJSON:         string(payloadBytes),
		Actor:               actor,
		CommandID:           commandID,
		CorrelationID:       correlationID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		t.Fatalf("append %s event: %v", eventType, err)
	}
	if res.AlreadyExists {
		t.Fatalf("expected unique append for %s command %q", eventType, commandID)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit %s event: %v", eventType, err)
	}
}
