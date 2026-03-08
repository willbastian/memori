package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"reflect"
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

func TestInitializeRejectsInvalidIssueKeyPrefixes(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-store-init-invalid.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Initialize(ctx, InitializeParams{IssueKeyPrefix: "1bad"}); err == nil || !strings.Contains(err.Error(), "invalid issue key prefix") {
		t.Fatalf("expected invalid prefix format error, got %v", err)
	}
	if err := s.Initialize(ctx, InitializeParams{IssueKeyPrefix: "task"}); err == nil || !strings.Contains(err.Error(), "issue key prefix") {
		t.Fatalf("expected embedded type prefix error, got %v", err)
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

func TestGateProjectionFunctionsAreReplayIdempotentAndNormalizeState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-a9c8e7d",
		Type:      "task",
		Title:     "Projection replay idempotency",
		Actor:     "agent-1",
		CommandID: "cmd-projection-replay-issue-1",
	}); err != nil {
		t.Fatalf("create projection issue: %v", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	templatePayload, err := json.Marshal(gateTemplateCreatedPayload{
		TemplateID:     "projection-template",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`,
		CreatedAt:      "2026-03-08T00:00:00Z",
		CreatedBy:      "human:alice",
	})
	if err != nil {
		t.Fatalf("marshal template payload: %v", err)
	}
	templateEvent := Event{
		EventID:     "evt_projection_template_create",
		EventType:   eventTypeGateTemplateCreate,
		PayloadJSON: string(templatePayload),
		CreatedAt:   "2026-03-08T00:00:00Z",
	}

	if err := applyGateTemplateCreatedProjectionTx(ctx, tx, templateEvent); err != nil {
		t.Fatalf("apply gate template projection first time: %v", err)
	}

	template, found, err := gateTemplateByIDVersionTx(ctx, tx, "projection-template", 1)
	if err != nil {
		t.Fatalf("lookup projected template: %v", err)
	}
	if !found {
		t.Fatal("expected projected gate template to exist")
	}
	if template.ApprovedBy != "human:alice" || !template.Executable {
		t.Fatalf("expected human-authored executable template to auto-approve, got %#v", template)
	}

	var approvalRows int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1) FROM gate_template_approvals
		WHERE template_id = ? AND version = ?
	`, "projection-template", 1).Scan(&approvalRows); err != nil {
		t.Fatalf("count template approvals: %v", err)
	}
	if approvalRows != 1 {
		t.Fatalf("expected exactly one auto-approval row, got %d", approvalRows)
	}

	defs, err := buildGateSetDefinitionsTx(ctx, tx, "Task", []gateTemplateRef{{TemplateID: "projection-template", Version: 1}})
	if err != nil {
		t.Fatalf("build gate set definitions: %v", err)
	}
	frozenJSON, frozenObj, err := buildFrozenGateDefinition([]string{"projection-template@1"}, defs)
	if err != nil {
		t.Fatalf("build frozen gate definition: %v", err)
	}
	frozenHash := sha256.Sum256([]byte(frozenJSON))
	gateSetPayload, err := json.Marshal(gateSetInstantiatedPayload{
		GateSetID:        "gset_projection_replay",
		IssueID:          "mem-a9c8e7d",
		CycleNo:          1,
		TemplateRefs:     []string{"projection-template@1"},
		FrozenDefinition: frozenObj,
		GateSetHash:      hex.EncodeToString(frozenHash[:]),
		CreatedAt:        "2026-03-08T00:00:01Z",
		CreatedBy:        "agent-1",
		Items:            defs,
	})
	if err != nil {
		t.Fatalf("marshal gate set payload: %v", err)
	}
	gateSetEvent := Event{
		EventID:     "evt_projection_gate_set_create",
		EventType:   eventTypeGateSetCreate,
		PayloadJSON: string(gateSetPayload),
		CreatedAt:   "2026-03-08T00:00:01Z",
	}

	if err := applyGateSetInstantiatedProjectionTx(ctx, tx, gateSetEvent); err != nil {
		t.Fatalf("apply gate set projection first time: %v", err)
	}

	gateSet, found, err := gateSetByIDTx(ctx, tx, "gset_projection_replay")
	if err != nil {
		t.Fatalf("lookup projected gate set: %v", err)
	}
	if !found {
		t.Fatal("expected projected gate set to exist")
	}
	if gateSet.IssueID != "mem-a9c8e7d" || len(gateSet.Items) != 1 || gateSet.Items[0].GateID != "build" {
		t.Fatalf("unexpected projected gate set contents: %#v", gateSet)
	}

	var gateItemRows int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1) FROM gate_set_items WHERE gate_set_id = ?
	`, "gset_projection_replay").Scan(&gateItemRows); err != nil {
		t.Fatalf("count gate set items: %v", err)
	}
	if gateItemRows != 1 {
		t.Fatalf("expected exactly one projected gate item, got %d", gateItemRows)
	}

	evalPayload, err := json.Marshal(gateEvaluatedPayload{
		IssueID:      "mem-a9c8e7d",
		GateSetID:    "gset_projection_replay",
		GateID:       "build",
		Result:       "PASS",
		EvidenceRefs: []string{" ci://run/1 ", "ci://run/1", "docs://proof"},
	})
	if err != nil {
		t.Fatalf("marshal gate evaluation payload: %v", err)
	}
	evalEvent := Event{
		EventID:     "evt_projection_gate_eval",
		EventType:   eventTypeGateEval,
		PayloadJSON: string(evalPayload),
		CreatedAt:   "2026-03-08T00:00:02Z",
	}

	if err := applyGateEvaluatedProjectionTx(ctx, tx, evalEvent); err != nil {
		t.Fatalf("apply gate evaluation projection: %v", err)
	}

	var (
		result       string
		evidenceJSON string
		evaluatedAt  string
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT result, evidence_refs_json, evaluated_at
		FROM gate_status_projection
		WHERE issue_id = ? AND gate_set_id = ? AND gate_id = ?
	`, "mem-a9c8e7d", "gset_projection_replay", "build").Scan(&result, &evidenceJSON, &evaluatedAt); err != nil {
		t.Fatalf("read projected gate status row: %v", err)
	}
	if result != "PASS" || evaluatedAt != "2026-03-08T00:00:02Z" {
		t.Fatalf("expected event created_at fallback in gate status row, got result=%q evaluated_at=%q", result, evaluatedAt)
	}
	evidenceRefs, err := parseReferencesJSON(evidenceJSON)
	if err != nil {
		t.Fatalf("decode projected evidence refs: %v", err)
	}
	if !reflect.DeepEqual(evidenceRefs, []string{"ci://run/1", "docs://proof"}) {
		t.Fatalf("expected normalized evidence refs, got %#v", evidenceRefs)
	}

	missingGateEvalPayload, err := json.Marshal(gateEvaluatedPayload{
		IssueID:      "mem-a9c8e7d",
		GateSetID:    "gset_projection_replay",
		GateID:       "deploy",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/2"},
	})
	if err != nil {
		t.Fatalf("marshal missing gate evaluation payload: %v", err)
	}
	if err := applyGateEvaluatedProjectionTx(ctx, tx, Event{
		EventID:     "evt_projection_gate_eval_missing",
		EventType:   eventTypeGateEval,
		PayloadJSON: string(missingGateEvalPayload),
		CreatedAt:   "2026-03-08T00:00:03Z",
	}); err == nil || !strings.Contains(err.Error(), `gate "deploy" not found in gate_set "gset_projection_replay"`) {
		t.Fatalf("expected missing gate projection error, got %v", err)
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

func TestReplayProjectionsClearsStaleGateProjectionRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-b7c8d9e",
		Type:      "task",
		Title:     "Replay stale gate projections",
		Actor:     "agent-1",
		CommandID: "cmd-replay-gate-stale-issue-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "stale-replay-gates",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-replay-gate-stale-template-1",
	}); err != nil {
		t.Fatalf("create gate template: %v", err)
	}

	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-b7c8d9e",
		TemplateRefs: []string{"stale-replay-gates@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-replay-gate-stale-set-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-b7c8d9e",
		Actor:     "agent-1",
		CommandID: "cmd-replay-gate-stale-lock-1",
	}); err != nil {
		t.Fatalf("lock gate set: %v", err)
	}
	gateEval, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      "mem-b7c8d9e",
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/stale-replay-1"},
		Actor:        "agent-1",
		CommandID:    "cmd-replay-gate-stale-eval-1",
	})
	if err != nil {
		t.Fatalf("evaluate gate: %v", err)
	}

	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_templates(
			template_id, version, applies_to_json, definition_json, definition_hash, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?)
	`, "stale-template", 9, `["Task"]`, `{"gates":[{"id":"docs"}]}`, "stale-template-hash", nowUTC(), "agent-1"); err != nil {
		t.Fatalf("insert stale gate template: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_template_approvals(template_id, version, approved_at, approved_by)
		VALUES(?, ?, ?, ?)
	`, "stale-template", 9, nowUTC(), "human:alice"); err != nil {
		t.Fatalf("insert stale gate template approval: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "gset_stale_projection", "mem-b7c8d9e", 99, `["stale-template@9"]`, `{"gates":[{"id":"docs"}]}`, "gset_stale_projection_hash", nowUTC(), nowUTC(), "agent-1"); err != nil {
		t.Fatalf("insert stale gate set: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES(?, ?, ?, ?, ?)
	`, "gset_stale_projection", "docs", "check", 1, `{"ref":"manual-validation"}`); err != nil {
		t.Fatalf("insert stale gate set item: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_status_projection(
			issue_id, gate_set_id, gate_id, result, evidence_refs_json, evaluated_at, updated_at, last_event_id
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
	`, "mem-b7c8d9e", "gset_stale_projection", "docs", "PASS", `["docs://stale"]`, nowUTC(), nowUTC(), "evt_stale_gate_status"); err != nil {
		t.Fatalf("insert stale gate status projection: %v", err)
	}

	replay, err := s.ReplayProjections(ctx)
	if err != nil {
		t.Fatalf("replay projections: %v", err)
	}
	if replay.EventsApplied < 5 {
		t.Fatalf("expected replay to apply gate workflow events, got %d", replay.EventsApplied)
	}

	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM gate_templates WHERE template_id = ?`, "stale-template").Scan(&count); err != nil {
		t.Fatalf("count stale template rows after replay: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected stale gate template rows to be cleared, got %d", count)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM gate_sets WHERE gate_set_id = ?`, "gset_stale_projection").Scan(&count); err != nil {
		t.Fatalf("count stale gate set rows after replay: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected stale gate set rows to be cleared, got %d", count)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM gate_status_projection WHERE gate_set_id = ?`, "gset_stale_projection").Scan(&count); err != nil {
		t.Fatalf("count stale gate status rows after replay: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected stale gate status rows to be cleared, got %d", count)
	}

	status, err := s.GetGateStatus(ctx, "mem-b7c8d9e")
	if err != nil {
		t.Fatalf("get gate status after replay: %v", err)
	}
	if status.GateSetID != gateSet.GateSetID || len(status.Gates) != 1 {
		t.Fatalf("unexpected gate status after replay: %#v", status)
	}
	if status.Gates[0].GateID != "build" || status.Gates[0].Result != gateEval.Result {
		t.Fatalf("expected replayed gate evaluation to survive stale cleanup, got %#v", status.Gates[0])
	}
	if !reflect.DeepEqual(status.Gates[0].EvidenceRefs, gateEval.EvidenceRefs) {
		t.Fatalf("expected replayed evidence refs %#v, got %#v", gateEval.EvidenceRefs, status.Gates[0].EvidenceRefs)
	}
}

func TestReplayProjectionsSurfacesProjectionCleanupAndEventQueryFailures(t *testing.T) {
	t.Parallel()

	t.Run("missing gate status projection table", func(t *testing.T) {
		t.Parallel()

		s := newTestStore(t)
		ctx := context.Background()
		if _, err := s.db.ExecContext(ctx, `DROP TABLE gate_status_projection`); err != nil {
			t.Fatalf("drop gate_status_projection: %v", err)
		}

		if _, err := s.ReplayProjections(ctx); err == nil || !strings.Contains(err.Error(), "clear gate_status_projection") {
			t.Fatalf("expected replay cleanup error for gate_status_projection, got %v", err)
		}
	})

	t.Run("missing events table", func(t *testing.T) {
		t.Parallel()

		s := newTestStore(t)
		ctx := context.Background()
		if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
			IssueID:   "mem-c8d9e0f",
			Type:      "task",
			Title:     "Replay missing events table",
			Actor:     "agent-1",
			CommandID: "cmd-replay-missing-events-1",
		}); err != nil {
			t.Fatalf("create issue before dropping events table: %v", err)
		}
		if _, err := s.db.ExecContext(ctx, `DROP TABLE events`); err != nil {
			t.Fatalf("drop events table: %v", err)
		}

		if _, err := s.ReplayProjections(ctx); err == nil || !strings.Contains(err.Error(), "query events for replay") {
			t.Fatalf("expected replay query events error, got %v", err)
		}
	})
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

func TestApproveGateTemplateRetryFailsWhenTemplateProjectionIsMissing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	template, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "approval-missing-template",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`,
		Actor:          "llm:openai:gpt-5",
		CommandID:      "cmd-approval-missing-template-create-1",
	})
	if err != nil {
		t.Fatalf("create executable template: %v", err)
	}

	if _, _, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: template.TemplateID,
		Version:    template.Version,
		Actor:      "human:alice",
		CommandID:  "cmd-approval-missing-template-approve-1",
	}); err != nil {
		t.Fatalf("approve executable template: %v", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin projection cleanup tx: %v", err)
	}
	if err := dropReplayProjectionDeleteTriggersTx(ctx, tx); err != nil {
		t.Fatalf("drop replay delete triggers: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_template_approvals WHERE template_id = ? AND version = ?`, template.TemplateID, template.Version); err != nil {
		t.Fatalf("delete gate template approval projection: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_templates WHERE template_id = ? AND version = ?`, template.TemplateID, template.Version); err != nil {
		t.Fatalf("delete gate template projection: %v", err)
	}
	if err := restoreReplayProjectionDeleteTriggersTx(ctx, tx); err != nil {
		t.Fatalf("restore replay delete triggers: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit projection cleanup tx: %v", err)
	}

	if _, _, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: template.TemplateID,
		Version:    template.Version,
		Actor:      "human:alice",
		CommandID:  "cmd-approval-missing-template-approve-1",
	}); err == nil || !strings.Contains(err.Error(), `template approval-missing-template@1 not found`) {
		t.Fatalf("expected approval replay to fail without template projection, got %v", err)
	}
}

func TestLockGateSetRetryFailsWhenGateSetProjectionIsMissing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-c0ffee1",
		Type:      "task",
		Title:     "Lock retry missing projection",
		Actor:     "agent-1",
		CommandID: "cmd-lock-missing-projection-issue-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	template, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "lock-missing-projection",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"docs","kind":"check","required":true,"criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-lock-missing-projection-template-1",
	})
	if err != nil {
		t.Fatalf("create template: %v", err)
	}

	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-c0ffee1",
		TemplateRefs: []string{template.TemplateID + "@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-lock-missing-projection-instantiate-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}

	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-c0ffee1",
		Actor:     "agent-1",
		CommandID: "cmd-lock-missing-projection-lock-1",
	}); err != nil {
		t.Fatalf("lock gate set: %v", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin gate set cleanup tx: %v", err)
	}
	if err := dropReplayProjectionDeleteTriggersTx(ctx, tx); err != nil {
		t.Fatalf("drop replay delete triggers: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_set_items WHERE gate_set_id = ?`, gateSet.GateSetID); err != nil {
		t.Fatalf("delete gate set items projection: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_sets WHERE gate_set_id = ?`, gateSet.GateSetID); err != nil {
		t.Fatalf("delete gate set projection: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE work_items SET active_gate_set_id = NULL WHERE id = ?`, "mem-c0ffee1"); err != nil {
		t.Fatalf("clear active gate set reference: %v", err)
	}
	if err := restoreReplayProjectionDeleteTriggersTx(ctx, tx); err != nil {
		t.Fatalf("restore replay delete triggers: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit gate set cleanup tx: %v", err)
	}

	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-c0ffee1",
		Actor:     "agent-1",
		CommandID: "cmd-lock-missing-projection-lock-1",
	}); err == nil || !strings.Contains(err.Error(), `gate set "`+gateSet.GateSetID+`" not found`) {
		t.Fatalf("expected lock replay to fail without gate set projection, got %v", err)
	}
}

func TestReplayProjectionsRestoresImmutableDeleteTriggers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-d00df00",
		Type:      "task",
		Title:     "Replay restore delete guards",
		Actor:     "agent-1",
		CommandID: "cmd-replay-delete-guards-issue-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	template, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "replay-delete-guards",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"docs","kind":"check","required":true,"criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-replay-delete-guards-template-1",
	})
	if err != nil {
		t.Fatalf("create template: %v", err)
	}

	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-d00df00",
		TemplateRefs: []string{template.TemplateID + "@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-replay-delete-guards-instantiate-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}

	if _, err := s.ReplayProjections(ctx); err != nil {
		t.Fatalf("replay projections: %v", err)
	}

	if _, err := s.db.ExecContext(ctx, `DELETE FROM gate_templates WHERE template_id = ? AND version = ?`, template.TemplateID, template.Version); err == nil || !strings.Contains(err.Error(), "gate_templates are immutable") {
		t.Fatalf("expected gate_templates delete guard after replay, got %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM gate_sets WHERE gate_set_id = ?`, gateSet.GateSetID); err == nil || !strings.Contains(err.Error(), "gate_sets are immutable") {
		t.Fatalf("expected gate_sets delete guard after replay, got %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM gate_set_items WHERE gate_set_id = ?`, gateSet.GateSetID); err == nil || !strings.Contains(err.Error(), "gate_set_items are immutable") {
		t.Fatalf("expected gate_set_items delete guard after replay, got %v", err)
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
