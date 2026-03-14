package store

import (
	"context"
	"strings"
	"testing"
)

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
		t.Fatalf("expected existing locked gate set, got %#v lockedAgain=%v", lockedAgain, lockedNow)
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
		IssueID: "mem-c1d2e3f",
		Actor:   "agent-1",
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
