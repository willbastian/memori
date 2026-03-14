package store

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestEvaluateGateAppendsEventAndUpdatesProjection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-4545454"
	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Gate evaluation projection test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-eval-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-eval-progress-1",
	})
	if err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}

	gateSetID := "gs_eval_1"
	seedLockedGateSetForTest(t, s, issueID, gateSetID)
	seedGateSetItemForTest(t, s, gateSetID, "build", "check", 1)
	seedGateSetItemForTest(t, s, gateSetID, "lint", "check", 0)

	evaluation, event, idempotent, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      issueID,
		GateID:       "build",
		Result:       "pass",
		EvidenceRefs: []string{"go test ./...", " go test ./... ", "ci://run/1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gate-eval-1",
	})
	if err != nil {
		t.Fatalf("evaluate gate: %v", err)
	}
	if idempotent {
		t.Fatalf("first gate evaluation should not be idempotent")
	}
	if event.EventType != eventTypeGateEval {
		t.Fatalf("expected gate.evaluated event, got %s", event.EventType)
	}
	if evaluation.Result != "PASS" {
		t.Fatalf("expected normalized PASS result, got %q", evaluation.Result)
	}
	if !reflect.DeepEqual(evaluation.EvidenceRefs, []string{"go test ./...", "ci://run/1"}) {
		t.Fatalf("unexpected normalized evidence refs: %#v", evaluation.EvidenceRefs)
	}

	status, err := s.GetGateStatus(ctx, issueID)
	if err != nil {
		t.Fatalf("get gate status: %v", err)
	}
	if status.GateSetID != gateSetID {
		t.Fatalf("expected gate_set_id %q, got %q", gateSetID, status.GateSetID)
	}
	if len(status.Gates) != 2 {
		t.Fatalf("expected 2 gate status rows, got %d", len(status.Gates))
	}
	if status.Gates[0].GateID != "build" || status.Gates[0].Result != "PASS" {
		t.Fatalf("expected build gate PASS, got %#v", status.Gates[0])
	}
	if status.Gates[1].GateID != "lint" || status.Gates[1].Result != "MISSING" {
		t.Fatalf("expected lint gate MISSING, got %#v", status.Gates[1])
	}
}

func TestGetGateStatusSupportsCycleSelection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5858585"
	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Gate status cycle selection test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-cycle-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-cycle-progress-1",
	})
	if err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "gs_cycle_1", issueID, 1, `["tmpl-default@1"]`, `{"gates":[{"id":"build"}]}`, "gs_cycle_hash_1", nowUTC(), nowUTC(), "agent-1")
	if err != nil {
		t.Fatalf("insert cycle 1 gate set: %v", err)
	}
	seedGateSetItemForTest(t, s, "gs_cycle_1", "build", "check", 1)

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "gs_cycle_2", issueID, 2, `["tmpl-default@2"]`, `{"gates":[{"id":"deploy"}]}`, "gs_cycle_hash_2", nowUTC(), nowUTC(), "agent-1")
	if err != nil {
		t.Fatalf("insert cycle 2 gate set: %v", err)
	}
	seedGateSetItemForTest(t, s, "gs_cycle_2", "deploy", "check", 1)

	defaultStatus, err := s.GetGateStatus(ctx, issueID)
	if err != nil {
		t.Fatalf("get default gate status: %v", err)
	}
	if defaultStatus.CycleNo != 1 || defaultStatus.GateSetID != "gs_cycle_1" {
		t.Fatalf("expected default gate status for current cycle 1, got cycle=%d gate_set_id=%q", defaultStatus.CycleNo, defaultStatus.GateSetID)
	}

	cycleTwo := 2
	historicalStatus, err := s.GetGateStatusForCycle(ctx, GetGateStatusParams{
		IssueID: issueID,
		CycleNo: &cycleTwo,
	})
	if err != nil {
		t.Fatalf("get historical gate status: %v", err)
	}
	if historicalStatus.CycleNo != 2 || historicalStatus.GateSetID != "gs_cycle_2" {
		t.Fatalf("expected cycle 2 gate status, got cycle=%d gate_set_id=%q", historicalStatus.CycleNo, historicalStatus.GateSetID)
	}
	if len(historicalStatus.Gates) != 1 || historicalStatus.Gates[0].GateID != "deploy" {
		t.Fatalf("expected deploy gate for cycle 2, got %#v", historicalStatus.Gates)
	}

	cycleThree := 3
	_, err = s.GetGateStatusForCycle(ctx, GetGateStatusParams{
		IssueID: issueID,
		CycleNo: &cycleThree,
	})
	if err == nil || !strings.Contains(err.Error(), `no locked gate set found for issue "mem-5858585" cycle 3`) {
		t.Fatalf("expected cycle not found error, got: %v", err)
	}
}

func TestEvaluateGateRequiresEvidenceAndKnownGate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5656565"
	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Gate validation test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-validate-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-validate-progress-1",
	})
	if err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	gateSetID := "gs_eval_2"
	seedLockedGateSetForTest(t, s, issueID, gateSetID)
	seedGateSetItemForTest(t, s, gateSetID, "build", "check", 1)

	_, _, _, err = s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:   issueID,
		GateID:    "build",
		Result:    "PASS",
		Actor:     "agent-1",
		CommandID: "cmd-gate-validate-no-evidence-1",
	})
	if err == nil || !strings.Contains(err.Error(), "--evidence is required") {
		t.Fatalf("expected missing evidence error, got: %v", err)
	}

	_, _, _, err = s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      issueID,
		GateID:       "not-defined",
		Result:       "PASS",
		EvidenceRefs: []string{"ci://run/2"},
		Actor:        "agent-1",
		CommandID:    "cmd-gate-validate-missing-gate-1",
	})
	if err == nil || !strings.Contains(err.Error(), `gate "not-defined" is not defined`) {
		t.Fatalf("expected missing gate definition error, got: %v", err)
	}
}

func TestLookupGateVerificationSpecReturnsLockedCriteriaCommand(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5757575"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Gate verify lookup test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-verify-spec-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-verify-spec-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	seedGateTemplateRowForTest(t, s, "tmpl-default", 1, []string{"Task"}, `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`, "human:alice")

	gateSetID := "gs_verify_spec_1"
	seedLockedGateSetForTest(t, s, issueID, gateSetID)
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES(?, ?, ?, ?, ?)
	`, gateSetID, "build", "check", 1, `{"command":"go test ./..."}`); err != nil {
		t.Fatalf("insert gate_set_item with command criteria: %v", err)
	}

	spec, err := s.LookupGateVerificationSpec(ctx, issueID, "build")
	if err != nil {
		t.Fatalf("lookup gate verification spec: %v", err)
	}
	if spec.GateSetID != gateSetID {
		t.Fatalf("expected gate_set_id %q, got %q", gateSetID, spec.GateSetID)
	}
	if spec.Command != "go test ./..." {
		t.Fatalf("expected verifier command, got %q", spec.Command)
	}
	if spec.GateSetHash == "" {
		t.Fatalf("expected non-empty gate_set_hash in verification spec")
	}
}

func TestLookupGateVerificationSpecGovernanceFallbacksAndDrift(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("legacy human created gate set without template refs is allowed", func(t *testing.T) {
		t.Parallel()

		s := newTestStore(t)
		issueID := "mem-5787878"
		if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
			IssueID:   issueID,
			Type:      "task",
			Title:     "Legacy gate set provenance",
			Actor:     "agent-1",
			CommandID: "cmd-gate-verify-legacy-create-1",
		}); err != nil {
			t.Fatalf("create issue: %v", err)
		}
		seedLockedGateSetWithProvenanceForTest(
			t,
			s,
			issueID,
			"gs_verify_legacy_1",
			`[]`,
			`{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`,
			"human:alice",
		)
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
			VALUES(?, ?, ?, ?, ?)
		`, "gs_verify_legacy_1", "build", "check", 1, `{"command":"go test ./..."}`); err != nil {
			t.Fatalf("insert legacy gate_set_item: %v", err)
		}

		spec, err := s.LookupGateVerificationSpec(ctx, issueID, "build")
		if err != nil {
			t.Fatalf("lookup gate verification spec: %v", err)
		}
		if spec.GateSetID != "gs_verify_legacy_1" || spec.Command != "go test ./..." {
			t.Fatalf("unexpected verification spec: %#v", spec)
		}
	})

	t.Run("missing or drifted template provenance is rejected", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name             string
			gateSetID        string
			templateRefsJSON string
			createdBy        string
			setup            func(t *testing.T, s *Store)
			criteriaJSON     string
			wantErr          string
		}{
			{
				name:             "no refs non human creator",
				gateSetID:        "gs_verify_missing_refs",
				templateRefsJSON: `[]`,
				createdBy:        "llm:openai:gpt-5",
				criteriaJSON:     `{"command":"go test ./..."}`,
				wantErr:          "without approved template provenance",
			},
			{
				name:             "missing template row",
				gateSetID:        "gs_verify_missing_template",
				templateRefsJSON: `["missing@1"]`,
				createdBy:        "agent-1",
				criteriaJSON:     `{"command":"go test ./..."}`,
				wantErr:          "references missing template missing@1",
			},
			{
				name:             "command mismatch from approved template",
				gateSetID:        "gs_verify_command_mismatch",
				templateRefsJSON: `["tmpl-default@1"]`,
				createdBy:        "agent-1",
				setup: func(t *testing.T, s *Store) {
					t.Helper()
					seedGateTemplateRowForTest(t, s, "tmpl-default", 1, []string{"Task"}, `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`, "human:alice")
				},
				criteriaJSON: `{"command":"go test ./internal/store"}`,
				wantErr:      "command does not match approved template provenance",
			},
			{
				name:             "existing but unapproved executable template",
				gateSetID:        "gs_verify_unapproved_template",
				templateRefsJSON: `["tmpl-pending@1"]`,
				createdBy:        "agent-1",
				setup: func(t *testing.T, s *Store) {
					t.Helper()
					seedGateTemplateRowForTest(t, s, "tmpl-pending", 1, []string{"Task"}, `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`, "llm:openai:gpt-5")
				},
				criteriaJSON: `{"command":"go test ./..."}`,
				wantErr:      "uses executable criteria.command from unapproved template tmpl-pending@1",
			},
		}

		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				s := newTestStore(t)
				issueID := "mem-5797979"
				if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
					IssueID:   issueID,
					Type:      "task",
					Title:     "Gate provenance drift",
					Actor:     "agent-1",
					CommandID: "cmd-gate-verify-drift-create-" + tc.gateSetID,
				}); err != nil {
					t.Fatalf("create issue: %v", err)
				}
				if tc.setup != nil {
					tc.setup(t, s)
				}
				seedLockedGateSetWithProvenanceForTest(
					t,
					s,
					issueID,
					tc.gateSetID,
					tc.templateRefsJSON,
					`{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`,
					tc.createdBy,
				)
				if _, err := s.db.ExecContext(ctx, `
					INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
					VALUES(?, ?, ?, ?, ?)
				`, tc.gateSetID, "build", "check", 1, tc.criteriaJSON); err != nil {
					t.Fatalf("insert gate_set_item: %v", err)
				}

				if _, err := s.LookupGateVerificationSpec(ctx, issueID, "build"); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected verification governance error %q, got %v", tc.wantErr, err)
				}
			})
		}
	})
}

func TestInstantiateGateSetRejectsExecutableTemplateWithoutHumanGovernance(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5858585"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Unsafe template instantiate test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-unsafe-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-unsafe-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	seedGateTemplateRowForTest(t, s, "unsafe", 1, []string{"Task"}, `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`, "llm:openai:gpt-5")

	_, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"unsafe@1"},
		Actor:        "human:alice",
		CommandID:    "cmd-gate-unsafe-instantiate-1",
	})
	if err == nil || !strings.Contains(err.Error(), "pending human approval") {
		t.Fatalf("expected approval rejection, got: %v", err)
	}
}

func TestApproveGateTemplateAllowsAgentAuthoredExecutableTemplateAfterHumanApproval(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5868686"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Executable template approval workflow",
		Actor:     "agent-1",
		CommandID: "cmd-gate-approve-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-approve-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}

	template, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "agent-authored",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`,
		Actor:          "llm:openai:gpt-5",
		CommandID:      "cmd-gate-approve-template-1",
	})
	if err != nil {
		t.Fatalf("create executable template: %v", err)
	}
	if !template.Executable {
		t.Fatalf("expected executable template")
	}
	if template.ApprovedBy != "" {
		t.Fatalf("expected executable template to start unapproved, got approved_by=%q", template.ApprovedBy)
	}

	_, _, err = s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"agent-authored@1"},
		Actor:        "human:alice",
		CommandID:    "cmd-gate-approve-instantiate-pre-1",
	})
	if err == nil || !strings.Contains(err.Error(), "pending human approval") {
		t.Fatalf("expected pre-approval instantiate rejection, got: %v", err)
	}

	approved, idempotent, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: "agent-authored",
		Version:    1,
		Actor:      "human:alice",
		CommandID:  "cmd-gate-approve-template-approve-1",
	})
	if err != nil {
		t.Fatalf("approve executable template: %v", err)
	}
	if idempotent {
		t.Fatalf("expected first approval to be non-idempotent")
	}
	if approved.ApprovedBy != "human:alice" {
		t.Fatalf("expected approval actor recorded, got %q", approved.ApprovedBy)
	}

	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"agent-authored@1"},
		Actor:        "human:alice",
		CommandID:    "cmd-gate-approve-instantiate-post-1",
	})
	if err != nil {
		t.Fatalf("instantiate approved template: %v", err)
	}
	if gateSet.GateSetID == "" {
		t.Fatalf("expected instantiated gate set id")
	}
}

func TestInstantiateGateSetRejectsRequiredNonExecutableTemplate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5878787"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Manual required gate instantiate test",
		Actor:     "human:alice",
		CommandID: "cmd-gate-manual-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "human:alice",
		CommandID: "cmd-gate-manual-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "manual-required",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"review","kind":"check","required":true,"criteria":{"ref":"manual-review"}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-gate-manual-template-1",
	}); err != nil {
		t.Fatalf("create manual gate template: %v", err)
	}

	_, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"manual-required@1"},
		Actor:        "human:alice",
		CommandID:    "cmd-gate-manual-instantiate-1",
	})
	if err == nil || !strings.Contains(err.Error(), "required gate(s) lack executable criteria.command") {
		t.Fatalf("expected required non-executable gate rejection, got: %v", err)
	}
}

func TestInstantiateGateSetAllowsRequiredManualValidationTemplate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5879797"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Manual validation instantiate test",
		Actor:     "human:alice",
		CommandID: "cmd-gate-manual-validation-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "human:alice",
		CommandID: "cmd-gate-manual-validation-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "manual-validation-required",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"validated","kind":"check","required":true,"criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-gate-manual-validation-template-1",
	}); err != nil {
		t.Fatalf("create manual-validation gate template: %v", err)
	}

	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"manual-validation-required@1"},
		Actor:        "human:alice",
		CommandID:    "cmd-gate-manual-validation-instantiate-1",
	})
	if err != nil {
		t.Fatalf("expected required manual-validation gate set to instantiate: %v", err)
	}
	if gateSet.GateSetID == "" {
		t.Fatalf("expected gate set id")
	}
}

func TestLookupGateVerificationSpecRejectsExecutableCommandFromNonHumanTemplate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5959595"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Unsafe template verify test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-unsafe-verify-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-unsafe-verify-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	seedGateTemplateRowForTest(t, s, "unsafe", 1, []string{"Task"}, `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`, "llm:openai:gpt-5")

	gateSetID := "gs_verify_spec_unsafe"
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, gateSetID, issueID, 1, `["unsafe@1"]`, `{"templates":["unsafe@1"],"gates":[{"gate_id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`, gateSetID+"_hash", nowUTC(), nowUTC(), "llm:openai:gpt-5"); err != nil {
		t.Fatalf("insert unsafe locked gate set: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES(?, ?, ?, ?, ?)
	`, gateSetID, "build", "check", 1, `{"command":"go test ./..."}`); err != nil {
		t.Fatalf("insert gate_set_item with unsafe command criteria: %v", err)
	}

	_, err := s.LookupGateVerificationSpec(ctx, issueID, "build")
	if err == nil || !strings.Contains(err.Error(), "unapproved template") {
		t.Fatalf("expected executable governance rejection, got: %v", err)
	}
}

func TestReplayProjectionsRebuildsGateStatusProjection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-6767676"
	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Replay gate status test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-replay-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}
	_, _, _, err = s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-replay-progress-1",
	})
	if err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	createLockedGateSetEventSourcedForTest(t, s, issueID, "gate-replay", "build", "cmd-gate-replay")

	if _, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      issueID,
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/3"},
		Actor:        "agent-1",
		CommandID:    "cmd-gate-replay-eval-1",
	}); err != nil {
		t.Fatalf("evaluate gate before replay: %v", err)
	}

	if _, err := s.db.ExecContext(ctx, `DELETE FROM gate_status_projection WHERE issue_id = ?`, issueID); err != nil {
		t.Fatalf("clear gate status projection manually: %v", err)
	}

	replay, err := s.ReplayProjections(ctx)
	if err != nil {
		t.Fatalf("replay projections: %v", err)
	}
	if replay.EventsApplied != 6 {
		t.Fatalf("expected replay to apply 6 events, got %d", replay.EventsApplied)
	}

	status, err := s.GetGateStatus(ctx, issueID)
	if err != nil {
		t.Fatalf("get gate status after replay: %v", err)
	}
	if len(status.Gates) != 1 || status.Gates[0].Result != "FAIL" {
		t.Fatalf("expected replayed gate status FAIL, got %#v", status.Gates)
	}
}
