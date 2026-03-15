package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

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

func TestGateProjectionFunctionsRemainStableOnDuplicateReplay(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-b1c2d3e",
		Type:      "task",
		Title:     "Projection duplicate replay stability",
		Actor:     "agent-1",
		CommandID: "cmd-projection-duplicate-issue-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	definitionJSON, definitionHash, err := canonicalizeGateDefinition(`{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}},{"id":"docs","kind":"check","required":false,"criteria":{"ref":"manual-validation"}}]}`)
	if err != nil {
		t.Fatalf("canonicalize gate definition: %v", err)
	}

	templatePayload, err := json.Marshal(gateTemplateCreatedPayload{
		TemplateID:     "projection-duplicate-template",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: definitionJSON,
		DefinitionHash: definitionHash,
		CreatedAt:      "2026-03-08T00:10:00Z",
		CreatedBy:      "llm:openai:gpt-5",
	})
	if err != nil {
		t.Fatalf("marshal template payload: %v", err)
	}
	templateEvent := Event{
		EventID:     "evt_projection_duplicate_template_create",
		EventType:   eventTypeGateTemplateCreate,
		PayloadJSON: string(templatePayload),
		CreatedAt:   "2026-03-08T00:10:00Z",
	}

	if err := applyGateTemplateCreatedProjectionTx(ctx, tx, templateEvent); err != nil {
		t.Fatalf("apply template projection first time: %v", err)
	}
	if err := applyGateTemplateCreatedProjectionTx(ctx, tx, templateEvent); err != nil {
		t.Fatalf("apply template projection second time: %v", err)
	}

	var templateRows int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1) FROM gate_templates
		WHERE template_id = ? AND version = ?
	`, "projection-duplicate-template", 1).Scan(&templateRows); err != nil {
		t.Fatalf("count template rows: %v", err)
	}
	if templateRows != 1 {
		t.Fatalf("expected one template row after duplicate replay, got %d", templateRows)
	}

	var approvalRows int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1) FROM gate_template_approvals
		WHERE template_id = ? AND version = ?
	`, "projection-duplicate-template", 1).Scan(&approvalRows); err != nil {
		t.Fatalf("count approval rows before approval event: %v", err)
	}
	if approvalRows != 0 {
		t.Fatalf("expected no auto-approval rows for llm-authored template, got %d", approvalRows)
	}

	approvalPayload, err := json.Marshal(gateTemplateApprovedPayload{
		TemplateID:     "projection-duplicate-template",
		Version:        1,
		DefinitionHash: definitionHash,
		ApprovedAt:     "2026-03-08T00:11:00Z",
		ApprovedBy:     "human:alice",
	})
	if err != nil {
		t.Fatalf("marshal approval payload: %v", err)
	}
	approvalEvent := Event{
		EventID:     "evt_projection_duplicate_template_approve",
		EventType:   eventTypeGateTemplateApprove,
		PayloadJSON: string(approvalPayload),
		CreatedAt:   "2026-03-08T00:11:00Z",
	}

	if err := applyGateTemplateApprovedProjectionTx(ctx, tx, approvalEvent); err != nil {
		t.Fatalf("apply approval projection first time: %v", err)
	}
	if err := applyGateTemplateApprovedProjectionTx(ctx, tx, approvalEvent); err != nil {
		t.Fatalf("apply approval projection second time: %v", err)
	}

	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1) FROM gate_template_approvals
		WHERE template_id = ? AND version = ?
	`, "projection-duplicate-template", 1).Scan(&approvalRows); err != nil {
		t.Fatalf("count approval rows after duplicate approval replay: %v", err)
	}
	if approvalRows != 1 {
		t.Fatalf("expected one approval row after duplicate approval replay, got %d", approvalRows)
	}

	defs, err := buildGateSetDefinitionsTx(ctx, tx, "Task", []gateTemplateRef{{TemplateID: "projection-duplicate-template", Version: 1}})
	if err != nil {
		t.Fatalf("build gate set definitions: %v", err)
	}
	frozenJSON, frozenObj, err := buildFrozenGateDefinition([]string{"projection-duplicate-template@1"}, defs)
	if err != nil {
		t.Fatalf("build frozen gate definition: %v", err)
	}
	frozenHash := sha256.Sum256([]byte(frozenJSON))

	gateSetPayload, err := json.Marshal(gateSetInstantiatedPayload{
		GateSetID:        "gset_projection_duplicate",
		IssueID:          "mem-b1c2d3e",
		CycleNo:          1,
		TemplateRefs:     []string{"projection-duplicate-template@1"},
		FrozenDefinition: frozenObj,
		GateSetHash:      hex.EncodeToString(frozenHash[:]),
		CreatedAt:        "2026-03-08T00:12:00Z",
		CreatedBy:        "agent-1",
		Items:            defs,
	})
	if err != nil {
		t.Fatalf("marshal gate set payload: %v", err)
	}
	gateSetEvent := Event{
		EventID:     "evt_projection_duplicate_gate_set_create",
		EventType:   eventTypeGateSetCreate,
		PayloadJSON: string(gateSetPayload),
		CreatedAt:   "2026-03-08T00:12:00Z",
	}

	if err := applyGateSetInstantiatedProjectionTx(ctx, tx, gateSetEvent); err != nil {
		t.Fatalf("apply gate set projection first time: %v", err)
	}
	if err := applyGateSetInstantiatedProjectionTx(ctx, tx, gateSetEvent); err != nil {
		t.Fatalf("apply gate set projection second time: %v", err)
	}

	var gateSetRows int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1) FROM gate_sets WHERE gate_set_id = ?
	`, "gset_projection_duplicate").Scan(&gateSetRows); err != nil {
		t.Fatalf("count gate set rows: %v", err)
	}
	if gateSetRows != 1 {
		t.Fatalf("expected one gate set row after duplicate replay, got %d", gateSetRows)
	}

	var gateItemRows int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1) FROM gate_set_items WHERE gate_set_id = ?
	`, "gset_projection_duplicate").Scan(&gateItemRows); err != nil {
		t.Fatalf("count gate set item rows: %v", err)
	}
	if gateItemRows != 2 {
		t.Fatalf("expected two gate set items after duplicate replay, got %d", gateItemRows)
	}

	projectedGateSet, found, err := gateSetByIDTx(ctx, tx, "gset_projection_duplicate")
	if err != nil {
		t.Fatalf("lookup projected gate set: %v", err)
	}
	if !found {
		t.Fatal("expected projected gate set after duplicate replay")
	}
	if len(projectedGateSet.Items) != 2 {
		t.Fatalf("expected two projected gate items, got %#v", projectedGateSet.Items)
	}
	if projectedGateSet.Items[0].GateID != "build" || !projectedGateSet.Items[0].Required {
		t.Fatalf("expected required build gate, got %#v", projectedGateSet.Items[0])
	}
	if projectedGateSet.Items[1].GateID != "docs" || projectedGateSet.Items[1].Required {
		t.Fatalf("expected optional docs gate, got %#v", projectedGateSet.Items[1])
	}

	lockPayload, err := json.Marshal(gateSetLockedPayload{
		GateSetID: "gset_projection_duplicate",
		IssueID:   "mem-b1c2d3e",
		CycleNo:   1,
		LockedAt:  "2026-03-08T00:13:00Z",
	})
	if err != nil {
		t.Fatalf("marshal lock payload: %v", err)
	}
	lockEvent := Event{
		EventID:     "evt_projection_duplicate_gate_set_lock",
		EventType:   eventTypeGateSetLock,
		PayloadJSON: string(lockPayload),
		CreatedAt:   "2026-03-08T00:13:00Z",
	}

	if err := applyGateSetLockedProjectionTx(ctx, tx, lockEvent); err != nil {
		t.Fatalf("apply lock projection first time: %v", err)
	}
	if err := applyGateSetLockedProjectionTx(ctx, tx, lockEvent); err != nil {
		t.Fatalf("apply lock projection second time: %v", err)
	}

	projectedGateSet, found, err = gateSetByIDTx(ctx, tx, "gset_projection_duplicate")
	if err != nil {
		t.Fatalf("lookup locked gate set: %v", err)
	}
	if !found || projectedGateSet.LockedAt != "2026-03-08T00:13:00Z" {
		t.Fatalf("expected stable locked gate set after duplicate lock replay, got %#v found=%v", projectedGateSet, found)
	}

	var activeGateSetID string
	if err := tx.QueryRowContext(ctx, `
		SELECT active_gate_set_id FROM work_items WHERE id = ?
	`, "mem-b1c2d3e").Scan(&activeGateSetID); err != nil {
		t.Fatalf("read active gate set id: %v", err)
	}
	if activeGateSetID != "gset_projection_duplicate" {
		t.Fatalf("expected active gate set id %q, got %q", "gset_projection_duplicate", activeGateSetID)
	}
}
