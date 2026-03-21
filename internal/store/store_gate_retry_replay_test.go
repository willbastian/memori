package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

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
	if lockedNow {
		t.Fatal("expected existing-event retry to report idempotent lock replay")
	}
	if strings.TrimSpace(locked.LockedAt) == "" || locked.GateSetID != gateSetID || len(locked.Items) != 1 {
		t.Fatalf("expected existing-event retry to restore locked gate set projection, got %#v", locked)
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
