package store

import (
	"context"
	"strings"
	"testing"
)

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

func TestGateCommandsRejectCorruptExistingEventsOnRetry(t *testing.T) {
	t.Parallel()

	t.Run("create gate template", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		s := newTestStore(t)

		payload := `{"template_id":"retry-corrupt-create","version":1,"applies_to":["task"],"definition_json":"{\"gates\":[]}","definition_hash":"deadbeef","created_at":"2026-03-08T12:05:00Z","created_by":"agent-1"}`
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO events(
				event_id, event_order, entity_type, entity_id, entity_seq, event_type,
				payload_json, actor, command_id, causation_id, correlation_id, created_at,
				hash, prev_hash, event_payload_version
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, ?)
		`, "evt_retry_corrupt_create", 1, entityTypeGateTemplate, gateTemplateEntityID("retry-corrupt-create", 1), 1, eventTypeGateTemplateCreate, payload, "agent-1", "cmd-retry-corrupt-create-1", gateTemplateCorrelationID("retry-corrupt-create", 1), nowUTC(), "hash_retry_corrupt_create", 1); err != nil {
			t.Fatalf("insert corrupt create event: %v", err)
		}

		if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
			TemplateID:     "retry-corrupt-create",
			Version:        1,
			AppliesTo:      []string{"task"},
			DefinitionJSON: `{"gates":[]}`,
			Actor:          "agent-1",
			CommandID:      "cmd-retry-corrupt-create-1",
		}); err == nil || !strings.Contains(err.Error(), "definition_hash does not match definition_json") {
			t.Fatalf("expected corrupt existing create event error, got %v", err)
		}
	})

	t.Run("approve gate template", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		s := newTestStore(t)

		payload := `{"template_id":"retry-corrupt-approve","version":1,"definition_hash":"abc123","approved_at":"2026-03-08T12:06:00Z","approved_by":"llm:openai:gpt-5"}`
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO events(
				event_id, event_order, entity_type, entity_id, entity_seq, event_type,
				payload_json, actor, command_id, causation_id, correlation_id, created_at,
				hash, prev_hash, event_payload_version
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, ?)
		`, "evt_retry_corrupt_approve", 1, entityTypeGateTemplate, gateTemplateEntityID("retry-corrupt-approve", 1), 1, eventTypeGateTemplateApprove, payload, "human:alice", "cmd-retry-corrupt-approve-1", gateTemplateCorrelationID("retry-corrupt-approve", 1), nowUTC(), "hash_retry_corrupt_approve", 1); err != nil {
			t.Fatalf("insert corrupt approval event: %v", err)
		}

		if _, _, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
			TemplateID: "retry-corrupt-approve",
			Version:    1,
			Actor:      "human:alice",
			CommandID:  "cmd-retry-corrupt-approve-1",
		}); err == nil || !strings.Contains(err.Error(), "approved_by must be human-governed") {
			t.Fatalf("expected corrupt existing approval event error, got %v", err)
		}
	})

	t.Run("instantiate gate set", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		s := newTestStore(t)

		payload := `{"gate_set_id":"gset_retry_corrupt","issue_id":"mem-a1b2c3d","cycle_no":1,"template_refs":["retry-corrupt-template@1"],"frozen_definition":{"templates":["retry-corrupt-template@1"],"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]},"gate_set_hash":"deadbeef","created_at":"2026-03-08T12:07:00Z","created_by":"agent-1","items":[{"gate_id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO events(
				event_id, event_order, entity_type, entity_id, entity_seq, event_type,
				payload_json, actor, command_id, causation_id, correlation_id, created_at,
				hash, prev_hash, event_payload_version
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, ?)
		`, "evt_retry_corrupt_instantiate", 1, entityTypeGateSet, "gset_retry_corrupt", 1, eventTypeGateSetCreate, payload, "agent-1", "cmd-retry-corrupt-instantiate-1", gateCycleCorrelationID("mem-a1b2c3d", 1), nowUTC(), "hash_retry_corrupt_instantiate", 1); err != nil {
			t.Fatalf("insert corrupt instantiate event: %v", err)
		}

		if _, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
			IssueID:      "mem-a1b2c3d",
			TemplateRefs: []string{"retry-corrupt-template@1"},
			Actor:        "agent-1",
			CommandID:    "cmd-retry-corrupt-instantiate-1",
		}); err == nil || !strings.Contains(err.Error(), "gate_set_hash does not match frozen definition") {
			t.Fatalf("expected corrupt existing instantiate event error, got %v", err)
		}
	})

	t.Run("lock gate set", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		s := newTestStore(t)

		payload := `{"gate_set_id":"gset_retry_corrupt_lock","issue_id":"mem-a1b2c3d","cycle_no":1,"locked_at":" "}`
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO events(
				event_id, event_order, entity_type, entity_id, entity_seq, event_type,
				payload_json, actor, command_id, causation_id, correlation_id, created_at,
				hash, prev_hash, event_payload_version
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, ?)
		`, "evt_retry_corrupt_lock", 1, entityTypeGateSet, "gset_retry_corrupt_lock", 1, eventTypeGateSetLock, payload, "agent-1", "cmd-retry-corrupt-lock-1", gateCycleCorrelationID("mem-a1b2c3d", 1), nowUTC(), "hash_retry_corrupt_lock", 1); err != nil {
			t.Fatalf("insert corrupt lock event: %v", err)
		}

		if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
			IssueID:   "mem-a1b2c3d",
			Actor:     "agent-1",
			CommandID: "cmd-retry-corrupt-lock-1",
		}); err == nil || !strings.Contains(err.Error(), "locked_at is required") {
			t.Fatalf("expected corrupt existing lock event error, got %v", err)
		}
	})
}
