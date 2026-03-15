package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

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

func TestReplayProjectionsRejectsCorruptGateLedgerEvents(t *testing.T) {
	t.Parallel()

	t.Run("template create definition hash mismatch", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		s := newTestStore(t)

		payload := `{"template_id":"replay-corrupt-create","version":1,"applies_to":["task"],"definition_json":"{\"gates\":[{\"id\":\"build\",\"kind\":\"check\",\"required\":true,\"criteria\":{\"command\":\"go test ./...\"}}]}","definition_hash":"deadbeef","created_at":"2026-03-08T12:01:00Z","created_by":"human:alice"}`
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO events(
				event_id, event_order, entity_type, entity_id, entity_seq, event_type,
				payload_json, actor, command_id, causation_id, correlation_id, created_at,
				hash, prev_hash, event_payload_version
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, ?)
		`, "evt_replay_corrupt_template_create", 1, entityTypeGateTemplate, gateTemplateEntityID("replay-corrupt-create", 1), 1, eventTypeGateTemplateCreate, payload, "human:alice", "cmd-replay-corrupt-template-create-1", gateTemplateCorrelationID("replay-corrupt-create", 1), nowUTC(), "hash_replay_corrupt_template_create", 1); err != nil {
			t.Fatalf("insert corrupt template create event: %v", err)
		}

		if _, err := s.ReplayProjections(ctx); err == nil || !strings.Contains(err.Error(), "definition_hash does not match definition_json") {
			t.Fatalf("expected replay corruption error for template create hash mismatch, got %v", err)
		}
	})

	t.Run("approval hash mismatch", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		s := newTestStore(t)

		template, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
			TemplateID:     "replay-corrupt-approval",
			Version:        1,
			AppliesTo:      []string{"task"},
			DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`,
			Actor:          "llm:openai:gpt-5",
			CommandID:      "cmd-replay-corrupt-approval-create-1",
		})
		if err != nil {
			t.Fatalf("create template: %v", err)
		}

		var nextOrder int
		if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(event_order), 0) + 1 FROM events`).Scan(&nextOrder); err != nil {
			t.Fatalf("read next event order: %v", err)
		}

		payload := `{"template_id":"replay-corrupt-approval","version":1,"definition_hash":"deadbeef","approved_at":"2026-03-08T12:03:00Z","approved_by":"human:alice"}`
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO events(
				event_id, event_order, entity_type, entity_id, entity_seq, event_type,
				payload_json, actor, command_id, causation_id, correlation_id, created_at,
				hash, prev_hash, event_payload_version
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, ?)
		`, "evt_replay_corrupt_approval", nextOrder, entityTypeGateTemplate, gateTemplateEntityID(template.TemplateID, template.Version), 2, eventTypeGateTemplateApprove, payload, "human:alice", "cmd-replay-corrupt-approval-approve-1", gateTemplateCorrelationID(template.TemplateID, template.Version), nowUTC(), "hash_replay_corrupt_approval", 1); err != nil {
			t.Fatalf("insert corrupt approval event: %v", err)
		}

		if _, err := s.ReplayProjections(ctx); err == nil || !strings.Contains(err.Error(), "definition hash mismatch") {
			t.Fatalf("expected replay corruption error for approval hash mismatch, got %v", err)
		}
	})

	t.Run("approval requires human-governed approver", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		s := newTestStore(t)

		definitionJSON, definitionHash, err := canonicalizeGateDefinition(`{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`)
		if err != nil {
			t.Fatalf("canonicalize gate definition: %v", err)
		}
		createPayloadBytes, err := json.Marshal(gateTemplateCreatedPayload{
			TemplateID:     "replay-corrupt-nonhuman-approval",
			Version:        1,
			AppliesTo:      []string{"task"},
			DefinitionJSON: definitionJSON,
			DefinitionHash: definitionHash,
			CreatedAt:      "2026-03-08T12:02:30Z",
			CreatedBy:      "llm:openai:gpt-5",
		})
		if err != nil {
			t.Fatalf("marshal template create payload: %v", err)
		}
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO events(
				event_id, event_order, entity_type, entity_id, entity_seq, event_type,
				payload_json, actor, command_id, causation_id, correlation_id, created_at,
				hash, prev_hash, event_payload_version
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, ?)
		`, "evt_replay_corrupt_nonhuman_approval_create", 1, entityTypeGateTemplate, gateTemplateEntityID("replay-corrupt-nonhuman-approval", 1), 1, eventTypeGateTemplateCreate, string(createPayloadBytes), "llm:openai:gpt-5", "cmd-replay-corrupt-nonhuman-approval-create-1", gateTemplateCorrelationID("replay-corrupt-nonhuman-approval", 1), nowUTC(), "hash_replay_corrupt_nonhuman_approval_create", 1); err != nil {
			t.Fatalf("insert template create event: %v", err)
		}

		approvalPayloadBytes, err := json.Marshal(gateTemplateApprovedPayload{
			TemplateID:     "replay-corrupt-nonhuman-approval",
			Version:        1,
			DefinitionHash: definitionHash,
			ApprovedAt:     "2026-03-08T12:03:00Z",
			ApprovedBy:     "llm:openai:gpt-5",
		})
		if err != nil {
			t.Fatalf("marshal approval payload: %v", err)
		}
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO events(
				event_id, event_order, entity_type, entity_id, entity_seq, event_type,
				payload_json, actor, command_id, causation_id, correlation_id, created_at,
				hash, prev_hash, event_payload_version
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
		`, "evt_replay_corrupt_nonhuman_approval", 2, entityTypeGateTemplate, gateTemplateEntityID("replay-corrupt-nonhuman-approval", 1), 2, eventTypeGateTemplateApprove, string(approvalPayloadBytes), "llm:openai:gpt-5", "cmd-replay-corrupt-nonhuman-approval-1", gateTemplateCorrelationID("replay-corrupt-nonhuman-approval", 1), nowUTC(), "hash_replay_corrupt_nonhuman_approval", "hash_replay_corrupt_nonhuman_approval_create", 1); err != nil {
			t.Fatalf("insert corrupt approval event: %v", err)
		}

		if _, err := s.ReplayProjections(ctx); err == nil || !strings.Contains(err.Error(), "approved_by must be human-governed") {
			t.Fatalf("expected replay corruption error for non-human approval, got %v", err)
		}
	})

	t.Run("gate set instantiation hash mismatch", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		s := newTestStore(t)

		if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
			IssueID:   "mem-f1a2b3c",
			Type:      "task",
			Title:     "Replay corrupt gate set create",
			Actor:     "agent-1",
			CommandID: "cmd-replay-corrupt-gate-set-issue-1",
		}); err != nil {
			t.Fatalf("create issue: %v", err)
		}

		var nextOrder int
		if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(event_order), 0) + 1 FROM events`).Scan(&nextOrder); err != nil {
			t.Fatalf("read next event order: %v", err)
		}

		payload := `{"gate_set_id":"gset_corrupt_replay","issue_id":"mem-f1a2b3c","cycle_no":1,"template_refs":["replay-corrupt-template@1"],"frozen_definition":{"templates":["replay-corrupt-template@1"],"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]},"gate_set_hash":"deadbeef","created_at":"2026-03-08T12:02:00Z","created_by":"agent-1","items":[{"gate_id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO events(
				event_id, event_order, entity_type, entity_id, entity_seq, event_type,
				payload_json, actor, command_id, causation_id, correlation_id, created_at,
				hash, prev_hash, event_payload_version
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, ?)
		`, "evt_replay_corrupt_gate_set_create", nextOrder, entityTypeGateSet, "gset_corrupt_replay", 1, eventTypeGateSetCreate, payload, "agent-1", "cmd-replay-corrupt-gate-set-create-1", gateCycleCorrelationID("mem-f1a2b3c", 1), nowUTC(), "hash_replay_corrupt_gate_set_create", 1); err != nil {
			t.Fatalf("insert corrupt gate set create event: %v", err)
		}

		if _, err := s.ReplayProjections(ctx); err == nil || !strings.Contains(err.Error(), "gate_set_hash does not match frozen definition") {
			t.Fatalf("expected replay corruption error for gate set hash mismatch, got %v", err)
		}
	})

	t.Run("lock payload requires locked timestamp", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		s := newTestStore(t)

		if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
			IssueID:   "mem-a1b2c3d",
			Type:      "task",
			Title:     "Replay corrupt empty lock timestamp",
			Actor:     "agent-1",
			CommandID: "cmd-replay-corrupt-empty-lock-issue-1",
		}); err != nil {
			t.Fatalf("create issue: %v", err)
		}

		var nextOrder int
		if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(event_order), 0) + 1 FROM events`).Scan(&nextOrder); err != nil {
			t.Fatalf("read next event order: %v", err)
		}

		payload := `{"gate_set_id":"gset_corrupt_empty_lock","issue_id":"mem-a1b2c3d","cycle_no":1,"locked_at":" "}`
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO events(
				event_id, event_order, entity_type, entity_id, entity_seq, event_type,
				payload_json, actor, command_id, causation_id, correlation_id, created_at,
				hash, prev_hash, event_payload_version
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, ?)
		`, "evt_replay_corrupt_empty_lock", nextOrder, entityTypeGateSet, "gset_corrupt_empty_lock", 1, eventTypeGateSetLock, payload, "agent-1", "cmd-replay-corrupt-empty-lock-1", gateCycleCorrelationID("mem-a1b2c3d", 1), nowUTC(), "hash_replay_corrupt_empty_lock", 1); err != nil {
			t.Fatalf("insert corrupt empty lock event: %v", err)
		}

		if _, err := s.ReplayProjections(ctx); err == nil || !strings.Contains(err.Error(), "locked_at is required") {
			t.Fatalf("expected replay corruption error for missing lock timestamp, got %v", err)
		}
	})

	t.Run("lock without gate set", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		s := newTestStore(t)

		if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
			IssueID:   "mem-e1f2a3b",
			Type:      "task",
			Title:     "Replay corrupt lock",
			Actor:     "agent-1",
			CommandID: "cmd-replay-corrupt-lock-issue-1",
		}); err != nil {
			t.Fatalf("create issue: %v", err)
		}

		var nextOrder int
		if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(event_order), 0) + 1 FROM events`).Scan(&nextOrder); err != nil {
			t.Fatalf("read next event order: %v", err)
		}

		payload := `{"gate_set_id":"gset_missing_replay","issue_id":"mem-e1f2a3b","cycle_no":1,"locked_at":"2026-03-08T12:04:00Z"}`
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO events(
				event_id, event_order, entity_type, entity_id, entity_seq, event_type,
				payload_json, actor, command_id, causation_id, correlation_id, created_at,
				hash, prev_hash, event_payload_version
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, ?)
		`, "evt_replay_corrupt_lock", nextOrder, entityTypeGateSet, "gset_missing_replay", 1, eventTypeGateSetLock, payload, "agent-1", "cmd-replay-corrupt-lock-1", gateCycleCorrelationID("mem-e1f2a3b", 1), nowUTC(), "hash_replay_corrupt_lock", 1); err != nil {
			t.Fatalf("insert corrupt lock event: %v", err)
		}

		if _, err := s.ReplayProjections(ctx); err == nil || !strings.Contains(err.Error(), `gate set "gset_missing_replay" not found`) {
			t.Fatalf("expected replay corruption error for missing gate set, got %v", err)
		}
	})
}
