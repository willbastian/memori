package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func applyGateEvaluatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	payload, err := decodeGateEvaluatedPayload(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("decode gate.evaluated payload for event %s: %w", event.EventID, err)
	}
	if payload.EvaluatedAt == "" {
		payload.EvaluatedAt = event.CreatedAt
	}

	var gateCount int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM gate_set_items
		WHERE gate_set_id = ? AND gate_id = ?
	`, payload.GateSetID, payload.GateID).Scan(&gateCount); err != nil {
		return fmt.Errorf("validate gate.evaluated payload for event %s: %w", event.EventID, err)
	}
	if gateCount == 0 {
		return fmt.Errorf(
			"validate gate.evaluated payload for event %s: gate %q not found in gate_set %q",
			event.EventID,
			payload.GateID,
			payload.GateSetID,
		)
	}

	evidenceJSON, err := json.Marshal(normalizeReferences(payload.EvidenceRefs))
	if err != nil {
		return fmt.Errorf("encode gate.evaluated evidence refs for event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO gate_status_projection(
			issue_id, gate_set_id, gate_id, result,
			evidence_refs_json, evaluated_at, updated_at, last_event_id
		)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(issue_id, gate_set_id, gate_id) DO UPDATE SET
			result=excluded.result,
			evidence_refs_json=excluded.evidence_refs_json,
			evaluated_at=excluded.evaluated_at,
			updated_at=excluded.updated_at,
			last_event_id=excluded.last_event_id
	`,
		payload.IssueID,
		payload.GateSetID,
		payload.GateID,
		payload.Result,
		string(evidenceJSON),
		payload.EvaluatedAt,
		event.CreatedAt,
		event.EventID,
	)
	if err != nil {
		return fmt.Errorf("upsert gate status projection from event %s: %w", event.EventID, err)
	}
	if _, err := syncOpenLoopsForCurrentCycleTx(ctx, tx, payload.IssueID, event.EventID); err != nil {
		return fmt.Errorf("sync open loops from gate event %s: %w", event.EventID, err)
	}
	return nil
}

func applyGateTemplateCreatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	payload, err := decodeGateTemplateCreatedPayload(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("decode gate_template.created payload for event %s: %w", event.EventID, err)
	}
	appliesToJSON, err := json.Marshal(payload.AppliesTo)
	if err != nil {
		return fmt.Errorf("encode gate_template.created applies_to for event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO gate_templates(
			template_id, version, applies_to_json, definition_json,
			definition_hash, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?)
	`, payload.TemplateID, payload.Version, string(appliesToJSON), payload.DefinitionJSON, payload.DefinitionHash, payload.CreatedAt, payload.CreatedBy)
	if err != nil {
		existing, found, lookupErr := gateTemplateByIDVersionTx(ctx, tx, payload.TemplateID, payload.Version)
		if lookupErr != nil {
			return lookupErr
		}
		if !found ||
			existing.DefinitionHash != payload.DefinitionHash ||
			existing.DefinitionJSON != payload.DefinitionJSON ||
			!equalStringSlices(existing.AppliesTo, payload.AppliesTo) {
			return fmt.Errorf("insert gate template from event %s: %w", event.EventID, err)
		}
	}
	if gateDefinitionContainsExecutableCommand(payload.DefinitionJSON) && actorIsHumanGoverned(payload.CreatedBy) {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO gate_template_approvals(template_id, version, approved_at, approved_by)
			VALUES(?, ?, ?, ?)
			ON CONFLICT(template_id, version) DO NOTHING
		`, payload.TemplateID, payload.Version, payload.CreatedAt, payload.CreatedBy); err != nil {
			return fmt.Errorf("auto-approve gate template from event %s: %w", event.EventID, err)
		}
	}
	return nil
}

func applyGateTemplateApprovedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	payload, err := decodeGateTemplateApprovedPayload(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("decode gate_template.approved payload for event %s: %w", event.EventID, err)
	}

	var currentDefinitionHash string
	err = tx.QueryRowContext(ctx, `
		SELECT definition_hash
		FROM gate_templates
		WHERE template_id = ? AND version = ?
	`, payload.TemplateID, payload.Version).Scan(&currentDefinitionHash)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("approve gate template from event %s: template %s@%d not found", event.EventID, payload.TemplateID, payload.Version)
	}
	if err != nil {
		return fmt.Errorf("lookup gate template for approval from event %s: %w", event.EventID, err)
	}
	if currentDefinitionHash != payload.DefinitionHash {
		return fmt.Errorf("approve gate template from event %s: definition hash mismatch for %s@%d", event.EventID, payload.TemplateID, payload.Version)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO gate_template_approvals(template_id, version, approved_at, approved_by)
		VALUES(?, ?, ?, ?)
		ON CONFLICT(template_id, version) DO NOTHING
	`, payload.TemplateID, payload.Version, payload.ApprovedAt, payload.ApprovedBy); err != nil {
		return fmt.Errorf("approve gate template from event %s: %w", event.EventID, err)
	}
	return nil
}

func applyGateSetInstantiatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	payload, err := decodeGateSetInstantiatedPayload(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("decode gate_set.instantiated payload for event %s: %w", event.EventID, err)
	}
	frozenJSON, frozenObj, err := buildFrozenGateDefinition(payload.TemplateRefs, payload.Items)
	if err != nil {
		return fmt.Errorf("decode gate_set.instantiated payload for event %s: %w", event.EventID, err)
	}
	if len(payload.FrozenDefinition) > 0 {
		payload.FrozenDefinition = frozenObj
	}
	templateRefsJSON, err := json.Marshal(payload.TemplateRefs)
	if err != nil {
		return fmt.Errorf("encode gate_set.instantiated template refs for event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, NULL, ?, ?)
	`, payload.GateSetID, payload.IssueID, payload.CycleNo, string(templateRefsJSON), frozenJSON, payload.GateSetHash, payload.CreatedAt, payload.CreatedBy)
	if err != nil {
		existing, found, lookupErr := gateSetByIDTx(ctx, tx, payload.GateSetID)
		if lookupErr != nil {
			return lookupErr
		}
		if !found ||
			existing.IssueID != payload.IssueID ||
			existing.CycleNo != payload.CycleNo ||
			existing.GateSetHash != payload.GateSetHash ||
			!equalStringSlices(existing.TemplateRefs, payload.TemplateRefs) {
			return fmt.Errorf("insert gate set from event %s: %w", event.EventID, err)
		}
	}

	for _, item := range payload.Items {
		criteriaJSON, err := json.Marshal(item.Criteria)
		if err != nil {
			return fmt.Errorf("encode gate_set.instantiated criteria for event %s gate %s: %w", event.EventID, item.GateID, err)
		}
		requiredInt := 0
		if item.Required {
			requiredInt = 1
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
			VALUES(?, ?, ?, ?, ?)
			ON CONFLICT(gate_set_id, gate_id) DO NOTHING
		`, payload.GateSetID, item.GateID, item.Kind, requiredInt, string(criteriaJSON)); err != nil {
			return fmt.Errorf("insert gate set item from event %s gate %s: %w", event.EventID, item.GateID, err)
		}
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE work_items
		SET active_gate_set_id = ?, updated_at = ?
		WHERE id = ?
	`, payload.GateSetID, payload.CreatedAt, payload.IssueID); err != nil {
		return fmt.Errorf("set active gate set from event %s: %w", event.EventID, err)
	}
	return nil
}

func applyGateSetLockedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	payload, err := decodeGateSetLockedPayload(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("decode gate_set.locked payload for event %s: %w", event.EventID, err)
	}
	current, found, err := gateSetByIDTx(ctx, tx, payload.GateSetID)
	if err != nil {
		return fmt.Errorf("lookup gate set from event %s: %w", event.EventID, err)
	}
	if !found {
		return fmt.Errorf("lookup gate set from event %s: gate set %q not found", event.EventID, payload.GateSetID)
	}
	if strings.TrimSpace(current.LockedAt) == "" {
		if _, err := tx.ExecContext(ctx, `
			UPDATE gate_sets
			SET locked_at = ?
			WHERE gate_set_id = ?
		`, payload.LockedAt, payload.GateSetID); err != nil {
			return fmt.Errorf("lock gate set from event %s: %w", event.EventID, err)
		}
	} else if current.LockedAt != payload.LockedAt {
		return fmt.Errorf("lock gate set from event %s: gate set %q already locked at %s", event.EventID, payload.GateSetID, current.LockedAt)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE work_items
		SET active_gate_set_id = ?, updated_at = ?
		WHERE id = ?
	`, payload.GateSetID, payload.LockedAt, payload.IssueID); err != nil {
		return fmt.Errorf("set active gate set from lock event %s: %w", event.EventID, err)
	}
	return nil
}
