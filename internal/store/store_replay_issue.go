package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

func applyIssueCreatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload issueCreatedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode issue.created payload for event %s: %w", event.EventID, err)
	}
	referencesJSON, err := json.Marshal(normalizeReferences(payload.References))
	if err != nil {
		return fmt.Errorf("encode issue.created references payload for event %s: %w", event.EventID, err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO work_items(
			id, type, title, parent_id, status,
			description, acceptance_criteria, references_json,
			labels_json, current_cycle_no, active_gate_set_id,
			created_at, updated_at, last_event_id
		)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, '[]', 1, NULL, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			type=excluded.type,
			title=excluded.title,
			parent_id=excluded.parent_id,
			status=excluded.status,
			description=excluded.description,
			acceptance_criteria=excluded.acceptance_criteria,
			references_json=excluded.references_json,
			updated_at=excluded.updated_at,
			last_event_id=excluded.last_event_id
	`,
		payload.IssueID,
		payload.Type,
		payload.Title,
		nullIfEmpty(payload.ParentID),
		payload.Status,
		strings.TrimSpace(payload.Description),
		strings.TrimSpace(payload.AcceptanceCriteria),
		string(referencesJSON),
		payload.CreatedAt,
		event.CreatedAt,
		event.EventID,
	)
	if err != nil {
		return fmt.Errorf("upsert work_item from event %s: %w", event.EventID, err)
	}

	return nil
}

func applyIssueUpdatedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload issueUpdatedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode issue.updated payload for event %s: %w", event.EventID, err)
	}

	setClauses := make([]string, 0, 8)
	args := make([]any, 0, 10)
	reopenTransition := false

	if payload.TitleTo != nil {
		titleTo := strings.TrimSpace(*payload.TitleTo)
		if titleTo == "" {
			return fmt.Errorf("decode issue.updated payload for event %s: --title is required", event.EventID)
		}
		setClauses = append(setClauses, "title = ?")
		args = append(args, titleTo)
	}
	if payload.StatusTo != nil {
		issueStatus, err := normalizeIssueStatus(*payload.StatusTo)
		if err != nil {
			return fmt.Errorf("decode issue.updated payload for event %s: %w", event.EventID, err)
		}
		setClauses = append(setClauses, "status = ?")
		args = append(args, issueStatus)
		if payload.StatusFrom != nil {
			statusFrom, err := normalizeIssueStatus(*payload.StatusFrom)
			if err != nil {
				return fmt.Errorf("decode issue.updated payload for event %s: %w", event.EventID, err)
			}
			reopenTransition = statusFrom == "Done" && issueStatus != "Done"
		}
	}
	if payload.PriorityTo != nil {
		setClauses = append(setClauses, "priority = ?")
		args = append(args, nullIfEmpty(strings.TrimSpace(*payload.PriorityTo)))
	}
	if payload.LabelsTo != nil {
		labelsJSON, err := json.Marshal(normalizeLabels(*payload.LabelsTo))
		if err != nil {
			return fmt.Errorf("encode issue.updated labels payload for event %s: %w", event.EventID, err)
		}
		setClauses = append(setClauses, "labels_json = ?")
		args = append(args, string(labelsJSON))
	}
	if payload.DescriptionTo != nil {
		setClauses = append(setClauses, "description = ?")
		args = append(args, strings.TrimSpace(*payload.DescriptionTo))
	}
	if payload.AcceptanceCriteriaTo != nil {
		setClauses = append(setClauses, "acceptance_criteria = ?")
		args = append(args, strings.TrimSpace(*payload.AcceptanceCriteriaTo))
	}
	if payload.ReferencesTo != nil {
		referencesJSON, err := json.Marshal(normalizeReferences(*payload.ReferencesTo))
		if err != nil {
			return fmt.Errorf("encode issue.updated references payload for event %s: %w", event.EventID, err)
		}
		setClauses = append(setClauses, "references_json = ?")
		args = append(args, string(referencesJSON))
	}
	if reopenTransition {
		setClauses = append(setClauses, "current_cycle_no = current_cycle_no + 1", "active_gate_set_id = NULL")
	}
	if len(setClauses) == 0 {
		return fmt.Errorf("decode issue.updated payload for event %s: no mutable fields provided", event.EventID)
	}
	setClauses = append(setClauses, "updated_at = ?", "last_event_id = ?")
	args = append(args, event.CreatedAt, event.EventID, payload.IssueID)

	query := `
		UPDATE work_items
		SET ` + strings.Join(setClauses, ", ") + `
		WHERE id = ?
	`
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update work_item from event %s: %w", event.EventID, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check updated rows for event %s: %w", event.EventID, err)
	}
	if rows == 0 {
		return fmt.Errorf("update work_item from event %s: issue %q not found", event.EventID, payload.IssueID)
	}
	if reopenTransition {
		if _, err := syncOpenLoopsForCurrentCycleTx(ctx, tx, payload.IssueID, event.EventID); err != nil {
			return fmt.Errorf("sync reopened cycle open loops from event %s: %w", event.EventID, err)
		}
	}

	return nil
}

func applyIssueLinkedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload issueLinkedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode issue.linked payload for event %s: %w", event.EventID, err)
	}

	linkedParentID, err := normalizeIssueKey(payload.ParentIDTo)
	if err != nil {
		return fmt.Errorf("decode issue.linked payload for event %s: %w", event.EventID, err)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE work_items
		SET parent_id = ?, updated_at = ?, last_event_id = ?
		WHERE id = ?
	`, linkedParentID, event.CreatedAt, event.EventID, payload.IssueID)
	if err != nil {
		return fmt.Errorf("update work_item from event %s: %w", event.EventID, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check updated rows for event %s: %w", event.EventID, err)
	}
	if rows == 0 {
		return fmt.Errorf("update work_item from event %s: issue %q not found", event.EventID, payload.IssueID)
	}

	return nil
}
