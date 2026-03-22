package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

func applyWorktreeRegisteredProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload worktreeRegisteredPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode worktree.registered payload for event %s: %w", event.EventID, err)
	}

	_, err := tx.ExecContext(ctx, `
		INSERT INTO worktrees(
			worktree_id, path, repo_root, branch, head_oid, issue_id,
			status, created_at, updated_at, last_event_id
		) VALUES(?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
		ON CONFLICT(worktree_id) DO UPDATE SET
			path=excluded.path,
			repo_root=excluded.repo_root,
			branch=excluded.branch,
			head_oid=excluded.head_oid,
			status=excluded.status,
			updated_at=excluded.updated_at,
			last_event_id=excluded.last_event_id
	`, payload.WorktreeID, payload.Path, payload.RepoRoot, nullIfEmpty(strings.TrimSpace(payload.Branch)), nullIfEmpty(strings.TrimSpace(payload.HeadOID)), payload.Status, payload.RegisteredAt, payload.RegisteredAt, event.EventID)
	if err != nil {
		return fmt.Errorf("upsert worktree from event %s: %w", event.EventID, err)
	}
	return nil
}

func applyWorktreeAttachedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload worktreeAttachedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode worktree.attached payload for event %s: %w", event.EventID, err)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE worktrees
		SET issue_id = ?, updated_at = ?, last_event_id = ?
		WHERE worktree_id = ?
	`, payload.IssueIDTo, payload.AttachedAt, event.EventID, payload.WorktreeID)
	if err != nil {
		return fmt.Errorf("attach worktree from event %s: %w", event.EventID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check attach rows for event %s: %w", event.EventID, err)
	}
	if rows == 0 {
		return fmt.Errorf("attach worktree from event %s: worktree %q not found", event.EventID, payload.WorktreeID)
	}
	return nil
}

func applyWorktreeDetachedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload worktreeDetachedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode worktree.detached payload for event %s: %w", event.EventID, err)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE worktrees
		SET issue_id = NULL, updated_at = ?, last_event_id = ?
		WHERE worktree_id = ?
	`, payload.DetachedAt, event.EventID, payload.WorktreeID)
	if err != nil {
		return fmt.Errorf("detach worktree from event %s: %w", event.EventID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check detach rows for event %s: %w", event.EventID, err)
	}
	if rows == 0 {
		return fmt.Errorf("detach worktree from event %s: worktree %q not found", event.EventID, payload.WorktreeID)
	}
	return nil
}

func applyWorktreeArchivedProjectionTx(ctx context.Context, tx *sql.Tx, event Event) error {
	var payload worktreeArchivedPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode worktree.archived payload for event %s: %w", event.EventID, err)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE worktrees
		SET issue_id = NULL, status = ?, updated_at = ?, last_event_id = ?
		WHERE worktree_id = ?
	`, worktreeStatusArchived, payload.ArchivedAt, event.EventID, payload.WorktreeID)
	if err != nil {
		return fmt.Errorf("archive worktree from event %s: %w", event.EventID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check archive rows for event %s: %w", event.EventID, err)
	}
	if rows == 0 {
		return fmt.Errorf("archive worktree from event %s: worktree %q not found", event.EventID, payload.WorktreeID)
	}
	return nil
}
