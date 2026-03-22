package store

import (
	"context"
	"fmt"
	"strings"
)

func (s *Store) GetWorktree(ctx context.Context, worktreeID string) (Worktree, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Worktree{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	worktree, err := worktreeByIDTx(ctx, tx, strings.TrimSpace(worktreeID))
	if err != nil {
		return Worktree{}, err
	}
	if err := tx.Commit(); err != nil {
		return Worktree{}, fmt.Errorf("commit tx: %w", err)
	}
	return worktree, nil
}

func (s *Store) ListWorktrees(ctx context.Context, p ListWorktreesParams) ([]Worktree, error) {
	clauses := make([]string, 0, 2)
	args := make([]any, 0, 2)

	if strings.TrimSpace(p.IssueID) != "" {
		issueID, err := normalizeIssueKey(p.IssueID)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, "issue_id = ?")
		args = append(args, issueID)
	}
	if strings.TrimSpace(p.Status) != "" {
		status, err := normalizeWorktreeStatus(p.Status)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, "status = ?")
		args = append(args, status)
	}

	query := `
		SELECT
			worktree_id, path, repo_root, COALESCE(branch, ''), COALESCE(head_oid, ''),
			COALESCE(issue_id, ''), status, created_at, updated_at, last_event_id
		FROM worktrees
	`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY updated_at DESC, worktree_id ASC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query worktrees: %w", err)
	}
	defer rows.Close()

	worktrees := make([]Worktree, 0)
	for rows.Next() {
		worktree, err := scanWorktree(rows)
		if err != nil {
			return nil, fmt.Errorf("scan worktree row: %w", err)
		}
		worktrees = append(worktrees, worktree)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate worktree rows: %w", err)
	}
	return worktrees, nil
}
