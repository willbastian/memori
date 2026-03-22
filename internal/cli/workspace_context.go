package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/willbastian/memori/internal/store"
)

type workspaceContext struct {
	WorktreeID string `json:"worktree_id"`
	Path       string `json:"path"`
	RepoRoot   string `json:"repo_root,omitempty"`
	Branch     string `json:"branch,omitempty"`
	HeadOID    string `json:"head_oid,omitempty"`
	IssueID    string `json:"issue_id,omitempty"`
	Status     string `json:"status,omitempty"`
}

func activeWorkspaceForIssue(ctx context.Context, s *store.Store, issueID string) (*workspaceContext, error) {
	issueID = strings.TrimSpace(issueID)
	if issueID == "" {
		return nil, nil
	}

	worktrees, err := s.ListWorktrees(ctx, store.ListWorktreesParams{
		IssueID: issueID,
		Status:  "active",
	})
	if err != nil {
		return nil, err
	}
	if len(worktrees) == 0 {
		return nil, nil
	}
	return workspaceContextFromWorktree(worktrees[0]), nil
}

func activeWorkspaceByIssue(ctx context.Context, s *store.Store) (map[string]*workspaceContext, error) {
	worktrees, err := s.ListWorktrees(ctx, store.ListWorktreesParams{Status: "active"})
	if err != nil {
		return nil, err
	}

	byIssue := make(map[string]*workspaceContext, len(worktrees))
	for _, worktree := range worktrees {
		issueID := strings.TrimSpace(worktree.IssueID)
		if issueID == "" {
			continue
		}
		if _, exists := byIssue[issueID]; exists {
			continue
		}
		byIssue[issueID] = workspaceContextFromWorktree(worktree)
	}
	return byIssue, nil
}

func workspaceContextFromWorktree(worktree store.Worktree) *workspaceContext {
	if strings.TrimSpace(worktree.WorktreeID) == "" {
		return nil
	}
	return &workspaceContext{
		WorktreeID: worktree.WorktreeID,
		Path:       worktree.Path,
		RepoRoot:   worktree.RepoRoot,
		Branch:     worktree.Branch,
		HeadOID:    worktree.HeadOID,
		IssueID:    worktree.IssueID,
		Status:     worktree.Status,
	}
}

func workspaceContextFromPacket(packet store.RehydratePacket) *workspaceContext {
	if packet.Packet == nil {
		return nil
	}
	raw, ok := packet.Packet["workspace"].(map[string]any)
	if !ok || len(raw) == 0 {
		return nil
	}

	workspace := &workspaceContext{
		WorktreeID: workspaceValue(raw["worktree_id"]),
		Path:       workspaceValue(raw["path"]),
		RepoRoot:   workspaceValue(raw["repo_root"]),
		Branch:     workspaceValue(raw["branch"]),
		HeadOID:    workspaceValue(raw["head_oid"]),
		IssueID:    workspaceValue(raw["issue_id"]),
		Status:     workspaceValue(raw["status"]),
	}
	if workspace.IssueID == "" {
		workspace.IssueID = packetIssueIDForCLI(packet)
	}
	if workspace.WorktreeID == "" && workspace.Path == "" {
		return nil
	}
	return workspace
}

func resolveWorkspaceForPacket(ctx context.Context, s *store.Store, packet store.RehydratePacket) (*workspaceContext, error) {
	if workspace := workspaceContextFromPacket(packet); workspace != nil {
		return workspace, nil
	}
	return activeWorkspaceForIssue(ctx, s, packetIssueIDForCLI(packet))
}

func workspaceValue(raw any) string {
	text := strings.TrimSpace(fmt.Sprint(raw))
	switch text {
	case "", "<nil>":
		return ""
	default:
		return text
	}
}

func formatWorkspaceSummary(workspace *workspaceContext) string {
	if workspace == nil {
		return ""
	}
	parts := make([]string, 0, 2)
	if strings.TrimSpace(workspace.Path) != "" {
		parts = append(parts, workspace.Path)
	}
	if strings.TrimSpace(workspace.Branch) != "" {
		parts = append(parts, "branch="+workspace.Branch)
	}
	return strings.Join(parts, " | ")
}

func formatIssueWorkspaceLine(issueID string, workspace *workspaceContext) string {
	if workspace == nil {
		return ""
	}
	line := strings.TrimSpace(issueID)
	if line != "" {
		line += " -> "
	}
	line += workspace.Path
	if strings.TrimSpace(workspace.Branch) != "" {
		line += " branch=" + workspace.Branch
	}
	return strings.TrimSpace(line)
}
