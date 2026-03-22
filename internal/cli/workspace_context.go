package cli

import (
	"context"
	"fmt"
	"os"
	"sort"
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
	Health     string `json:"health,omitempty"`
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
	return assessWorkspaceContext(workspaceContextFromWorktree(worktrees[0])), nil
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
		byIssue[issueID] = assessWorkspaceContext(workspaceContextFromWorktree(worktree))
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
		return assessWorkspaceContext(workspace), nil
	}
	return activeWorkspaceForIssue(ctx, s, packetIssueIDForCLI(packet))
}

func assessWorkspaceContext(workspace *workspaceContext) *workspaceContext {
	if workspace == nil {
		return nil
	}
	assessed := *workspace
	path := strings.TrimSpace(assessed.Path)
	if path == "" {
		return &assessed
	}
	if _, err := os.Stat(path); err == nil {
		assessed.Health = "available"
	} else if os.IsNotExist(err) {
		assessed.Health = "missing"
	}
	return &assessed
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
	parts := make([]string, 0, 3)
	if strings.TrimSpace(workspace.Path) != "" {
		parts = append(parts, workspace.Path)
	}
	if strings.TrimSpace(workspace.Branch) != "" {
		parts = append(parts, "branch="+workspace.Branch)
	}
	if strings.TrimSpace(workspace.Health) != "" {
		parts = append(parts, "health="+workspace.Health)
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
	if strings.TrimSpace(workspace.Health) != "" {
		line += " health=" + workspace.Health
	}
	return strings.TrimSpace(line)
}

func workspaceReason(workspace *workspaceContext) string {
	if workspace == nil {
		return ""
	}
	switch strings.TrimSpace(workspace.Health) {
	case "available":
		return "attached workspace is available on this machine"
	case "missing":
		return "attached workspace path is missing on this machine"
	default:
		if strings.TrimSpace(workspace.WorktreeID) != "" {
			return "attached workspace metadata is present"
		}
		return ""
	}
}

func workspaceScoreDelta(workspace *workspaceContext) int {
	if workspace == nil {
		return 0
	}
	switch strings.TrimSpace(workspace.Health) {
	case "available":
		return 10
	case "missing":
		return -10
	default:
		return 0
	}
}

func annotateIssueNextResultWithWorkspace(next store.IssueNextResult, workspaceByIssue map[string]*workspaceContext) store.IssueNextResult {
	next.Candidates = annotateIssueNextCandidatesWithWorkspace(next.Candidates, workspaceByIssue)
	if len(next.Candidates) > 0 {
		next.Candidate = next.Candidates[0]
	}
	return next
}

func annotateIssueNextCandidatesWithWorkspace(candidates []store.IssueNextCandidate, workspaceByIssue map[string]*workspaceContext) []store.IssueNextCandidate {
	if len(candidates) == 0 {
		return nil
	}

	annotated := append([]store.IssueNextCandidate(nil), candidates...)
	for i := range annotated {
		workspace := workspaceByIssue[annotated[i].Issue.ID]
		if reason := workspaceReason(workspace); reason != "" {
			annotated[i].Reasons = append(annotated[i].Reasons, reason)
			annotated[i].Score += workspaceScoreDelta(workspace)
		}
	}

	sort.SliceStable(annotated, func(i, j int) bool {
		if annotated[i].Score != annotated[j].Score {
			return annotated[i].Score > annotated[j].Score
		}
		return i < j
	})
	return annotated
}
