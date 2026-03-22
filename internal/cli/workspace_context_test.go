package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/willbastian/memori/internal/store"
)

func TestAssessWorkspaceContextDetectsAvailableAndMissingPaths(t *testing.T) {
	t.Parallel()

	availablePath := filepath.Join(t.TempDir(), "available")
	if err := os.MkdirAll(availablePath, 0o755); err != nil {
		t.Fatalf("mkdir available workspace: %v", err)
	}

	available := assessWorkspaceContext(&workspaceContext{WorktreeID: "wt-available", Path: availablePath})
	if available.Health != "available" {
		t.Fatalf("expected available health, got %+v", available)
	}

	missing := assessWorkspaceContext(&workspaceContext{WorktreeID: "wt-missing", Path: filepath.Join(t.TempDir(), "missing")})
	if missing.Health != "missing" {
		t.Fatalf("expected missing health, got %+v", missing)
	}
}

func TestAnnotateIssueNextCandidatesWithWorkspaceAdjustsReasonsAndOrdering(t *testing.T) {
	t.Parallel()

	availablePath := filepath.Join(t.TempDir(), "rank-available")
	if err := os.MkdirAll(availablePath, 0o755); err != nil {
		t.Fatalf("mkdir ranked workspace: %v", err)
	}

	candidates := []store.IssueNextCandidate{
		{Issue: store.Issue{ID: "mem-a111111"}, Score: 100},
		{Issue: store.Issue{ID: "mem-b222222"}, Score: 100},
	}
	workspaceByIssue := map[string]*workspaceContext{
		"mem-a111111": assessWorkspaceContext(&workspaceContext{WorktreeID: "wt-a", Path: filepath.Join(t.TempDir(), "rank-missing")}),
		"mem-b222222": assessWorkspaceContext(&workspaceContext{WorktreeID: "wt-b", Path: availablePath}),
	}

	annotated := annotateIssueNextCandidatesWithWorkspace(candidates, workspaceByIssue)
	if annotated[0].Issue.ID != "mem-b222222" {
		t.Fatalf("expected available workspace candidate first, got %+v", annotated)
	}
	if annotated[0].Score <= annotated[1].Score {
		t.Fatalf("expected workspace scoring delta to affect ordering, got %+v", annotated)
	}
	if got := annotated[0].Reasons[len(annotated[0].Reasons)-1]; got != "attached workspace is available on this machine" {
		t.Fatalf("unexpected available workspace reason %q", got)
	}
	if got := annotated[1].Reasons[len(annotated[1].Reasons)-1]; got != "attached workspace path is missing on this machine" {
		t.Fatalf("unexpected missing workspace reason %q", got)
	}
}
