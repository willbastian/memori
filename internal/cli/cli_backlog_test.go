package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/willbastian/memori/internal/store"
)

func TestRenderBacklogTreeShowsParentChildRelationships(t *testing.T) {
	t.Parallel()

	issues := []store.Issue{
		{ID: "epic_1", Type: "Epic", Status: "Todo", Title: "Top epic"},
		{ID: "story_1", Type: "Story", Status: "Todo", Title: "Story one", ParentID: "epic_1"},
		{ID: "task_1", Type: "Task", Status: "InProgress", Title: "Task one", ParentID: "story_1"},
		{ID: "orphan_1", Type: "Task", Status: "Todo", Title: "Orphan task", ParentID: "missing_parent"},
	}

	var out bytes.Buffer
	renderBacklogTree(&out, issues, false)
	got := out.String()

	mustContain(t, got, "- epic_1 [Epic/Todo] Top epic")
	mustContain(t, got, "`- story_1 [Story/Todo] Story one")
	mustContain(t, got, "`- task_1 [Task/InProgress] Task one")
	mustContain(t, got, "- orphan_1 [Task/Todo] Orphan task (parent: missing_parent)")
}

func mustContain(t *testing.T, got, expected string) {
	t.Helper()
	if !strings.Contains(got, expected) {
		t.Fatalf("expected output to contain %q, got:\n%s", expected, got)
	}
}
