package cli

import (
	"bufio"
	"strings"
	"testing"

	"github.com/willbastian/memori/internal/store"
)

func TestBoardTUIReduceNavigationAndDetailToggle(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		LikelyNext: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Next one")},
			{Issue: boardTestIssue("mem-b222222", "Task", "Todo", "Next two")},
		},
		Active: []boardIssueRow{
			{Issue: boardTestIssue("mem-c333333", "Task", "InProgress", "Active one")},
		},
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-d444444", "Task", "Todo", "Ready one")},
		},
	}, 120, 30)

	if model.lane != boardLaneNext || model.index != 0 || !model.detailOpen {
		t.Fatalf("unexpected initial model: %+v", model)
	}

	model = boardReduce(model, boardActionDown)
	if model.index != 1 || model.selectedIssue != "mem-b222222" {
		t.Fatalf("expected selection to move down, got %+v", model)
	}

	model = boardReduce(model, boardActionPrevLane)
	if model.lane != boardLaneReady {
		t.Fatalf("expected h to switch to previous populated lane, got %+v", model)
	}

	model = boardApplySnapshot(model, model.snapshot, 80, 24)
	model = boardReduce(model, boardActionPrevLane)
	if model.lane != boardLaneActive {
		t.Fatalf("expected to move to active lane, got %+v", model)
	}

	model.detailOpen = false
	model = boardReduce(model, boardActionToggleDetail)
	if !model.detailOpen {
		t.Fatalf("expected enter to toggle detail open")
	}
}

func TestBoardTUIApplySnapshotPreservesSelectionByIssueID(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
			{Issue: boardTestIssue("mem-b222222", "Task", "Todo", "Ready two")},
		},
	}, 80, 24)
	model.lane = boardLaneReady
	model.index = 1
	model.selectedIssue = "mem-b222222"
	model = boardNormalizeModel(model)

	updated := boardApplySnapshot(model, boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-b222222", "Task", "Todo", "Ready two moved")},
			{Issue: boardTestIssue("mem-c333333", "Task", "Todo", "Ready three")},
		},
	}, 80, 24)

	if updated.selectedIssue != "mem-b222222" || updated.index != 0 {
		t.Fatalf("expected selection to follow issue id, got %+v", updated)
	}
}

func TestBoardTUIApplySnapshotKeepsDetailOpenOnNarrowRefresh(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
		},
	}, 100, 24)
	model.detailOpen = true

	updated := boardApplySnapshot(model, model.snapshot, 48, 20)
	if !updated.detailOpen {
		t.Fatalf("expected detail pane to stay open after refresh on narrow width")
	}
}

func TestBoardTUIApplySnapshotPreservesExpansionStateForHierarchyRows(t *testing.T) {
	t.Parallel()

	parent := boardIssueRow{
		Issue: boardTestIssue("mem-a111111", "Story", "Todo", "Parent story"),
		Hierarchy: boardIssueHierarchy{
			HasChildren: true,
			ChildCount:  2,
		},
	}
	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{parent},
	}, 100, 24)

	if !model.expanded["mem-a111111"] {
		t.Fatalf("expected hierarchy row to default expanded, got %+v", model.expanded)
	}

	model.expanded["mem-a111111"] = false
	updated := boardApplySnapshot(model, boardSnapshot{
		Ready: []boardIssueRow{parent},
	}, 100, 24)

	if updated.expanded["mem-a111111"] {
		t.Fatalf("expected explicit collapsed state to survive refresh, got %+v", updated.expanded)
	}
}

func TestBoardTUIHierarchyNavigationAndFolding(t *testing.T) {
	t.Parallel()

	parent := boardIssueRow{
		Issue: boardTestIssue("mem-a111111", "Story", "Todo", "Parent story"),
		Hierarchy: boardIssueHierarchy{
			ChildIDs:        []string{"mem-b222222", "mem-c333333"},
			ChildCount:      2,
			DescendantCount: 2,
			HasChildren:     true,
		},
	}
	childOne := boardIssueRow{
		Issue: boardTestIssue("mem-b222222", "Task", "Todo", "Child task"),
		Hierarchy: boardIssueHierarchy{
			Depth:       1,
			Path:        []string{"mem-a111111", "mem-b222222"},
			AncestorIDs: []string{"mem-a111111"},
			ParentID:    "mem-a111111",
		},
	}
	childTwo := boardIssueRow{
		Issue: boardTestIssue("mem-c333333", "Bug", "Todo", "Second child"),
		Hierarchy: boardIssueHierarchy{
			Depth:       1,
			Path:        []string{"mem-a111111", "mem-c333333"},
			AncestorIDs: []string{"mem-a111111"},
			ParentID:    "mem-a111111",
		},
	}

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{parent, childOne, childTwo},
	}, 120, 28)

	rows := model.rowsForLane(boardLaneReady)
	if len(rows) != 3 || rows[0].Issue.ID != "mem-a111111" || rows[1].Issue.ID != "mem-b222222" {
		t.Fatalf("expected expanded tree ordering, got %+v", rows)
	}

	model = boardReduce(model, boardActionChild)
	if model.selectedIssue != "mem-b222222" {
		t.Fatalf("expected ] to jump to first child, got %+v", model)
	}

	model = boardReduce(model, boardActionParent)
	if model.selectedIssue != "mem-a111111" {
		t.Fatalf("expected [ to jump to parent, got %+v", model)
	}

	model = boardReduce(model, boardActionCollapse)
	if got := len(model.rowsForLane(boardLaneReady)); got != 1 {
		t.Fatalf("expected collapsed tree to hide children, got %d rows", got)
	}

	model = boardReduce(model, boardActionExpand)
	if got := len(model.rowsForLane(boardLaneReady)); got != 3 {
		t.Fatalf("expected expanded tree to restore children, got %d rows", got)
	}
}

func TestBoardTUINextLaneDoesNotApplyTreePrefixesOrCollapseState(t *testing.T) {
	t.Parallel()

	parent := boardIssueRow{
		Issue: boardTestIssue("mem-a111111", "Story", "Todo", "Parent story"),
		Hierarchy: boardIssueHierarchy{
			ChildIDs:        []string{"mem-b222222"},
			ChildCount:      1,
			DescendantCount: 1,
			HasChildren:     true,
		},
	}
	child := boardIssueRow{
		Issue: boardTestIssue("mem-b222222", "Task", "Todo", "Child task"),
		Hierarchy: boardIssueHierarchy{
			Depth:       1,
			Path:        []string{"mem-a111111", "mem-b222222"},
			AncestorIDs: []string{"mem-a111111"},
			ParentID:    "mem-a111111",
		},
	}

	model := newBoardTUIModel(boardSnapshot{
		LikelyNext: []boardIssueRow{parent, child},
		Ready:      []boardIssueRow{parent, child},
	}, 108, 24)

	rendered := renderBoardTUI(model, false)
	if strings.Contains(rendered, "[-] mem-a111111") || strings.Contains(rendered, "|- mem-b222222") {
		t.Fatalf("expected next lane to stay flat, got:\n%s", rendered)
	}

	model = boardReduce(model, boardActionCollapse)
	if !model.expanded["mem-a111111"] {
		t.Fatalf("expected collapse in next lane to leave tree state unchanged, got %+v", model.expanded)
	}
}

func TestBoardTUISearchFromNextPrefersStructuralLane(t *testing.T) {
	t.Parallel()

	row := boardIssueRow{
		Issue: boardTestIssue("mem-e5328a8", "Epic", "Todo", "Gate Ergonomics"),
		Hierarchy: boardIssueHierarchy{
			ChildIDs:        []string{"mem-1b723aa"},
			ChildCount:      1,
			DescendantCount: 1,
			HasChildren:     true,
		},
	}

	model := newBoardTUIModel(boardSnapshot{
		LikelyNext: []boardIssueRow{row},
		Ready:      []boardIssueRow{row},
	}, 120, 28)

	var quit bool
	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionSearchOpen})
	if quit || !model.searchOpen {
		t.Fatalf("expected search to open, got %+v quit=%v", model, quit)
	}
	for _, ch := range "e5328a8" {
		model, quit = boardHandleInput(model, boardKeyInput{text: string(ch)})
		if quit {
			t.Fatalf("did not expect text entry to quit")
		}
	}

	results := boardSearchResults(model)
	if len(results) != 1 || results[0].lane != boardLaneReady {
		t.Fatalf("expected search to prefer ready lane over next, got %+v", results)
	}

	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionToggleDetail})
	if quit || model.searchOpen {
		t.Fatalf("expected enter to close search and jump, got %+v quit=%v", model, quit)
	}
	if model.lane != boardLaneReady || model.selectedIssue != "mem-e5328a8" {
		t.Fatalf("expected search selection to focus ready lane issue, got %+v", model)
	}
}

func TestBoardTUISearchFiltersAndJumpsToResultLane(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
		},
		Blocked: []boardIssueRow{
			{Issue: boardTestIssue("mem-b222222", "Bug", "Blocked", "Blocked match")},
		},
	}, 120, 28)
	model.lane = boardLaneReady
	model.index = 0
	model = boardNormalizeModel(model)

	var quit bool
	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionSearchOpen})
	if quit || !model.searchOpen {
		t.Fatalf("expected / to open search, got %+v quit=%v", model, quit)
	}

	model, quit = boardHandleInput(model, boardKeyInput{text: "b22"})
	if quit {
		t.Fatalf("did not expect text entry to quit")
	}
	results := boardSearchResults(model)
	if len(results) != 1 || results[0].row.Issue.ID != "mem-b222222" {
		t.Fatalf("expected filtered blocked issue result, got %+v", results)
	}

	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionToggleDetail})
	if quit || model.searchOpen {
		t.Fatalf("expected enter to close search and jump, got %+v quit=%v", model, quit)
	}
	if model.lane != boardLaneBlocked || model.selectedIssue != "mem-b222222" {
		t.Fatalf("expected search selection to focus blocked lane result, got %+v", model)
	}
}

func TestBoardTUISearchCancelRestoresPreviousSelection(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
			{Issue: boardTestIssue("mem-b222222", "Task", "Todo", "Ready two")},
		},
	}, 120, 28)
	model.lane = boardLaneReady
	model.index = 1
	model = boardNormalizeModel(model)

	var quit bool
	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionSearchOpen})
	if quit || !model.searchOpen {
		t.Fatalf("expected search to open, got %+v quit=%v", model, quit)
	}
	model, quit = boardHandleInput(model, boardKeyInput{text: "a11"})
	if quit {
		t.Fatalf("did not expect text entry to quit")
	}

	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionQuit})
	if quit {
		t.Fatalf("expected escape during search to cancel, not quit")
	}
	if model.searchOpen || model.lane != boardLaneReady || model.selectedIssue != "mem-b222222" {
		t.Fatalf("expected cancel to restore prior selection, got %+v", model)
	}
}

func TestBoardTUIApplySnapshotKeepsSearchStateAcrossRefresh(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
		},
		Blocked: []boardIssueRow{
			{Issue: boardTestIssue("mem-b222222", "Bug", "Blocked", "Blocked match")},
		},
	}, 120, 28)
	model.lane = boardLaneReady
	model = boardNormalizeModel(model)

	var quit bool
	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionSearchOpen})
	if quit {
		t.Fatalf("did not expect search open to quit")
	}
	model, quit = boardHandleInput(model, boardKeyInput{text: "b22"})
	if quit {
		t.Fatalf("did not expect search text entry to quit")
	}

	updated := boardApplySnapshot(model, boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one refreshed")},
			{Issue: boardTestIssue("mem-c333333", "Task", "Todo", "Extra ready issue")},
		},
		Blocked: []boardIssueRow{
			{Issue: boardTestIssue("mem-b222222", "Bug", "Blocked", "Blocked match refreshed")},
		},
	}, 120, 28)

	if !updated.searchOpen || updated.searchQuery != "b22" {
		t.Fatalf("expected refresh to preserve active search state, got %+v", updated)
	}
	results := boardSearchResults(updated)
	if len(results) != 1 || results[0].row.Issue.ID != "mem-b222222" {
		t.Fatalf("expected refreshed search results to stay focused on the blocked match, got %+v", results)
	}
}

func TestRenderBoardTUIWideShowsDetailPane(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Agent:   "agent-wide-1",
		Summary: boardSummary{Total: 3, Todo: 1, InProgress: 1, Blocked: 1},
		LikelyNext: []boardIssueRow{
			{
				Issue:   boardTestIssue("mem-a111111", "Task", "Todo", "Next one"),
				Score:   220,
				Reasons: []string{"matches the agent's active focus for resume", "has 1 open loop(s) that need continuity"},
			},
		},
		Active:  []boardIssueRow{{Issue: boardTestIssue("mem-b222222", "Task", "InProgress", "Active one")}},
		Blocked: []boardIssueRow{{Issue: boardTestIssue("mem-c333333", "Bug", "Blocked", "Blocked one")}},
	}, 120, 28)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"MEMORI BOARD",
		"NEXT 1",
		"ISSUE DETAIL",
		"mem-a111111 · Next one",
		"[ REASONS ]",
		"focus for resume",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected wide render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUINarrowShowsSinglePaneAndHelp(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")}},
	}, 72, 22)
	model = boardReduce(model, boardActionToggleHelp)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"KEYBOARD",
		"move selection",
		"jump parent / child",
		"quit",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected narrow help render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUIShowsHierarchyCuesInListAndDetail(t *testing.T) {
	t.Parallel()

	parent := boardTestIssue("mem-a111111", "Story", "Todo", "Parent story")
	parent.Description = "Parent work item."
	child := boardTestIssue("mem-b222222", "Task", "Todo", "Child task")
	child.Description = "Child work item."

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{
				Issue: parent,
				Hierarchy: boardIssueHierarchy{
					ChildIDs:        []string{"mem-b222222"},
					ChildCount:      1,
					DescendantCount: 1,
					HasChildren:     true,
				},
			},
			{
				Issue: child,
				Hierarchy: boardIssueHierarchy{
					Depth:           1,
					Path:            []string{"mem-a111111", "mem-b222222"},
					AncestorIDs:     []string{"mem-a111111"},
					ParentID:        "mem-a111111",
					ParentTitle:     "Parent story",
					ParentType:      "Story",
					ParentStatus:    "Todo",
					DescendantCount: 0,
				},
			},
		},
	}, 108, 24)
	model.lane = boardLaneReady
	model.index = 1
	model.detailOpen = true
	model = boardNormalizeModel(model)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"[-] a111111  Parent story",
		"   `- b222222  Child task",
		"[ HIERARCHY ]",
		"path: mem-a111111 > mem-b222222",
		"parent: mem-a111111 (Parent story)",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected hierarchy render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUIShowsConsistentNestedHierarchyPrefixes(t *testing.T) {
	t.Parallel()

	root := boardIssueRow{
		Issue: boardTestIssue("mem-a111111", "Epic", "Todo", "Root epic"),
		Hierarchy: boardIssueHierarchy{
			ChildIDs:        []string{"mem-b222222", "mem-c333333"},
			ChildCount:      2,
			DescendantCount: 3,
			HasChildren:     true,
		},
	}
	child := boardIssueRow{
		Issue: boardTestIssue("mem-b222222", "Story", "Todo", "Nested story"),
		Hierarchy: boardIssueHierarchy{
			Depth:           1,
			Path:            []string{"mem-a111111", "mem-b222222"},
			AncestorIDs:     []string{"mem-a111111"},
			ParentID:        "mem-a111111",
			SiblingIndex:    0,
			SiblingCount:    2,
			ChildIDs:        []string{"mem-d444444"},
			ChildCount:      1,
			DescendantCount: 1,
			HasChildren:     true,
		},
	}
	childSibling := boardIssueRow{
		Issue: boardTestIssue("mem-c333333", "Story", "Todo", "Sibling story"),
		Hierarchy: boardIssueHierarchy{
			Depth:        1,
			Path:         []string{"mem-a111111", "mem-c333333"},
			AncestorIDs:  []string{"mem-a111111"},
			ParentID:     "mem-a111111",
			SiblingIndex: 1,
			SiblingCount: 2,
		},
	}
	grandchild := boardIssueRow{
		Issue: boardTestIssue("mem-d444444", "Task", "Todo", "Grandchild task"),
		Hierarchy: boardIssueHierarchy{
			Depth:        2,
			Path:         []string{"mem-a111111", "mem-b222222", "mem-d444444"},
			AncestorIDs:  []string{"mem-a111111", "mem-b222222"},
			ParentID:     "mem-b222222",
			SiblingCount: 1,
		},
	}

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{root, child, grandchild, childSibling},
	}, 120, 28)
	model.lane = boardLaneReady
	model = boardNormalizeModel(model)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"[-] a111111  Root epic",
		"   [-] b222222  Nested story",
		"      `- d444444  Grandchild task",
		"   `- c333333  Sibling story",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected nested hierarchy render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUIUsesLaneSiblingOrderForLeafBranches(t *testing.T) {
	t.Parallel()

	root := boardIssueRow{
		Issue: boardTestIssue("mem-a111111", "Epic", "Todo", "Root epic"),
		Hierarchy: boardIssueHierarchy{
			ChildIDs:        []string{"mem-b222222", "mem-c333333", "mem-d444444"},
			ChildCount:      3,
			DescendantCount: 3,
			HasChildren:     true,
		},
	}
	first := boardIssueRow{
		Issue: boardTestIssue("mem-b222222", "Story", "Todo", "First child"),
		Hierarchy: boardIssueHierarchy{
			Depth:        1,
			Path:         []string{"mem-a111111", "mem-b222222"},
			AncestorIDs:  []string{"mem-a111111"},
			ParentID:     "mem-a111111",
			SiblingIndex: 2,
			SiblingCount: 3,
		},
	}
	second := boardIssueRow{
		Issue: boardTestIssue("mem-c333333", "Story", "Todo", "Second child"),
		Hierarchy: boardIssueHierarchy{
			Depth:        1,
			Path:         []string{"mem-a111111", "mem-c333333"},
			AncestorIDs:  []string{"mem-a111111"},
			ParentID:     "mem-a111111",
			SiblingIndex: 0,
			SiblingCount: 3,
		},
	}
	last := boardIssueRow{
		Issue: boardTestIssue("mem-d444444", "Story", "Todo", "Third child"),
		Hierarchy: boardIssueHierarchy{
			Depth:        1,
			Path:         []string{"mem-a111111", "mem-d444444"},
			AncestorIDs:  []string{"mem-a111111"},
			ParentID:     "mem-a111111",
			SiblingIndex: 1,
			SiblingCount: 3,
		},
	}

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{root, first, second, last},
	}, 120, 28)
	model.lane = boardLaneReady
	model = boardNormalizeModel(model)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"   |- b222222  First child",
		"   |- c333333  Second child",
		"   `- d444444  Third child",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected lane-order branches to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUIShowsSearchPanel(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
		},
		Blocked: []boardIssueRow{
			{Issue: boardTestIssue("mem-b222222", "Bug", "Blocked", "Blocked match")},
		},
	}, 108, 24)
	model.lane = boardLaneReady
	model = boardNormalizeModel(model)

	var quit bool
	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionSearchOpen})
	if quit {
		t.Fatalf("did not expect search open to quit")
	}
	model, quit = boardHandleInput(model, boardKeyInput{text: "b22"})
	if quit {
		t.Fatalf("did not expect text entry to quit")
	}

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"SEARCH",
		"/b22",
		"BLOCKED",
		"b222222",
		"Search /b22",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected search render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUINarrowShowsSearchPanel(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
		},
		Blocked: []boardIssueRow{
			{Issue: boardTestIssue("mem-b222222", "Bug", "Blocked", "Blocked match")},
		},
	}, 72, 22)
	model.lane = boardLaneReady
	model = boardNormalizeModel(model)

	var quit bool
	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionSearchOpen})
	if quit {
		t.Fatalf("did not expect search open to quit")
	}
	model, quit = boardHandleInput(model, boardKeyInput{text: "b22"})
	if quit {
		t.Fatalf("did not expect text entry to quit")
	}

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"SEARCH",
		"/b22",
		"BLOCKED",
		"Search /b22",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected narrow search render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUIVeryNarrowStillShowsTickets(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "A narrow pane should still show tickets")},
		},
	}, 28, 14)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"BOARD",
		"Ready |",
		"a11111 A narrow pane",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected very narrow render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUINarrowDetailPrefersFullIssueContent(t *testing.T) {
	t.Parallel()

	issue := boardTestIssue("mem-a111111", "Task", "Todo", "Narrow detail")
	issue.Description = "Ship the full issue detail in narrow mode before continuity hints."
	issue.Acceptance = "Description and acceptance criteria remain visible in compact panes."
	issue.References = []string{"docs/board.md", "internal/cli/board_tui.go"}

	model := newBoardTUIModel(boardSnapshot{
		LikelyNext: []boardIssueRow{
			{
				Issue:   issue,
				Reasons: []string{"active focus resume", "open loops waiting"},
			},
		},
	}, 72, 18)
	model.detailOpen = true

	rendered := renderBoardTUI(model, false)
	descriptionIndex := strings.Index(rendered, "[ DESCRIPTION ]")
	acceptanceIndex := strings.Index(rendered, "[ ACCEPTANCE ]")
	reasonsIndex := strings.Index(rendered, "[ REASONS ]")
	for _, want := range []string{
		"[ DESCRIPTION ]",
		"Ship the full issue detail",
		"[ ACCEPTANCE ]",
		"compact panes.",
		"[ REFERENCES ]",
		"docs/board.md",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected narrow detail render to contain %q, got:\n%s", want, rendered)
		}
	}
	if reasonsIndex != -1 && descriptionIndex != -1 && reasonsIndex < descriptionIndex {
		t.Fatalf("expected reasons to come after full issue detail in narrow mode, got:\n%s", rendered)
	}
	if reasonsIndex != -1 && acceptanceIndex != -1 && reasonsIndex < acceptanceIndex {
		t.Fatalf("expected reasons to come after acceptance details in narrow mode, got:\n%s", rendered)
	}
}

func TestReadBoardInputParsesSearchKeysAndEscape(t *testing.T) {
	t.Parallel()

	searchOpen, err := readBoardInput(bufio.NewReader(strings.NewReader("/")))
	if err != nil {
		t.Fatalf("read search open: %v", err)
	}
	if searchOpen.action != boardActionSearchOpen {
		t.Fatalf("expected / to open search, got %+v", searchOpen)
	}

	text, err := readBoardInput(bufio.NewReader(strings.NewReader("a")))
	if err != nil {
		t.Fatalf("read text: %v", err)
	}
	if text.text != "a" {
		t.Fatalf("expected printable key to become search text, got %+v", text)
	}

	backspace, err := readBoardInput(bufio.NewReader(strings.NewReader("\x7f")))
	if err != nil {
		t.Fatalf("read backspace: %v", err)
	}
	if !backspace.backspace {
		t.Fatalf("expected delete to map to backspace, got %+v", backspace)
	}

	escape, err := readBoardInput(bufio.NewReader(strings.NewReader("\x1b")))
	if err != nil {
		t.Fatalf("read escape: %v", err)
	}
	if escape.action != boardActionQuit {
		t.Fatalf("expected bare escape to map to quit/cancel, got %+v", escape)
	}
}

func boardTestIssue(id, issueType, status, title string) store.Issue {
	return store.Issue{ID: id, Type: issueType, Status: status, Title: title}
}
