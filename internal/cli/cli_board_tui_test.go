package cli

import (
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

func TestBoardTUIApplySnapshotKeepsNextLaneStickyAcrossRefresh(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		LikelyNext: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Next issue")},
		},
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Next issue")},
			{Issue: boardTestIssue("mem-b222222", "Task", "Todo", "Ready issue")},
		},
	}, 100, 24)
	model.lane = boardLaneNext
	model.index = 0
	model.selectedIssue = "mem-a111111"
	model = boardNormalizeModel(model)

	updated := boardApplySnapshot(model, boardSnapshot{
		LikelyNext: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Next issue refreshed")},
		},
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Next issue refreshed")},
			{Issue: boardTestIssue("mem-b222222", "Task", "Todo", "Ready issue")},
		},
	}, 100, 24)

	if updated.lane != boardLaneNext || updated.selectedIssue != "mem-a111111" {
		t.Fatalf("expected refresh to keep next lane focus, got %+v", updated)
	}
}

func TestBoardTUIApplySnapshotFallsBackWhenNextLaneDisappears(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		LikelyNext: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Next issue")},
		},
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Next issue")},
		},
	}, 100, 24)
	model.lane = boardLaneNext
	model.index = 0
	model.selectedIssue = "mem-a111111"
	model = boardNormalizeModel(model)

	updated := boardApplySnapshot(model, boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready-only issue")},
		},
	}, 100, 24)

	if updated.lane != boardLaneReady || updated.selectedIssue != "mem-a111111" {
		t.Fatalf("expected refresh to fall back to ready when next disappears, got %+v", updated)
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

func TestBoardTUIToggleHistoryRevealsDoneAndWontDoLanes(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
		},
		Done: []boardIssueRow{
			{Issue: boardTestIssue("mem-b222222", "Task", "Done", "Done one")},
		},
		WontDo: []boardIssueRow{
			{Issue: boardTestIssue("mem-c333333", "Bug", "WontDo", "Declined one")},
		},
	}, 120, 28)

	if got := model.availableLanes(); len(got) != 1 || got[0] != boardLaneReady {
		t.Fatalf("expected default view to stay actionable-only, got %+v", got)
	}

	model = boardReduce(model, boardActionToggleHistory)
	if !model.showHistory {
		t.Fatalf("expected history toggle to enable all-work view")
	}
	if got := model.availableLanes(); len(got) != 3 {
		t.Fatalf("expected done and wontdo lanes to become navigable, got %+v", got)
	}

	model = boardReduce(model, boardActionNextLane)
	if model.lane != boardLaneDone {
		t.Fatalf("expected next lane step to enter done lane, got %+v", model)
	}
	model = boardReduce(model, boardActionNextLane)
	if model.lane != boardLaneWontDo {
		t.Fatalf("expected second lane step to enter wontdo lane, got %+v", model)
	}
}

func TestBoardTUISearchIncludesHistoryOnlyWhenVisible(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
		},
		Done: []boardIssueRow{
			{Issue: boardTestIssue("mem-d444444", "Task", "Done", "Done match")},
		},
	}, 120, 28)
	model.lane = boardLaneReady
	model = boardNormalizeModel(model)

	var quit bool
	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionSearchOpen})
	if quit {
		t.Fatal("did not expect search open to quit")
	}
	model, quit = boardHandleInput(model, boardKeyInput{text: "d444"})
	if quit {
		t.Fatal("did not expect search text entry to quit")
	}
	if got := boardSearchResults(model); len(got) != 0 {
		t.Fatalf("expected done lane to stay hidden from default search, got %+v", got)
	}

	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionToggleHistory})
	if quit {
		t.Fatal("did not expect history toggle to quit")
	}
	results := boardSearchResults(model)
	if len(results) != 1 || results[0].lane != boardLaneDone || results[0].row.Issue.ID != "mem-d444444" {
		t.Fatalf("expected history search to reveal done match, got %+v", results)
	}
}

func boardTestIssue(id, issueType, status, title string) store.Issue {
	return store.Issue{ID: id, Type: issueType, Status: status, Title: title}
}
