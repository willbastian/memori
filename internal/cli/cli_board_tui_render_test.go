package cli

import (
	"strings"
	"testing"
)

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
