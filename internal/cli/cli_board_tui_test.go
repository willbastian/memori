package cli

import (
	"strings"
	"testing"

	"memori/internal/store"
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
		"REASONS:",
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
		"quit",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected narrow help render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func boardTestIssue(id, issueType, status, title string) store.Issue {
	return store.Issue{ID: id, Type: issueType, Status: status, Title: title}
}
