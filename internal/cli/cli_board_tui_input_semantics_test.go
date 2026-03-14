package cli

import "testing"

func TestBoardTUISearchNavigationClampsWithinResults(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
			{Issue: boardTestIssue("mem-b222222", "Task", "Todo", "Ready two")},
		},
		Blocked: []boardIssueRow{
			{Issue: boardTestIssue("mem-c333333", "Bug", "Blocked", "Blocked three")},
		},
	}, 120, 28)
	model.lane = boardLaneReady
	model = boardNormalizeModel(model)

	var quit bool
	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionSearchOpen})
	if quit {
		t.Fatal("did not expect search open to quit")
	}
	model, quit = boardHandleInput(model, boardKeyInput{text: "mem"})
	if quit {
		t.Fatal("did not expect search text entry to quit")
	}

	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionBottom})
	if quit || model.searchIndex != 2 {
		t.Fatalf("expected bottom to clamp at last result, got %+v quit=%v", model, quit)
	}

	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionDown})
	if quit || model.searchIndex != 2 {
		t.Fatalf("expected down to stay clamped at last result, got %+v quit=%v", model, quit)
	}

	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionTop})
	if quit || model.searchIndex != 0 {
		t.Fatalf("expected top to reset search selection, got %+v quit=%v", model, quit)
	}

	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionUp})
	if quit || model.searchIndex != 0 {
		t.Fatalf("expected up to stay clamped at first result, got %+v quit=%v", model, quit)
	}
}

func TestBoardTUISearchBackspaceResetsIndexAndIgnoresEmptyConfirm(t *testing.T) {
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
	if quit {
		t.Fatal("did not expect search open to quit")
	}
	model, quit = boardHandleInput(model, boardKeyInput{text: "zzz"})
	if quit {
		t.Fatal("did not expect search text entry to quit")
	}
	model.searchIndex = 3

	model, quit = boardHandleInput(model, boardKeyInput{backspace: true})
	if quit || model.searchQuery != "zz" || model.searchIndex != 0 {
		t.Fatalf("expected backspace to shrink query and reset index, got %+v quit=%v", model, quit)
	}

	model, quit = boardHandleInput(model, boardKeyInput{backspace: true})
	if quit || model.searchQuery != "z" || model.searchIndex != 0 {
		t.Fatalf("expected second backspace to keep resetting index, got %+v quit=%v", model, quit)
	}

	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionToggleDetail})
	if quit {
		t.Fatal("did not expect empty search confirm to quit")
	}
	if !model.searchOpen || model.selectedIssue != "mem-b222222" || model.lane != boardLaneReady {
		t.Fatalf("expected empty search confirm to keep search open and preserve selection, got %+v", model)
	}
}

func TestBoardTUIHelpNavigationKeysCloseHelpBeforeSwitchingLanes(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
		},
		Blocked: []boardIssueRow{
			{Issue: boardTestIssue("mem-b222222", "Bug", "Blocked", "Blocked one")},
		},
	}, 120, 28)
	model.lane = boardLaneReady
	model.helpOpen = true
	model = boardNormalizeModel(model)

	model = boardReduce(model, boardActionPrevLane)
	if model.helpOpen || model.lane != boardLaneReady {
		t.Fatalf("expected prev lane to close help without moving lanes, got %+v", model)
	}

	model.helpOpen = true
	model = boardReduce(model, boardActionNextLane)
	if model.helpOpen || model.lane != boardLaneReady {
		t.Fatalf("expected next lane to close help without moving lanes, got %+v", model)
	}
}

func TestBoardTUIHandleQuitReturnsQuitFlag(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
		},
	}, 120, 28)

	next, quit := boardHandleInput(model, boardKeyInput{action: boardActionQuit})
	if !quit {
		t.Fatal("expected quit action to return quit=true")
	}
	if next.selectedIssue != model.selectedIssue || next.lane != model.lane {
		t.Fatalf("expected quit action to preserve current selection, got %+v", next)
	}
}
