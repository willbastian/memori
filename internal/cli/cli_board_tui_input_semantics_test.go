package cli

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

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

func TestBoardHandleSearchInputUsesTeaKeyMsgsForEditCancelAndConfirm(t *testing.T) {
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
	model = boardNormalizeModel(model)
	model, _ = boardHandleInput(model, boardKeyInput{action: boardActionSearchOpen})

	updated, quit := boardHandleSearchInput(model, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("b22")})
	if quit {
		t.Fatal("did not expect tea search input update to quit")
	}
	if updated.searchQuery != "b22" || updated.searchIndex != 0 {
		t.Fatalf("expected tea search text entry to update query and reset index, got %+v", updated)
	}

	updated, quit = boardHandleSearchInput(updated, tea.KeyMsg{Type: tea.KeyDown})
	if quit || updated.searchIndex != 0 {
		t.Fatalf("expected down to clamp to the only result, got %+v quit=%v", updated, quit)
	}

	updated, quit = boardHandleSearchInput(updated, tea.KeyMsg{Type: tea.KeyEnter})
	if quit {
		t.Fatal("did not expect enter confirm to quit")
	}
	if updated.searchOpen || updated.selectedIssue != "mem-b222222" || updated.lane != boardLaneBlocked {
		t.Fatalf("expected enter confirm to jump to blocked issue, got %+v", updated)
	}
	if updated.toast.message == "" {
		t.Fatalf("expected successful search confirm to set a toast, got %+v", updated.toast)
	}

	model, _ = boardHandleInput(model, boardKeyInput{action: boardActionSearchOpen})
	model.searchInput.SetValue("b22")
	model.searchQuery = model.searchInput.Value()
	model = boardNormalizeModel(model)
	updated, quit = boardHandleSearchInput(model, tea.KeyMsg{Type: tea.KeyEsc})
	if quit {
		t.Fatal("did not expect esc cancel to quit")
	}
	if updated.searchOpen || updated.selectedIssue != model.searchOrigin || updated.lane != model.searchLane {
		t.Fatalf("expected esc to restore origin selection and close search, got %+v", updated)
	}
}

func TestBoardHandleSearchInputSupportsHistoryToggleAndBoundsNavigation(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")}},
		Done:  []boardIssueRow{{Issue: boardTestIssue("mem-c333333", "Task", "Done", "Done one")}},
	}, 120, 28)
	model = boardNormalizeModel(model)
	model, _ = boardHandleInput(model, boardKeyInput{action: boardActionSearchOpen})

	updated, quit := boardHandleSearchInput(model, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}})
	if quit || !updated.showHistory {
		t.Fatalf("expected f to toggle history inside search, got %+v quit=%v", updated, quit)
	}

	updated, quit = boardHandleSearchInput(updated, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("mem")})
	if quit {
		t.Fatal("did not expect text entry to quit")
	}
	updated, quit = boardHandleSearchInput(updated, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'G'}})
	if quit || updated.searchIndex != len(boardSearchResults(updated))-1 {
		t.Fatalf("expected G to jump to the last result, got index=%d len=%d quit=%v", updated.searchIndex, len(boardSearchResults(updated)), quit)
	}
	updated, quit = boardHandleSearchInput(updated, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'g'}})
	if quit || updated.searchIndex != 0 {
		t.Fatalf("expected g to jump back to the first result, got %+v quit=%v", updated, quit)
	}
}
