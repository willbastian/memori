package cli

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func TestBoardKeyInputFromKeyMsgParsesNavigationAndPanelKeys(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		msg       tea.KeyMsg
		want      boardAction
		backspace bool
	}{
		{name: "quit rune", msg: tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}}, want: boardActionQuit},
		{name: "ctrl c", msg: tea.KeyMsg{Type: tea.KeyCtrlC}, want: boardActionQuit},
		{name: "down rune", msg: tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'j'}}, want: boardActionDown},
		{name: "up rune", msg: tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'k'}}, want: boardActionUp},
		{name: "previous lane rune", msg: tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'h'}}, want: boardActionPrevLane},
		{name: "next lane rune", msg: tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'l'}}, want: boardActionNextLane},
		{name: "top", msg: tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'g'}}, want: boardActionTop},
		{name: "bottom", msg: tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'G'}}, want: boardActionBottom},
		{name: "toggle help", msg: tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'?'}}, want: boardActionToggleHelp},
		{name: "toggle continuity", msg: tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'c'}}, want: boardActionToggleContinuity},
		{name: "toggle history", msg: tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}}, want: boardActionToggleHistory},
		{name: "parent", msg: tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'['}}, want: boardActionParent},
		{name: "child", msg: tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{']'}}, want: boardActionChild},
		{name: "collapse", msg: tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'{'}}, want: boardActionCollapse},
		{name: "expand", msg: tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'}'}}, want: boardActionExpand},
		{name: "enter toggles detail", msg: tea.KeyMsg{Type: tea.KeyEnter}, want: boardActionToggleDetail},
		{name: "space toggles detail", msg: tea.KeyMsg{Type: tea.KeySpace}, want: boardActionToggleDetail},
		{name: "backspace", msg: tea.KeyMsg{Type: tea.KeyBackspace}, backspace: true},
		{name: "arrow up", msg: tea.KeyMsg{Type: tea.KeyUp}, want: boardActionUp},
		{name: "arrow down", msg: tea.KeyMsg{Type: tea.KeyDown}, want: boardActionDown},
		{name: "arrow right", msg: tea.KeyMsg{Type: tea.KeyRight}, want: boardActionNextLane},
		{name: "arrow left", msg: tea.KeyMsg{Type: tea.KeyLeft}, want: boardActionPrevLane},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := boardKeyInputFromKeyMsg(tc.msg)
			if !ok {
				t.Fatal("expected key message to be handled")
			}
			if got.action != tc.want || got.backspace != tc.backspace {
				t.Fatalf("expected action=%v backspace=%v, got %+v", tc.want, tc.backspace, got)
			}
		})
	}
}

func TestBoardKeyInputFromKeyMsgParsesSearchKeysAndPrintableText(t *testing.T) {
	t.Parallel()

	searchOpen, ok := boardKeyInputFromKeyMsg(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	if !ok || searchOpen.action != boardActionSearchOpen {
		t.Fatalf("expected / to open search, got %+v ok=%v", searchOpen, ok)
	}

	text, ok := boardKeyInputFromKeyMsg(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'a'}})
	if !ok || text.text != "a" {
		t.Fatalf("expected printable input to become text, got %+v ok=%v", text, ok)
	}

	paste, ok := boardKeyInputFromKeyMsg(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("abc123")})
	if !ok || paste.text != "abc123" {
		t.Fatalf("expected multi-rune printable input to become text, got %+v ok=%v", paste, ok)
	}
}

func TestBoardKeyInputFromKeyMsgIgnoresUnhandledInput(t *testing.T) {
	t.Parallel()

	if _, ok := boardKeyInputFromKeyMsg(tea.KeyMsg{Type: tea.KeyTab}); ok {
		t.Fatal("expected tab to be ignored")
	}
	if _, ok := boardKeyInputFromKeyMsg(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'\x01'}}); ok {
		t.Fatal("expected control rune to be ignored")
	}
}
