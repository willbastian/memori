package cli

import (
	"strings"
	"unicode"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
)

func boardKeyInputFromKeyMsg(msg tea.KeyMsg) (boardKeyInput, bool) {
	switch {
	case key.Matches(msg, boardKeys.Up):
		return boardKeyInput{action: boardActionUp}, true
	case key.Matches(msg, boardKeys.Down):
		return boardKeyInput{action: boardActionDown}, true
	case key.Matches(msg, boardKeys.PrevLane):
		return boardKeyInput{action: boardActionPrevLane}, true
	case key.Matches(msg, boardKeys.NextLane):
		return boardKeyInput{action: boardActionNextLane}, true
	case key.Matches(msg, boardKeys.ToggleDetail):
		return boardKeyInput{action: boardActionToggleDetail}, true
	case key.Matches(msg, boardKeys.Search):
		return boardKeyInput{action: boardActionSearchOpen}, true
	case key.Matches(msg, boardKeys.ToggleHistory):
		return boardKeyInput{action: boardActionToggleHistory}, true
	case key.Matches(msg, boardKeys.ToggleHelp):
		return boardKeyInput{action: boardActionToggleHelp}, true
	case key.Matches(msg, boardKeys.ToggleContinuity):
		return boardKeyInput{action: boardActionToggleContinuity}, true
	case key.Matches(msg, boardKeys.Parent):
		return boardKeyInput{action: boardActionParent}, true
	case key.Matches(msg, boardKeys.Child):
		return boardKeyInput{action: boardActionChild}, true
	case key.Matches(msg, boardKeys.Collapse):
		return boardKeyInput{action: boardActionCollapse}, true
	case key.Matches(msg, boardKeys.Expand):
		return boardKeyInput{action: boardActionExpand}, true
	case key.Matches(msg, boardKeys.Top):
		return boardKeyInput{action: boardActionTop}, true
	case key.Matches(msg, boardKeys.Bottom):
		return boardKeyInput{action: boardActionBottom}, true
	case key.Matches(msg, boardKeys.PanelPageUp):
		return boardKeyInput{action: boardActionPanelPageUp}, true
	case key.Matches(msg, boardKeys.PanelPageDown):
		return boardKeyInput{action: boardActionPanelPageDown}, true
	case key.Matches(msg, boardKeys.Backspace):
		return boardKeyInput{backspace: true}, true
	case key.Matches(msg, boardKeys.Quit):
		return boardKeyInput{action: boardActionQuit}, true
	}

	if msg.Type != tea.KeyRunes || len(msg.Runes) == 0 {
		return boardKeyInput{}, false
	}

	text := string(msg.Runes)
	if isPrintableInput(text) {
		return boardKeyInput{text: text}, true
	}
	return boardKeyInput{}, false
}

func isPrintableInput(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}
	for _, r := range text {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
