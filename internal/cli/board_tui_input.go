package cli

import (
	"strings"
	"unicode"

	tea "github.com/charmbracelet/bubbletea"
)

func boardKeyInputFromKeyMsg(msg tea.KeyMsg) (boardKeyInput, bool) {
	switch msg.Type {
	case tea.KeyUp:
		return boardKeyInput{action: boardActionUp}, true
	case tea.KeyDown:
		return boardKeyInput{action: boardActionDown}, true
	case tea.KeyLeft:
		return boardKeyInput{action: boardActionPrevLane}, true
	case tea.KeyRight:
		return boardKeyInput{action: boardActionNextLane}, true
	case tea.KeyEnter, tea.KeySpace:
		return boardKeyInput{action: boardActionToggleDetail}, true
	case tea.KeyBackspace:
		return boardKeyInput{backspace: true}, true
	case tea.KeyEsc, tea.KeyCtrlC:
		return boardKeyInput{action: boardActionQuit}, true
	}

	if msg.Type != tea.KeyRunes || len(msg.Runes) == 0 {
		return boardKeyInput{}, false
	}

	text := string(msg.Runes)
	switch text {
	case "/":
		return boardKeyInput{action: boardActionSearchOpen}, true
	case "q":
		return boardKeyInput{action: boardActionQuit}, true
	case "j":
		return boardKeyInput{action: boardActionDown}, true
	case "k":
		return boardKeyInput{action: boardActionUp}, true
	case "h":
		return boardKeyInput{action: boardActionPrevLane}, true
	case "l":
		return boardKeyInput{action: boardActionNextLane}, true
	case "g":
		return boardKeyInput{action: boardActionTop}, true
	case "G":
		return boardKeyInput{action: boardActionBottom}, true
	case "?":
		return boardKeyInput{action: boardActionToggleHelp}, true
	case "c":
		return boardKeyInput{action: boardActionToggleContinuity}, true
	case "f":
		return boardKeyInput{action: boardActionToggleHistory}, true
	case "[":
		return boardKeyInput{action: boardActionParent}, true
	case "]":
		return boardKeyInput{action: boardActionChild}, true
	case "{":
		return boardKeyInput{action: boardActionCollapse}, true
	case "}":
		return boardKeyInput{action: boardActionExpand}, true
	}

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
