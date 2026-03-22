package cli

import (
	"strings"
	"unicode"

	tea "github.com/charmbracelet/bubbletea"
)

func boardKeyInputFromKeyMsg(msg tea.KeyMsg) (boardKeyInput, bool) {
	for _, binding := range boardKeyBindings {
		for _, keyType := range binding.keyTypes {
			if msg.Type == keyType {
				return binding.input, true
			}
		}
	}

	if msg.Type != tea.KeyRunes || len(msg.Runes) == 0 {
		return boardKeyInput{}, false
	}

	text := string(msg.Runes)
	for _, binding := range boardKeyBindings {
		for _, keyRune := range binding.runes {
			if text == keyRune {
				return binding.input, true
			}
		}
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
