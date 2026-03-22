package cli

import tea "github.com/charmbracelet/bubbletea"

type boardKeyBinding struct {
	label       string
	description string
	input       boardKeyInput
	keyTypes    []tea.KeyType
	runes       []string
	help        bool
}

var boardKeyBindings = []boardKeyBinding{
	{
		label:       "up / k",
		description: "move selection",
		input:       boardKeyInput{action: boardActionUp},
		keyTypes:    []tea.KeyType{tea.KeyUp},
		runes:       []string{"k"},
		help:        true,
	},
	{
		label:       "down / j",
		description: "move selection",
		input:       boardKeyInput{action: boardActionDown},
		keyTypes:    []tea.KeyType{tea.KeyDown},
		runes:       []string{"j"},
		help:        true,
	},
	{
		label:       "left / h",
		description: "switch lanes",
		input:       boardKeyInput{action: boardActionPrevLane},
		keyTypes:    []tea.KeyType{tea.KeyLeft},
		runes:       []string{"h"},
		help:        true,
	},
	{
		label:       "right / l",
		description: "switch lanes",
		input:       boardKeyInput{action: boardActionNextLane},
		keyTypes:    []tea.KeyType{tea.KeyRight},
		runes:       []string{"l"},
		help:        true,
	},
	{
		label:       "enter",
		description: "toggle issue detail / confirm search",
		input:       boardKeyInput{action: boardActionToggleDetail},
		keyTypes:    []tea.KeyType{tea.KeyEnter, tea.KeySpace},
		help:        true,
	},
	{
		label:       "/",
		description: "search visible issue ids",
		input:       boardKeyInput{action: boardActionSearchOpen},
		runes:       []string{"/"},
		help:        true,
	},
	{
		label:       "f",
		description: "toggle actionable/all work",
		input:       boardKeyInput{action: boardActionToggleHistory},
		runes:       []string{"f"},
		help:        true,
	},
	{
		label:       "?",
		description: "toggle help",
		input:       boardKeyInput{action: boardActionToggleHelp},
		runes:       []string{"?"},
		help:        true,
	},
	{
		label:       "c",
		description: "toggle detail / continuity",
		input:       boardKeyInput{action: boardActionToggleContinuity},
		runes:       []string{"c"},
		help:        true,
	},
	{
		label:       "[ / ]",
		description: "jump parent / child",
		input:       boardKeyInput{action: boardActionParent},
		runes:       []string{"["},
		help:        true,
	},
	{
		label:       "[ / ]",
		description: "jump parent / child",
		input:       boardKeyInput{action: boardActionChild},
		runes:       []string{"]"},
	},
	{
		label:       "{ / }",
		description: "collapse / expand subtree",
		input:       boardKeyInput{action: boardActionCollapse},
		runes:       []string{"{"},
		help:        true,
	},
	{
		label:       "{ / }",
		description: "collapse / expand subtree",
		input:       boardKeyInput{action: boardActionExpand},
		runes:       []string{"}"},
	},
	{
		label:       "g / G",
		description: "jump top / bottom",
		input:       boardKeyInput{action: boardActionTop},
		runes:       []string{"g"},
		help:        true,
	},
	{
		label:       "g / G",
		description: "jump top / bottom",
		input:       boardKeyInput{action: boardActionBottom},
		runes:       []string{"G"},
	},
	{
		label:       "ctrl+u / pgup",
		description: "scroll inspector up",
		input:       boardKeyInput{action: boardActionPanelPageUp},
		keyTypes:    []tea.KeyType{tea.KeyCtrlU, tea.KeyPgUp},
		help:        true,
	},
	{
		label:       "ctrl+d / pgdn",
		description: "scroll inspector down",
		input:       boardKeyInput{action: boardActionPanelPageDown},
		keyTypes:    []tea.KeyType{tea.KeyCtrlD, tea.KeyPgDown},
		help:        true,
	},
	{
		label:       "backspace",
		description: "edit search",
		input:       boardKeyInput{backspace: true},
		keyTypes:    []tea.KeyType{tea.KeyBackspace, tea.KeyCtrlH},
	},
	{
		label:       "q / esc",
		description: "quit / cancel search",
		input:       boardKeyInput{action: boardActionQuit},
		keyTypes:    []tea.KeyType{tea.KeyEsc, tea.KeyCtrlC},
		runes:       []string{"q"},
		help:        true,
	},
}

func boardHelpBindings() []boardKeyBinding {
	entries := make([]boardKeyBinding, 0, len(boardKeyBindings))
	seen := make(map[string]struct{}, len(boardKeyBindings))
	for _, binding := range boardKeyBindings {
		if !binding.help {
			continue
		}
		key := binding.label + "\x00" + binding.description
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		entries = append(entries, binding)
	}
	return entries
}
