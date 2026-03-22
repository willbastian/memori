package cli

import "github.com/charmbracelet/bubbles/key"

type boardKeyMap struct {
	Up               key.Binding
	Down             key.Binding
	PrevLane         key.Binding
	NextLane         key.Binding
	ToggleDetail     key.Binding
	Search           key.Binding
	ToggleHistory    key.Binding
	ToggleHelp       key.Binding
	ToggleContinuity key.Binding
	Parent           key.Binding
	Child            key.Binding
	Collapse         key.Binding
	Expand           key.Binding
	Top              key.Binding
	Bottom           key.Binding
	PanelPageUp      key.Binding
	PanelPageDown    key.Binding
	Backspace        key.Binding
	Quit             key.Binding
}

var boardKeys = boardKeyMap{
	Up:               key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("up/k", "move selection")),
	Down:             key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("down/j", "move selection")),
	PrevLane:         key.NewBinding(key.WithKeys("left", "h"), key.WithHelp("left/h", "switch lanes")),
	NextLane:         key.NewBinding(key.WithKeys("right", "l"), key.WithHelp("right/l", "switch lanes")),
	ToggleDetail:     key.NewBinding(key.WithKeys("enter", " "), key.WithHelp("enter", "detail / confirm search")),
	Search:           key.NewBinding(key.WithKeys("/"), key.WithHelp("/", "search issue ids")),
	ToggleHistory:    key.NewBinding(key.WithKeys("f"), key.WithHelp("f", "toggle actionable/all work")),
	ToggleHelp:       key.NewBinding(key.WithKeys("?"), key.WithHelp("?", "toggle help")),
	ToggleContinuity: key.NewBinding(key.WithKeys("c"), key.WithHelp("c", "toggle detail / continuity")),
	Parent:           key.NewBinding(key.WithKeys("["), key.WithHelp("[", "jump parent")),
	Child:            key.NewBinding(key.WithKeys("]"), key.WithHelp("]", "jump child")),
	Collapse:         key.NewBinding(key.WithKeys("{"), key.WithHelp("{", "collapse subtree")),
	Expand:           key.NewBinding(key.WithKeys("}"), key.WithHelp("}", "expand subtree")),
	Top:              key.NewBinding(key.WithKeys("g"), key.WithHelp("g", "jump top")),
	Bottom:           key.NewBinding(key.WithKeys("G"), key.WithHelp("G", "jump bottom")),
	PanelPageUp:      key.NewBinding(key.WithKeys("ctrl+u", "pgup"), key.WithHelp("ctrl+u/pgup", "scroll inspector up")),
	PanelPageDown:    key.NewBinding(key.WithKeys("ctrl+d", "pgdown"), key.WithHelp("ctrl+d/pgdn", "scroll inspector down")),
	Backspace:        key.NewBinding(key.WithKeys("backspace", "ctrl+h")),
	Quit:             key.NewBinding(key.WithKeys("esc", "ctrl+c", "q"), key.WithHelp("q/esc", "quit / cancel search")),
}

func (boardKeyMap) ShortHelp() []key.Binding {
	return []key.Binding{
		boardKeys.Up,
		boardKeys.PrevLane,
		boardKeys.ToggleDetail,
		boardKeys.Search,
		boardKeys.ToggleHistory,
		boardKeys.ToggleHelp,
		boardKeys.Quit,
	}
}

func (boardKeyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{
		{
			boardKeys.Up,
			boardKeys.Down,
			boardKeys.PrevLane,
			boardKeys.NextLane,
			boardKeys.Top,
			boardKeys.Bottom,
		},
		{
			boardKeys.ToggleDetail,
			boardKeys.ToggleContinuity,
			boardKeys.PanelPageUp,
			boardKeys.PanelPageDown,
		},
		{
			boardKeys.Search,
			boardKeys.ToggleHistory,
			boardKeys.Parent,
			boardKeys.Child,
			boardKeys.Collapse,
			boardKeys.Expand,
			boardKeys.ToggleHelp,
			boardKeys.Quit,
		},
	}
}
