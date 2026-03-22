package cli

import (
	"strings"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
)

func boardReduce(model boardTUIModel, action boardAction) boardTUIModel {
	switch action {
	case boardActionUp:
		model.index--
	case boardActionDown:
		model.index++
	case boardActionPrevLane:
		if model.helpOpen {
			model.helpOpen = false
		} else {
			model.lane = boardShiftLane(model, -1)
		}
	case boardActionNextLane:
		if model.helpOpen {
			model.helpOpen = false
		} else {
			model.lane = boardShiftLane(model, 1)
		}
	case boardActionTop:
		model.index = 0
	case boardActionBottom:
		model.index = len(model.rows()) - 1
	case boardActionToggleDetail:
		model.detailOpen = !model.detailOpen
		if model.detailOpen {
			model.panelMode = boardPanelModeDetail
		}
	case boardActionToggleContinuity:
		model.detailOpen = true
		if model.panelMode == boardPanelModeContinuity {
			model.panelMode = boardPanelModeDetail
		} else {
			model.panelMode = boardPanelModeContinuity
		}
	case boardActionToggleHelp:
		model.helpOpen = !model.helpOpen
	case boardActionParent:
		if row, ok := model.selectedRow(); ok && boardLaneSupportsHierarchy(model.lane) && row.Hierarchy.ParentID != "" {
			model = boardFocusIssue(model, row.Hierarchy.ParentID)
		}
	case boardActionChild:
		if row, ok := model.selectedRow(); ok && boardLaneSupportsHierarchy(model.lane) {
			for _, childID := range row.Hierarchy.ChildIDs {
				if next := boardFocusIssue(model, childID); next.selectedIssue == childID {
					model = next
					break
				}
			}
		}
	case boardActionCollapse:
		if row, ok := model.selectedRow(); ok && boardLaneSupportsHierarchy(model.lane) && row.Hierarchy.HasChildren {
			model.expanded[row.Issue.ID] = false
		}
	case boardActionExpand:
		if row, ok := model.selectedRow(); ok && boardLaneSupportsHierarchy(model.lane) && row.Hierarchy.HasChildren {
			model.expanded[row.Issue.ID] = true
		}
	case boardActionToggleHistory:
		model.showHistory = !model.showHistory
	case boardActionPanelPageUp:
		if model.detailOpen {
			model = boardAdjustPanelScroll(model, -maxInt(boardCurrentPanelBodyHeight(model)-2, 1))
		}
	case boardActionPanelPageDown:
		if model.detailOpen {
			model = boardAdjustPanelScroll(model, maxInt(boardCurrentPanelBodyHeight(model)-2, 1))
		}
	case boardActionQuit:
		return model
	}
	return boardNormalizeModel(model)
}

func boardHandleInput(model boardTUIModel, input boardKeyInput) (boardTUIModel, bool) {
	if model.searchOpen {
		switch {
		case input.action == boardActionQuit:
			model.searchOpen = false
			model.searchQuery = ""
			model.searchInput.Blur()
			model.searchInput.Reset()
			model.searchIndex = 0
			if model.searchOrigin != "" {
				model = boardFocusIssuePreferred(model, model.searchOrigin, boardLanePreference(model.searchLane, model.navigationLanes()))
			}
			return model, false
		case input.action == boardActionToggleDetail:
			results := boardSearchResults(model)
			if len(results) == 0 {
				return model, false
			}
			model.searchOpen = false
			model.searchQuery = ""
			model.searchInput.Blur()
			model.searchInput.Reset()
			selected := results[minInt(model.searchIndex, len(results)-1)]
			model = boardFocusIssuePreferred(model, selected.row.Issue.ID, boardLanePreference(selected.lane, model.navigationLanes()))
			model = boardSetToast(model, boardToastToneSuccess, "Jumped to "+selected.row.Issue.ID+" in "+strings.ToLower(boardLaneTitle(selected.lane)))
			return model, false
		case input.action == boardActionToggleHistory:
			model.showHistory = !model.showHistory
			return boardNormalizeModel(model), false
		case input.backspace:
			value := model.searchInput.Value()
			if len(value) > 0 {
				model.searchInput.SetValue(value[:len(value)-1])
			}
			model.searchQuery = model.searchInput.Value()
			model.searchIndex = 0
			return boardNormalizeModel(model), false
		case input.text != "":
			model.searchInput.SetValue(model.searchInput.Value() + input.text)
			model.searchQuery = model.searchInput.Value()
			model.searchIndex = 0
			return boardNormalizeModel(model), false
		case input.action == boardActionDown:
			model.searchIndex++
			return boardNormalizeModel(model), false
		case input.action == boardActionUp:
			model.searchIndex--
			return boardNormalizeModel(model), false
		case input.action == boardActionTop:
			model.searchIndex = 0
			return boardNormalizeModel(model), false
		case input.action == boardActionBottom:
			model.searchIndex = maxInt(len(boardSearchResults(model))-1, 0)
			return boardNormalizeModel(model), false
		default:
			return model, false
		}
	}

	if input.action == boardActionSearchOpen {
		model.searchOpen = true
		model.searchInput.Reset()
		model.searchQuery = ""
		model.searchIndex = 0
		model.searchOrigin = model.selectedIssue
		model.searchLane = model.lane
		_ = model.searchInput.Focus()
		return boardNormalizeModel(model), false
	}

	model = boardReduce(model, input.action)
	return model, input.action == boardActionQuit
}

func boardHandleSearchInput(model boardTUIModel, msg tea.KeyMsg) (boardTUIModel, bool) {
	switch {
	case key.Matches(msg, boardKeys.Quit):
		model.searchOpen = false
		model.searchQuery = ""
		model.searchIndex = 0
		model.searchInput.Blur()
		if model.searchOrigin != "" {
			model = boardFocusIssuePreferred(model, model.searchOrigin, boardLanePreference(model.searchLane, model.navigationLanes()))
		}
		return model, false
	case key.Matches(msg, boardKeys.ToggleDetail):
		results := boardSearchResults(model)
		if len(results) == 0 {
			return model, false
		}
		model.searchOpen = false
		model.searchQuery = ""
		model.searchInput.Blur()
		selected := results[minInt(model.searchIndex, len(results)-1)]
		model = boardFocusIssuePreferred(model, selected.row.Issue.ID, boardLanePreference(selected.lane, model.navigationLanes()))
		model = boardSetToast(model, boardToastToneSuccess, "Jumped to "+selected.row.Issue.ID+" in "+strings.ToLower(boardLaneTitle(selected.lane)))
		return model, false
	case key.Matches(msg, boardKeys.ToggleHistory):
		model.showHistory = !model.showHistory
		model = boardNormalizeModel(model)
		return model, false
	case key.Matches(msg, boardKeys.Down):
		model.searchIndex++
		return boardNormalizeModel(model), false
	case key.Matches(msg, boardKeys.Up):
		model.searchIndex--
		return boardNormalizeModel(model), false
	case key.Matches(msg, boardKeys.Top):
		model.searchIndex = 0
		return boardNormalizeModel(model), false
	case key.Matches(msg, boardKeys.Bottom):
		model.searchIndex = maxInt(len(boardSearchResults(model))-1, 0)
		return boardNormalizeModel(model), false
	default:
		var cmd tea.Cmd
		model.searchInput, cmd = model.searchInput.Update(msg)
		_ = cmd
		model.searchQuery = model.searchInput.Value()
		model.searchIndex = 0
		return boardNormalizeModel(model), false
	}
}
