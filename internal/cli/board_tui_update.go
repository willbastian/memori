package cli

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
	case boardActionQuit:
		return model
	}
	return boardNormalizeModel(model)
}

func boardHandleInput(model boardTUIModel, input boardKeyInput) (boardTUIModel, bool) {
	if model.searchOpen {
		return boardHandleSearchInput(model, input)
	}

	if input.action == boardActionSearchOpen {
		model.searchOpen = true
		model.searchQuery = ""
		model.searchIndex = 0
		model.searchOrigin = model.selectedIssue
		model.searchLane = model.lane
		return boardNormalizeModel(model), false
	}

	model = boardReduce(model, input.action)
	return model, input.action == boardActionQuit
}

func boardHandleSearchInput(model boardTUIModel, input boardKeyInput) (boardTUIModel, bool) {
	switch {
	case input.action == boardActionQuit:
		model.searchOpen = false
		model.searchQuery = ""
		model.searchIndex = 0
		if model.searchOrigin != "" {
			model = boardFocusIssuePreferred(model, model.searchOrigin, boardLanePreference(model.searchLane))
		}
		return model, false
	case input.action == boardActionToggleDetail:
		results := boardSearchResults(model)
		if len(results) == 0 {
			return model, false
		}
		model.searchOpen = false
		model.searchQuery = ""
		selected := results[minInt(model.searchIndex, len(results)-1)]
		model = boardFocusIssuePreferred(model, selected.row.Issue.ID, boardLanePreference(selected.lane))
		return model, false
	case input.backspace:
		if len(model.searchQuery) > 0 {
			model.searchQuery = model.searchQuery[:len(model.searchQuery)-1]
		}
		model.searchIndex = 0
		return boardNormalizeModel(model), false
	case input.text != "":
		model.searchQuery += input.text
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
