package cli

import (
	"sort"
	"strings"

	"github.com/willbastian/memori/internal/store"
)

type boardLane int

const (
	boardLaneNext boardLane = iota
	boardLaneActive
	boardLaneBlocked
	boardLaneReady
	boardLaneDone
	boardLaneWontDo
)

type boardAction int

const (
	boardActionNone boardAction = iota
	boardActionSearchOpen
	boardActionUp
	boardActionDown
	boardActionPrevLane
	boardActionNextLane
	boardActionTop
	boardActionBottom
	boardActionToggleDetail
	boardActionToggleContinuity
	boardActionToggleHelp
	boardActionParent
	boardActionChild
	boardActionCollapse
	boardActionExpand
	boardActionToggleHistory
	boardActionQuit
)

type boardKeyInput struct {
	action    boardAction
	text      string
	backspace bool
}

type boardPanelMode int

const (
	boardPanelModeDetail boardPanelMode = iota
	boardPanelModeContinuity
)

type boardTUIModel struct {
	snapshot      boardSnapshot
	width         int
	height        int
	lane          boardLane
	index         int
	detailOpen    bool
	helpOpen      bool
	panelMode     boardPanelMode
	selectedIssue string
	expanded      map[string]bool
	searchOpen    bool
	searchQuery   string
	searchIndex   int
	searchOrigin  string
	searchLane    boardLane
	showHistory   bool
	audit         store.ContinuityAuditSnapshot
}

func newBoardTUIModel(snapshot boardSnapshot, width, height int) boardTUIModel {
	model := boardTUIModel{
		snapshot:   snapshot,
		width:      maxInt(width, 24),
		height:     maxInt(height, 10),
		lane:       boardLaneNext,
		detailOpen: width >= 100,
		panelMode:  boardPanelModeDetail,
		expanded:   make(map[string]bool),
	}
	return boardNormalizeModel(model)
}

func boardApplySnapshot(model boardTUIModel, snapshot boardSnapshot, width, height int) boardTUIModel {
	selectedLane := model.lane
	selectedIndex := model.index
	selectedIssue := model.selectedIssue
	expanded := make(map[string]bool, len(model.expanded))
	for issueID, open := range model.expanded {
		expanded[issueID] = open
	}
	model.snapshot = snapshot
	model.width = maxInt(width, 24)
	model.height = maxInt(height, 10)
	model.expanded = expanded
	model = boardNormalizeModel(model)
	if boardLaneInSet(selectedLane, model.availableLanes()) {
		model.lane = selectedLane
		model.index = selectedIndex
		model = boardClampSelection(model)
		if selectedIssue != "" {
			preserved := boardFocusIssuePreferred(model, selectedIssue, []boardLane{selectedLane})
			if preserved.selectedIssue == selectedIssue && preserved.lane == selectedLane {
				return preserved
			}
		}
		return model
	}
	if selectedIssue == "" {
		return model
	}
	model = boardFocusIssue(model, selectedIssue)
	if model.selectedIssue == selectedIssue {
		return model
	}
	return model
}

func boardNormalizeModel(model boardTUIModel) boardTUIModel {
	model = boardSyncExpandedState(model)
	model = boardClampSearchSelection(model)
	lanes := model.availableLanes()
	if len(lanes) == 0 {
		model.lane = model.navigationLanes()[0]
		model.index = 0
		model.selectedIssue = ""
		return model
	}

	if !boardLaneInSet(model.lane, lanes) {
		model.lane = lanes[0]
		model.index = 0
	}

	return boardClampSelection(model)
}

func boardClampSearchSelection(model boardTUIModel) boardTUIModel {
	if !model.searchOpen {
		model.searchIndex = 0
		return model
	}
	results := boardSearchResults(model)
	if len(results) == 0 {
		model.searchIndex = 0
		return model
	}
	if model.searchIndex < 0 {
		model.searchIndex = 0
	}
	if model.searchIndex >= len(results) {
		model.searchIndex = len(results) - 1
	}
	return model
}

func boardSyncExpandedState(model boardTUIModel) boardTUIModel {
	if model.expanded == nil {
		model.expanded = make(map[string]bool)
	}
	valid := make(map[string]struct{})
	for _, lane := range boardAllLanes() {
		for _, row := range model.rawRowsForLane(lane) {
			if !row.Hierarchy.HasChildren {
				continue
			}
			valid[row.Issue.ID] = struct{}{}
			if _, ok := model.expanded[row.Issue.ID]; !ok {
				model.expanded[row.Issue.ID] = true
			}
		}
	}
	for issueID := range model.expanded {
		if _, ok := valid[issueID]; ok {
			continue
		}
		delete(model.expanded, issueID)
	}
	return model
}

func boardClampSelection(model boardTUIModel) boardTUIModel {
	rows := model.rows()
	if len(rows) == 0 {
		model.index = 0
		model.selectedIssue = ""
		return model
	}
	if model.index < 0 {
		model.index = 0
	}
	if model.index >= len(rows) {
		model.index = len(rows) - 1
	}
	model.selectedIssue = rows[model.index].Issue.ID
	return model
}

func (model boardTUIModel) availableLanes() []boardLane {
	lanes := make([]boardLane, 0, len(model.navigationLanes()))
	for _, lane := range model.navigationLanes() {
		if len(model.rowsForLane(lane)) > 0 {
			lanes = append(lanes, lane)
		}
	}
	if len(lanes) == 0 {
		return model.navigationLanes()
	}
	return lanes
}

func boardAllLanes() []boardLane {
	return []boardLane{
		boardLaneNext,
		boardLaneActive,
		boardLaneBlocked,
		boardLaneReady,
		boardLaneDone,
		boardLaneWontDo,
	}
}

func (model boardTUIModel) navigationLanes() []boardLane {
	lanes := []boardLane{
		boardLaneNext,
		boardLaneActive,
		boardLaneBlocked,
		boardLaneReady,
	}
	if model.showHistory {
		lanes = append(lanes, boardLaneDone, boardLaneWontDo)
	}
	return lanes
}

func boardLaneInSet(lane boardLane, lanes []boardLane) bool {
	for _, candidate := range lanes {
		if candidate == lane {
			return true
		}
	}
	return false
}

func boardShiftLane(model boardTUIModel, delta int) boardLane {
	lanes := model.availableLanes()
	if len(lanes) == 0 {
		return model.lane
	}
	current := 0
	for idx, lane := range lanes {
		if lane == model.lane {
			current = idx
			break
		}
	}
	next := (current + delta + len(lanes)) % len(lanes)
	return lanes[next]
}

func (model boardTUIModel) rows() []boardIssueRow {
	return model.rowsForLane(model.lane)
}

func (model boardTUIModel) rowsForLane(lane boardLane) []boardIssueRow {
	rows := model.baseRowsForLane(lane)
	if lane == boardLaneNext {
		return append([]boardIssueRow(nil), rows...)
	}
	return boardVisibleRows(rows, model.expanded)
}

func (model boardTUIModel) baseRowsForLane(lane boardLane) []boardIssueRow {
	switch lane {
	case boardLaneReady, boardLaneActive:
		return boardContextRowsForLane(model.snapshot, lane)
	default:
		return model.rawRowsForLane(lane)
	}
}

func (model boardTUIModel) rawRowsForLane(lane boardLane) []boardIssueRow {
	switch lane {
	case boardLaneNext:
		return model.snapshot.LikelyNext
	case boardLaneActive:
		return model.snapshot.Active
	case boardLaneBlocked:
		return model.snapshot.Blocked
	case boardLaneReady:
		return model.snapshot.Ready
	case boardLaneDone:
		return model.snapshot.Done
	case boardLaneWontDo:
		return model.snapshot.WontDo
	default:
		return nil
	}
}

func (model boardTUIModel) issueCountForLane(lane boardLane) int {
	return len(model.rawRowsForLane(lane))
}

func (model boardTUIModel) selectedRow() (boardIssueRow, bool) {
	rows := model.rows()
	if len(rows) == 0 || model.index < 0 || model.index >= len(rows) {
		return boardIssueRow{}, false
	}
	return rows[model.index], true
}

func boardLaneSupportsHierarchy(lane boardLane) bool {
	return lane != boardLaneNext
}

func boardVisibleRows(rows []boardIssueRow, expanded map[string]bool) []boardIssueRow {
	if len(rows) <= 1 {
		return append([]boardIssueRow(nil), rows...)
	}
	rowByID := make(map[string]boardIssueRow, len(rows))
	for _, row := range rows {
		rowByID[row.Issue.ID] = row
	}
	childrenByParent := make(map[string][]boardIssueRow)
	roots := make([]boardIssueRow, 0, len(rows))
	for _, row := range rows {
		parentID := row.Hierarchy.ParentID
		if parentID != "" {
			if _, ok := rowByID[parentID]; ok {
				childrenByParent[parentID] = append(childrenByParent[parentID], row)
				continue
			}
		}
		roots = append(roots, row)
	}

	visible := make([]boardIssueRow, 0, len(rows))
	var walk func(boardIssueRow)
	walk = func(row boardIssueRow) {
		visible = append(visible, row)
		if !row.Hierarchy.HasChildren || !expanded[row.Issue.ID] {
			return
		}
		for _, child := range childrenByParent[row.Issue.ID] {
			walk(child)
		}
	}
	for _, root := range roots {
		walk(root)
	}
	return visible
}

func boardFocusIssue(model boardTUIModel, issueID string) boardTUIModel {
	return boardFocusIssuePreferred(model, issueID, boardLanePreference(model.lane, model.navigationLanes()))
}

func boardFocusIssuePreferred(model boardTUIModel, issueID string, lanes []boardLane) boardTUIModel {
	for _, lane := range lanes {
		for _, row := range model.baseRowsForLane(lane) {
			if row.Issue.ID != issueID {
				continue
			}
			for _, ancestorID := range row.Hierarchy.AncestorIDs {
				model.expanded[ancestorID] = true
			}
			model.lane = lane
			rows := model.rowsForLane(lane)
			for idx, visible := range rows {
				if visible.Issue.ID == issueID {
					model.index = idx
					model.selectedIssue = issueID
					return boardClampSelection(model)
				}
			}
		}
	}
	return boardClampSelection(model)
}

func boardListHierarchyPrefix(model boardTUIModel, row boardIssueRow) string {
	if row.Hierarchy.Depth == 0 {
		return ""
	}

	rows := model.baseRowsForLane(model.lane)
	rowByID := make(map[string]boardIssueRow, len(rows))
	for _, candidate := range rows {
		rowByID[candidate.Issue.ID] = candidate
	}
	ancestorChain, parentInLane := boardLaneAncestorChain(rowByID, row)
	if !parentInLane {
		return "^ "
	}

	prefix := strings.Builder{}
	prefix.WriteString(strings.Repeat("   ", len(ancestorChain)))
	if row.Hierarchy.HasChildren {
		return prefix.String()
	}
	prefix.WriteString(boardHierarchyBranchForLane(rows, row))
	return prefix.String()
}

func boardLaneAncestorChain(rowByID map[string]boardIssueRow, row boardIssueRow) ([]string, bool) {
	if len(row.Hierarchy.AncestorIDs) == 0 {
		return nil, false
	}
	chain := make([]string, 0, len(row.Hierarchy.AncestorIDs))
	parentInLane := false
	for _, ancestorID := range row.Hierarchy.AncestorIDs {
		if _, ok := rowByID[ancestorID]; !ok {
			continue
		}
		chain = append(chain, ancestorID)
		if ancestorID == row.Hierarchy.ParentID {
			parentInLane = true
		}
	}
	return chain, parentInLane
}

func boardHierarchyBranchForLane(rows []boardIssueRow, row boardIssueRow) string {
	for _, candidate := range rows {
		if candidate.Issue.ID == row.Issue.ID {
			continue
		}
		if candidate.Hierarchy.ParentID != row.Hierarchy.ParentID {
			continue
		}
		if candidate.Hierarchy.ParentID == "" && candidate.Hierarchy.Depth != row.Hierarchy.Depth {
			continue
		}
		if boardRowAppearsLaterInLane(rows, row.Issue.ID, candidate.Issue.ID) {
			return "|- "
		}
	}
	if row.Hierarchy.SiblingCount > 0 && row.Hierarchy.SiblingIndex == row.Hierarchy.SiblingCount-1 {
		return "`- "
	}
	return "`- "
}

func boardRowAppearsLaterInLane(rows []boardIssueRow, currentID, candidateID string) bool {
	currentIndex := -1
	candidateIndex := -1
	for idx, row := range rows {
		switch row.Issue.ID {
		case currentID:
			currentIndex = idx
		case candidateID:
			candidateIndex = idx
		}
	}
	return currentIndex >= 0 && candidateIndex > currentIndex
}

func boardLaneTargetStatus(lane boardLane) string {
	switch lane {
	case boardLaneActive:
		return "InProgress"
	case boardLaneReady:
		return "Todo"
	default:
		return ""
	}
}

func boardRowMatchesLaneStatus(lane boardLane, row boardIssueRow) bool {
	targetStatus := boardLaneTargetStatus(lane)
	if targetStatus == "" {
		return true
	}
	return row.Issue.Status == targetStatus
}

func boardContextRowsForLane(snapshot boardSnapshot, lane boardLane) []boardIssueRow {
	targetStatus := boardLaneTargetStatus(lane)
	if targetStatus == "" {
		return nil
	}

	allRows := boardAllSnapshotRows(snapshot)
	if len(allRows) == 0 {
		return nil
	}

	rootRank := make(map[string]int)
	targetRankByID := make(map[string]int)
	for idx, row := range rawSnapshotRowsForStatus(snapshot, targetStatus) {
		targetRankByID[row.Issue.ID] = idx
		rootID := boardHierarchyRootID(row)
		if rank, ok := rootRank[rootID]; !ok || idx < rank {
			rootRank[rootID] = idx
		}
	}
	if len(rootRank) == 0 {
		return nil
	}

	included := make(map[string]struct{})
	for _, row := range allRows {
		if _, ok := rootRank[boardHierarchyRootID(row)]; ok {
			included[row.Issue.ID] = struct{}{}
		}
	}
	return boardOrderedContextRows(allRows, included, rootRank, targetStatus, targetRankByID)
}

func boardAllSnapshotRows(snapshot boardSnapshot) []boardIssueRow {
	rows := make([]boardIssueRow, 0, len(snapshot.Active)+len(snapshot.Blocked)+len(snapshot.Ready)+len(snapshot.Done)+len(snapshot.WontDo))
	rows = append(rows, snapshot.Active...)
	rows = append(rows, snapshot.Blocked...)
	rows = append(rows, snapshot.Ready...)
	rows = append(rows, snapshot.Done...)
	rows = append(rows, snapshot.WontDo...)
	return rows
}

func rawSnapshotRowsForStatus(snapshot boardSnapshot, status string) []boardIssueRow {
	switch status {
	case "InProgress":
		return snapshot.Active
	case "Blocked":
		return snapshot.Blocked
	case "Todo":
		return snapshot.Ready
	case "Done":
		return snapshot.Done
	case "WontDo":
		return snapshot.WontDo
	default:
		return nil
	}
}

func boardHierarchyRootID(row boardIssueRow) string {
	if len(row.Hierarchy.Path) > 0 {
		return row.Hierarchy.Path[0]
	}
	return row.Issue.ID
}

func boardOrderedContextRows(
	allRows []boardIssueRow,
	included map[string]struct{},
	rootRank map[string]int,
	targetStatus string,
	targetRankByID map[string]int,
) []boardIssueRow {
	rowByID := make(map[string]boardIssueRow, len(allRows))
	orderByID := make(map[string]int, len(allRows))
	for idx, row := range allRows {
		if _, ok := included[row.Issue.ID]; !ok {
			continue
		}
		rowByID[row.Issue.ID] = row
		orderByID[row.Issue.ID] = idx
	}

	childIDsByParent := make(map[string][]string)
	rootIDs := make([]string, 0)
	for issueID, row := range rowByID {
		parentID := strings.TrimSpace(row.Hierarchy.ParentID)
		if parentID != "" {
			if _, ok := included[parentID]; ok {
				childIDsByParent[parentID] = append(childIDsByParent[parentID], issueID)
				continue
			}
		}
		rootIDs = append(rootIDs, issueID)
	}

	sort.SliceStable(rootIDs, func(i, j int) bool {
		leftRoot := rootIDs[i]
		rightRoot := rootIDs[j]
		leftRank, leftHasRank := rootRank[leftRoot]
		rightRank, rightHasRank := rootRank[rightRoot]
		if leftHasRank != rightHasRank {
			return leftHasRank
		}
		if leftHasRank && rightHasRank && leftRank != rightRank {
			return leftRank < rightRank
		}
		leftRow := rowByID[leftRoot]
		rightRow := rowByID[rightRoot]
		if leftRow.Hierarchy.SiblingIndex != rightRow.Hierarchy.SiblingIndex {
			return leftRow.Hierarchy.SiblingIndex < rightRow.Hierarchy.SiblingIndex
		}
		return orderByID[leftRoot] < orderByID[rightRoot]
	})

	for parentID := range childIDsByParent {
		childIDs := childIDsByParent[parentID]
		sort.SliceStable(childIDs, func(i, j int) bool {
			leftRow := rowByID[childIDs[i]]
			rightRow := rowByID[childIDs[j]]
			leftMatches := leftRow.Issue.Status == targetStatus
			rightMatches := rightRow.Issue.Status == targetStatus
			if leftMatches != rightMatches {
				return leftMatches
			}
			leftTargetRank, leftRanked := targetRankByID[childIDs[i]]
			rightTargetRank, rightRanked := targetRankByID[childIDs[j]]
			if leftRanked != rightRanked {
				return leftRanked
			}
			if leftRanked && rightRanked && leftTargetRank != rightTargetRank {
				return leftTargetRank < rightTargetRank
			}
			leftOrder := orderByID[childIDs[i]]
			rightOrder := orderByID[childIDs[j]]
			if leftOrder != rightOrder {
				return leftOrder < rightOrder
			}
			return leftRow.Hierarchy.SiblingIndex < rightRow.Hierarchy.SiblingIndex
		})
		childIDsByParent[parentID] = childIDs
	}

	ordered := make([]boardIssueRow, 0, len(rowByID))
	var walk func(string)
	walk = func(issueID string) {
		row, ok := rowByID[issueID]
		if !ok {
			return
		}
		ordered = append(ordered, row)
		for _, childID := range childIDsByParent[issueID] {
			walk(childID)
		}
	}

	for _, rootID := range rootIDs {
		walk(rootID)
	}
	return ordered
}
