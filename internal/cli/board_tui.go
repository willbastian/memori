package cli

import (
	"bufio"
	"context"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/willbastian/memori/internal/store"
)

type boardLane int

const (
	boardLaneNext boardLane = iota
	boardLaneActive
	boardLaneBlocked
	boardLaneReady
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
	boardActionToggleHelp
	boardActionParent
	boardActionChild
	boardActionCollapse
	boardActionExpand
	boardActionQuit
)

type boardKeyInput struct {
	action    boardAction
	text      string
	backspace bool
}

type boardTUIModel struct {
	snapshot      boardSnapshot
	width         int
	height        int
	lane          boardLane
	index         int
	detailOpen    bool
	helpOpen      bool
	selectedIssue string
	expanded      map[string]bool
	searchOpen    bool
	searchQuery   string
	searchIndex   int
	searchOrigin  string
	searchLane    boardLane
}

type boardTUITicker interface {
	channel() <-chan time.Time
	stop()
}

type boardTimeTicker struct {
	*time.Ticker
}

func (ticker boardTimeTicker) channel() <-chan time.Time {
	return ticker.C
}

func (ticker boardTimeTicker) stop() {
	ticker.Ticker.Stop()
}

var (
	boardTUIEnterRawMode  = boardEnterRawMode
	boardTUITerminalSize  = boardTerminalSize
	boardTUIBuildSnapshot = buildBoardSnapshot
	boardTUIReadInputs    = func(keyCh chan<- boardKeyInput, errCh chan<- error) {
		go readBoardInputs(bufio.NewReader(boardInput()), keyCh, errCh)
	}
	boardTUINow = func() time.Time {
		return time.Now().UTC()
	}
	boardTUINewTicker = func(interval time.Duration) boardTUITicker {
		return boardTimeTicker{Ticker: time.NewTicker(interval)}
	}
)

// TODO(mem-5ece68e): split terminal control, input wiring, and render-loop setup
// behind injectable adapters so the Darwin interactive loop can be covered
// without PTY-driven tests before board_tui.go is decomposed.
func runBoardTUI(ctx context.Context, s *store.Store, agent string, interval time.Duration, out io.Writer) error {
	restore, err := boardTUIEnterRawMode()
	if err != nil {
		return err
	}
	defer restore()

	_, _ = io.WriteString(out, "\x1b[?1049h\x1b[?25l")
	defer func() {
		_, _ = io.WriteString(out, "\x1b[?25h\x1b[?1049l")
	}()

	width, height := boardTUITerminalSize(out)
	snapshot, err := boardTUIBuildSnapshot(ctx, s, agent, boardTUINow())
	if err != nil {
		return err
	}
	model := newBoardTUIModel(snapshot, width, height)

	renderFrame := func() error {
		frame := renderBoardTUI(model, shouldUseColor(out))
		_, _ = io.WriteString(out, frame)
		return nil
	}
	if err := renderFrame(); err != nil {
		return err
	}

	keyCh := make(chan boardKeyInput, 8)
	errCh := make(chan error, 1)
	boardTUIReadInputs(keyCh, errCh)

	ticker := boardTUINewTicker(interval)
	defer ticker.stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			if err == io.EOF {
				return nil
			}
			return err
		case input := <-keyCh:
			quit := false
			model, quit = boardHandleInput(model, input)
			if quit {
				return nil
			}
			if err := renderFrame(); err != nil {
				return err
			}
		case <-ticker.channel():
			width, height = boardTUITerminalSize(out)
			snapshot, err = boardTUIBuildSnapshot(ctx, s, agent, boardTUINow())
			if err != nil {
				return err
			}
			model = boardApplySnapshot(model, snapshot, width, height)
			if err := renderFrame(); err != nil {
				return err
			}
		}
	}
}

func newBoardTUIModel(snapshot boardSnapshot, width, height int) boardTUIModel {
	model := boardTUIModel{
		snapshot:   snapshot,
		width:      maxInt(width, 24),
		height:     maxInt(height, 10),
		lane:       boardLaneNext,
		detailOpen: width >= 100,
		expanded:   make(map[string]bool),
	}
	return boardNormalizeModel(model)
}

func boardApplySnapshot(model boardTUIModel, snapshot boardSnapshot, width, height int) boardTUIModel {
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
	if selectedIssue == "" {
		return model
	}
	model = boardFocusIssue(model, selectedIssue)
	if model.selectedIssue == selectedIssue {
		return model
	}
	return model
}

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

func boardNormalizeModel(model boardTUIModel) boardTUIModel {
	model = boardSyncExpandedState(model)
	model = boardClampSearchSelection(model)
	lanes := model.availableLanes()
	if len(lanes) == 0 {
		model.lane = boardLaneNext
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
	for _, row := range model.snapshot.LikelyNext {
		if row.Hierarchy.HasChildren {
			valid[row.Issue.ID] = struct{}{}
			if _, ok := model.expanded[row.Issue.ID]; !ok {
				model.expanded[row.Issue.ID] = true
			}
		}
	}
	for _, row := range model.snapshot.Active {
		if row.Hierarchy.HasChildren {
			valid[row.Issue.ID] = struct{}{}
			if _, ok := model.expanded[row.Issue.ID]; !ok {
				model.expanded[row.Issue.ID] = true
			}
		}
	}
	for _, row := range model.snapshot.Blocked {
		if row.Hierarchy.HasChildren {
			valid[row.Issue.ID] = struct{}{}
			if _, ok := model.expanded[row.Issue.ID]; !ok {
				model.expanded[row.Issue.ID] = true
			}
		}
	}
	for _, row := range model.snapshot.Ready {
		if row.Hierarchy.HasChildren {
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
	lanes := make([]boardLane, 0, 4)
	for _, lane := range []boardLane{boardLaneNext, boardLaneActive, boardLaneBlocked, boardLaneReady} {
		if len(model.rowsForLane(lane)) > 0 {
			lanes = append(lanes, lane)
		}
	}
	if len(lanes) == 0 {
		return []boardLane{boardLaneNext, boardLaneActive, boardLaneBlocked, boardLaneReady}
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
	rows := model.rawRowsForLane(lane)
	if lane == boardLaneNext {
		return append([]boardIssueRow(nil), rows...)
	}
	return boardVisibleRows(rows, model.expanded)
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

type boardSearchMatch struct {
	lane boardLane
	row  boardIssueRow
}

func boardSearchResults(model boardTUIModel) []boardSearchMatch {
	query := strings.ToLower(strings.TrimSpace(model.searchQuery))
	preference := boardLanePreference(model.lane)
	laneRank := make(map[boardLane]int, len(preference))
	for idx, lane := range preference {
		laneRank[lane] = idx
	}
	seen := make(map[string]struct{})
	results := make([]boardSearchMatch, 0)
	for _, lane := range preference {
		for _, row := range model.rawRowsForLane(lane) {
			if _, ok := seen[row.Issue.ID]; ok {
				continue
			}
			if !boardSearchMatches(row.Issue.ID, query) {
				continue
			}
			seen[row.Issue.ID] = struct{}{}
			results = append(results, boardSearchMatch{lane: lane, row: row})
		}
	}
	sort.SliceStable(results, func(i, j int) bool {
		leftScore := boardSearchScore(results[i].row.Issue.ID, query)
		rightScore := boardSearchScore(results[j].row.Issue.ID, query)
		if leftScore != rightScore {
			return leftScore < rightScore
		}
		if results[i].lane != results[j].lane {
			return laneRank[results[i].lane] < laneRank[results[j].lane]
		}
		return results[i].row.Issue.ID < results[j].row.Issue.ID
	})
	return results
}

func boardSearchMatches(issueID, query string) bool {
	if query == "" {
		return true
	}
	id := strings.ToLower(strings.TrimSpace(issueID))
	shortID := strings.TrimPrefix(id, "mem-")
	return strings.HasPrefix(id, query) || strings.HasPrefix(shortID, query) || strings.Contains(id, query) || strings.Contains(shortID, query)
}

func boardSearchScore(issueID, query string) int {
	if query == "" {
		return 3
	}
	id := strings.ToLower(strings.TrimSpace(issueID))
	shortID := strings.TrimPrefix(id, "mem-")
	switch {
	case id == query || shortID == query:
		return 0
	case strings.HasPrefix(id, query) || strings.HasPrefix(shortID, query):
		return 1
	default:
		return 2
	}
}

func boardLanePreference(preferred boardLane) []boardLane {
	if preferred == boardLaneNext {
		return []boardLane{boardLaneReady, boardLaneActive, boardLaneBlocked, boardLaneNext}
	}
	order := []boardLane{preferred, boardLaneActive, boardLaneBlocked, boardLaneReady, boardLaneNext}
	seen := make(map[boardLane]struct{}, len(order))
	out := make([]boardLane, 0, len(order))
	for _, lane := range order {
		if _, ok := seen[lane]; ok {
			continue
		}
		seen[lane] = struct{}{}
		out = append(out, lane)
	}
	return out
}

func boardLaneSupportsHierarchy(lane boardLane) bool {
	return lane != boardLaneNext
}

func readBoardInputs(reader *bufio.Reader, actions chan<- boardKeyInput, errCh chan<- error) {
	for {
		input, err := readBoardInput(reader)
		if err != nil {
			errCh <- err
			return
		}
		if input.action == boardActionNone && input.text == "" && !input.backspace {
			continue
		}
		actions <- input
	}
}

func readBoardInput(reader *bufio.Reader) (boardKeyInput, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return boardKeyInput{}, err
	}
	switch b {
	case '/':
		return boardKeyInput{action: boardActionSearchOpen}, nil
	case 'q':
		return boardKeyInput{action: boardActionQuit}, nil
	case 'j':
		return boardKeyInput{action: boardActionDown}, nil
	case 'k':
		return boardKeyInput{action: boardActionUp}, nil
	case 'h':
		return boardKeyInput{action: boardActionPrevLane}, nil
	case 'l':
		return boardKeyInput{action: boardActionNextLane}, nil
	case 'g':
		return boardKeyInput{action: boardActionTop}, nil
	case 'G':
		return boardKeyInput{action: boardActionBottom}, nil
	case '?':
		return boardKeyInput{action: boardActionToggleHelp}, nil
	case '[':
		return boardKeyInput{action: boardActionParent}, nil
	case ']':
		return boardKeyInput{action: boardActionChild}, nil
	case '{':
		return boardKeyInput{action: boardActionCollapse}, nil
	case '}':
		return boardKeyInput{action: boardActionExpand}, nil
	case 8, 127:
		return boardKeyInput{backspace: true}, nil
	case '\r', '\n', ' ':
		return boardKeyInput{action: boardActionToggleDetail}, nil
	case 27:
		if reader.Buffered() == 0 {
			return boardKeyInput{action: boardActionQuit}, nil
		}
		next, err := reader.ReadByte()
		if err != nil {
			return boardKeyInput{action: boardActionQuit}, nil
		}
		if next != '[' {
			return boardKeyInput{action: boardActionQuit}, nil
		}
		arrow, err := reader.ReadByte()
		if err != nil {
			return boardKeyInput{}, err
		}
		switch arrow {
		case 'A':
			return boardKeyInput{action: boardActionUp}, nil
		case 'B':
			return boardKeyInput{action: boardActionDown}, nil
		case 'C':
			return boardKeyInput{action: boardActionNextLane}, nil
		case 'D':
			return boardKeyInput{action: boardActionPrevLane}, nil
		default:
			return boardKeyInput{}, nil
		}
	default:
		if b >= 32 && b <= 126 {
			return boardKeyInput{text: string(b)}, nil
		}
		return boardKeyInput{}, nil
	}
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
	return boardFocusIssuePreferred(model, issueID, boardLanePreference(model.lane))
}

func boardFocusIssuePreferred(model boardTUIModel, issueID string, lanes []boardLane) boardTUIModel {
	for _, lane := range lanes {
		for _, row := range model.rawRowsForLane(lane) {
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

	rows := model.rawRowsForLane(model.lane)
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

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func init() {
	sort.Ints([]int{})
}
