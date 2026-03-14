package cli

import (
	"bufio"
	"context"
	"fmt"
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

type boardTheme struct {
	colors      bool
	titleFG     string
	titleBG     string
	titleMetaBG string
	accentFG    string
	mutedFG     string
	borderFG    string
	selectedFG  string
	selectedBG  string
	panelBG     string
	panelAltBG  string
	helpBG      string
	helpFG      string
	detailFG    string
	activeFG    string
	activeBG    string
	blockedFG   string
	blockedBG   string
	readyFG     string
	readyBG     string
	nextFG      string
	nextBG      string
	metaFG      string
	keyFG       string
	chromeFG    string
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

func renderBoardTUI(model boardTUIModel, colors bool) string {
	theme := boardTheme{
		colors:      colors,
		titleFG:     "241;245;249",
		titleBG:     "15;23;42",
		titleMetaBG: "30;41;59",
		accentFG:    "103;232;249",
		mutedFG:     "148;163;184",
		borderFG:    "71;85;105",
		selectedFG:  "248;250;252",
		selectedBG:  "37;99;235",
		panelBG:     "15;23;42",
		panelAltBG:  "17;24;39",
		helpBG:      "30;41;59",
		helpFG:      "226;232;240",
		detailFG:    "226;232;240",
		activeFG:    "17;24;39",
		activeBG:    "250;204;21",
		blockedFG:   "255;241;242",
		blockedBG:   "225;29;72",
		readyFG:     "8;47;73",
		readyBG:     "45;212;191",
		nextFG:      "30;27;75",
		nextBG:      "196;181;253",
		metaFG:      "125;211;252",
		keyFG:       "251;191;36",
		chromeFG:    "30;41;59",
	}

	width := maxInt(model.width, 24)
	height := maxInt(model.height, 10)
	lines := make([]string, 0, height)
	lines = append(lines, boardHeaderLine(model, theme, width))
	lines = append(lines, boardTabsLine(model, theme, width))

	bodyHeight := maxInt(height-4, 5)
	if model.helpOpen {
		lines = append(lines, boardHelpPanel(theme, width, bodyHeight)...)
	} else if model.searchOpen {
		if width >= 100 {
			leftWidth := minInt(maxInt(width/2-2, 34), 44)
			rightWidth := width - leftWidth - 3
			left := boardListPanel(model, theme, leftWidth, bodyHeight)
			right := boardSearchPanel(model, theme, rightWidth, bodyHeight)
			lines = append(lines, boardJoinColumns(left, right, leftWidth, rightWidth)...)
		} else {
			listHeight := maxInt(bodyHeight/2, 6)
			searchHeight := maxInt(bodyHeight-listHeight-1, 6)
			lines = append(lines, boardListPanel(model, theme, width, listHeight)...)
			lines = append(lines, theme.paintLine(theme.borderFG, "", false, strings.Repeat("-", width)))
			lines = append(lines, boardSearchPanel(model, theme, width, searchHeight)...)
		}
	} else if width >= 100 {
		leftWidth := minInt(maxInt(width/2-2, 34), 44)
		rightWidth := width - leftWidth - 3
		left := boardListPanel(model, theme, leftWidth, bodyHeight)
		right := boardDetailPanel(model, theme, rightWidth, bodyHeight)
		lines = append(lines, boardJoinColumns(left, right, leftWidth, rightWidth)...)
	} else {
		listHeight := bodyHeight
		if model.detailOpen {
			detailHeight := maxInt((bodyHeight*2)/3, 10)
			maxDetailHeight := maxInt(bodyHeight-4, 1)
			detailHeight = minInt(detailHeight, maxDetailHeight)
			listHeight = maxInt(bodyHeight-detailHeight-1, 3)
		}
		lines = append(lines, boardListPanel(model, theme, width, listHeight)...)
		if model.detailOpen {
			lines = append(lines, theme.paintLine(theme.borderFG, "", false, strings.Repeat("-", width)))
			lines = append(lines, boardDetailPanel(model, theme, width, bodyHeight-listHeight-1)...)
		}
	}

	lines = append(lines, boardFooterLine(model, theme, width))
	return "\x1b[H" + strings.Join(lines, "\n") + "\x1b[J"
}

func boardHeaderLine(model boardTUIModel, theme boardTheme, width int) string {
	if width < 36 {
		return theme.paintLine(theme.titleFG, theme.titleBG, true, padRight(truncateBoardLine(" BOARD "+formatBoardSummaryCompact(model.snapshot.Summary), width), width))
	}
	title := " MEMORI BOARD "
	meta := fmt.Sprintf(" %s ", formatBoardSummary(model.snapshot.Summary, false))
	if model.snapshot.Agent != "" {
		meta += fmt.Sprintf(" AGENT %s ", strings.ToUpper(model.snapshot.Agent))
	}
	if len(meta) > width/2 {
		meta = truncateBoardLine(meta, width/2)
	}
	left := theme.paintLine(theme.titleFG, theme.titleBG, true, padRight(title, width))
	rightStart := maxInt(width-len(meta), len(title))
	return replaceSegment(left, rightStart, theme.paintLine(theme.accentFG, theme.titleMetaBG, true, meta))
}

func boardTabsLine(model boardTUIModel, theme boardTheme, width int) string {
	if width < 44 {
		line := formatBoardTabsCompact(model)
		return theme.paintLine(theme.mutedFG, theme.panelAltBG, false, padRight(truncateBoardLine(line, width), width))
	}
	tabs := make([]string, 0, 4)
	for _, lane := range []boardLane{boardLaneNext, boardLaneActive, boardLaneBlocked, boardLaneReady} {
		label := fmt.Sprintf(" %s %d ", strings.ToUpper(boardLaneTitle(lane)), model.issueCountForLane(lane))
		fg, bg := theme.mutedFG, theme.panelAltBG
		bold := false
		switch lane {
		case boardLaneNext:
			bg = theme.nextBG
			fg = theme.nextFG
		case boardLaneActive:
			bg = theme.activeBG
			fg = theme.activeFG
		case boardLaneBlocked:
			bg = theme.blockedBG
			fg = theme.blockedFG
		case boardLaneReady:
			bg = theme.readyBG
			fg = theme.readyFG
		}
		if lane == model.lane {
			bold = true
			label = ">" + label + "<"
		} else {
			label = " " + label + " "
		}
		tabs = append(tabs, theme.paintLine(fg, bg, bold, label))
	}
	help := theme.paintLine(theme.mutedFG, "", false, " h/l lanes  j/k move  [] tree  {} fold  enter detail  ? help  q quit ")
	line := strings.Join(tabs, " ")
	if len(stripANSI(line))+len(stripANSI(help))+1 <= width {
		line += padRight("", width-len(stripANSI(line))-len(stripANSI(help))) + help
	}
	return padVisual(line, width)
}

func boardListPanel(model boardTUIModel, theme boardTheme, width, height int) []string {
	lines := make([]string, 0, height)
	title := fmt.Sprintf(" %s ", strings.ToUpper(boardLaneTitle(model.lane)))
	visibleCount := len(model.rows())
	totalCount := model.issueCountForLane(model.lane)
	subtitle := fmt.Sprintf(" %d ", totalCount)
	if visibleCount != totalCount {
		subtitle = fmt.Sprintf(" %d/%d ", visibleCount, totalCount)
	}
	header := theme.paintLine(theme.accentFG, theme.panelBG, true, padRight(title, width))
	header = replaceSegment(header, maxInt(width-len(subtitle), len(title)), theme.paintLine(theme.mutedFG, theme.panelAltBG, false, subtitle))
	lines = append(lines, header)

	rows := model.rows()
	if len(rows) == 0 {
		lines = append(lines, theme.paintLine(theme.mutedFG, "", false, padRight("  no issues in this lane", width)))
		for len(lines) < height {
			lines = append(lines, padRight("", width))
		}
		return lines
	}

	visible := maxInt(height-1, 1)
	start := 0
	if model.index >= visible {
		start = model.index - visible + 1
	}
	if start > len(rows)-visible {
		start = maxInt(len(rows)-visible, 0)
	}
	end := minInt(start+visible, len(rows))
	for idx := start; idx < end; idx++ {
		row := rows[idx]
		line := boardRenderListRow(model, row, model.lane == boardLaneNext, width)
		if idx == model.index {
			line = theme.paintLine(theme.selectedFG, theme.selectedBG, true, line)
		} else {
			bg := ""
			if idx%2 == 1 {
				bg = theme.panelAltBG
			}
			line = theme.paintLine(theme.detailFG, bg, false, line)
		}
		lines = append(lines, line)
	}
	for len(lines) < height {
		lines = append(lines, padRight("", width))
	}
	return lines
}

func boardListRow(row boardIssueRow, showScore bool, width int) string {
	return boardRenderListRow(boardTUIModel{}, row, showScore, width)
}

func boardRenderListRow(model boardTUIModel, row boardIssueRow, showScore bool, width int) string {
	chip := boardStatusCode(row.Issue.Status)
	issueID := boardDisplayIssueID(row.Issue.ID, width)
	prefix := ""
	if boardLaneSupportsHierarchy(model.lane) {
		prefix = boardListHierarchyPrefix(model, row)
	}
	toggle := ""
	if boardLaneSupportsHierarchy(model.lane) && row.Hierarchy.HasChildren {
		toggle = " " + strings.TrimSpace(boardHierarchyToggleToken(model.expanded[row.Issue.ID]))
	}
	lead := prefix
	if trimmed := strings.TrimSpace(toggle); trimmed != "" {
		lead += trimmed + " "
	}
	switch {
	case width < 28:
		return truncateBoardLine(fmt.Sprintf(" %s %s%s%s", chip, prefix, row.Issue.Title, toggle), width)
	case width < 40:
		return truncateBoardLine(fmt.Sprintf(" %s %s%s", chip, lead, row.Issue.Title), width)
	case showScore && row.Score > 0 && width >= 52:
		return truncateBoardLine(fmt.Sprintf(" %-3s %s%-8s %s · s%d", chip, lead, issueID, row.Issue.Title, row.Score), width)
	default:
		return truncateBoardLine(fmt.Sprintf(" %-3s %s%-8s %s", chip, lead, issueID, row.Issue.Title), width)
	}
}

func boardDetailPanel(model boardTUIModel, theme boardTheme, width, height int) []string {
	lines := make([]string, 0, height)
	if !model.detailOpen {
		lines = append(lines, theme.paintLine(theme.accentFG, theme.panelBG, true, padRight(" ISSUE DETAIL ", width)))
		lines = append(lines, theme.paintLine(theme.mutedFG, theme.panelAltBG, false, padRight(" press <enter> to expand the selected issue ", width)))
		for len(lines) < height {
			lines = append(lines, padRight("", width))
		}
		return lines
	}

	row, ok := model.selectedRow()
	if !ok {
		lines = append(lines, theme.paintLine(theme.mutedFG, "", false, padRight(" no issue selected", width)))
		for len(lines) < height {
			lines = append(lines, padRight("", width))
		}
		return lines
	}

	lines = append(lines, theme.paintLine(theme.accentFG, theme.panelBG, true, padRight(" ISSUE DETAIL ", width)))
	lines = append(lines, theme.paintLine(theme.detailFG, theme.panelAltBG, true, padRight(" "+row.Issue.ID+" · "+row.Issue.Title+" ", width)))
	meta := []string{
		boardMetaToken(theme, row.Issue.Type, theme.metaFG, ""),
		boardMetaToken(theme, row.Issue.Status, boardStatusPalette(theme, row.Issue.Status), ""),
	}
	if row.Issue.Priority != "" {
		meta = append(meta, boardMetaToken(theme, row.Issue.Priority, theme.keyFG, ""))
	}
	if row.Issue.ParentID != "" {
		meta = append(meta, boardMetaToken(theme, "parent "+row.Issue.ParentID, theme.mutedFG, ""))
	}
	if row.Hierarchy.HasChildren {
		meta = append(meta, boardMetaToken(theme, fmt.Sprintf("%d child", row.Hierarchy.ChildCount), theme.accentFG, ""))
	}
	lines = append(lines, padVisual(strings.Join(meta, " "), width))
	lines = append(lines, theme.paintLine(theme.borderFG, "", false, strings.Repeat(".", width)))

	sections := boardDetailSections(row, width, width < 100)
	for _, section := range sections {
		lines = append(lines, boardDetailHeaderLine(theme, section.label, width, section.muted))
		for _, line := range section.lines {
			fg := theme.detailFG
			if section.muted {
				fg = theme.mutedFG
			}
			lines = append(lines, theme.paintLine(fg, "", false, line))
		}
	}
	for len(lines) < height {
		lines = append(lines, padRight("", width))
	}
	return lines[:minInt(len(lines), height)]
}

type boardDetailSection struct {
	label string
	lines []string
	muted bool
}

func boardDetailSections(row boardIssueRow, width int, compact bool) []boardDetailSection {
	sections := make([]boardDetailSection, 0, 4)
	appendSection := func(label string, lines []string, muted bool) {
		if len(lines) == 0 && label == "" {
			return
		}
		sections = append(sections, boardDetailSection{label: label, lines: lines, muted: muted})
	}

	hierarchyLabel, hierarchy := boardHierarchySection(row, width)
	descriptionLabel, description := boardWrappedSection("Description", row.Issue.Description, width)
	acceptanceLabel, acceptance := boardWrappedSection("Acceptance", row.Issue.Acceptance, width)
	reasonsLabel, reasons := boardWrappedSection("Reasons", strings.Join(orderBoardReasons(row.Reasons), "; "), width)
	referencesLabel, references := boardReferenceSection(row.Issue.References, width)

	if compact {
		appendSection(hierarchyLabel, hierarchy, false)
		appendSection(descriptionLabel, description, false)
		appendSection(acceptanceLabel, acceptance, false)
		appendSection(referencesLabel, references, true)
		appendSection(reasonsLabel, reasons, false)
		return sections
	}

	appendSection(hierarchyLabel, hierarchy, false)
	appendSection(reasonsLabel, reasons, false)
	appendSection(descriptionLabel, description, false)
	appendSection(acceptanceLabel, acceptance, false)
	appendSection(referencesLabel, references, true)
	return sections
}

func boardHierarchySection(row boardIssueRow, width int) (string, []string) {
	lines := make([]string, 0, 4)
	appendWrapped := func(label, value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		for idx, line := range wrapText(value, maxInt(width-2, 20)) {
			prefix := "  "
			if idx == 0 {
				prefix = "  " + label + ": "
			}
			lines = append(lines, truncateBoardLine(prefix+line, width))
		}
	}

	if len(row.Hierarchy.Path) > 1 {
		appendWrapped("path", strings.Join(row.Hierarchy.Path, " > "))
	}
	if row.Hierarchy.ParentID != "" {
		parent := row.Hierarchy.ParentID
		if row.Hierarchy.ParentTitle != "" {
			parent += " (" + row.Hierarchy.ParentTitle + ")"
		}
		appendWrapped("parent", parent)
	}
	if len(row.Hierarchy.ChildIDs) > 0 {
		appendWrapped("children", strings.Join(row.Hierarchy.ChildIDs, ", "))
	}
	if row.Hierarchy.Depth > 0 || row.Hierarchy.DescendantCount > 0 {
		appendWrapped("shape", fmt.Sprintf("depth %d, descendants %d", row.Hierarchy.Depth, row.Hierarchy.DescendantCount))
	}
	if len(lines) == 0 {
		return "", nil
	}
	return "Hierarchy", lines
}

func boardReferenceSection(refs []string, width int) (string, []string) {
	if len(refs) == 0 {
		return "", nil
	}
	lines := make([]string, 0, len(refs))
	for _, ref := range refs {
		lines = append(lines, truncateBoardLine("  "+ref, width))
	}
	return "References", lines
}

func boardWrappedSection(label, value string, width int) (string, []string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	lines := make([]string, 0, 4)
	for _, line := range wrapText(value, maxInt(width-2, 20)) {
		lines = append(lines, truncateBoardLine("  "+line, width))
	}
	return label, lines
}

func boardDetailHeaderLine(theme boardTheme, label string, width int, muted bool) string {
	label = strings.ToUpper(strings.TrimSpace(label))
	if label == "" {
		return padRight("", width)
	}
	fg, bg := boardDetailSectionPalette(theme, label, muted)
	chip := theme.paintLine(fg, bg, true, " [ "+label+" ] ")
	rule := theme.paintLine(theme.borderFG, "", false, strings.Repeat(".", maxInt(width-len(stripANSI(chip)), 0)))
	return padVisual(chip+rule, width)
}

func boardDetailSectionPalette(theme boardTheme, label string, muted bool) (string, string) {
	if muted {
		return theme.mutedFG, theme.panelAltBG
	}
	switch label {
	case "DESCRIPTION":
		return theme.accentFG, theme.titleMetaBG
	case "ACCEPTANCE":
		return theme.readyFG, theme.readyBG
	case "REASONS":
		return theme.keyFG, theme.panelAltBG
	default:
		return theme.metaFG, theme.titleMetaBG
	}
}

func boardHelpPanel(theme boardTheme, width, height int) []string {
	lines := []string{
		theme.paintLine(theme.helpFG, theme.helpBG, true, padRight(" KEYBOARD ", width)),
		boardHelpLine(theme, "j / k", "move selection", width),
		boardHelpLine(theme, "h / l", "switch lanes", width),
		boardHelpLine(theme, "[ / ]", "jump parent / child", width),
		boardHelpLine(theme, "{ / }", "collapse / expand subtree", width),
		boardHelpLine(theme, "g / G", "jump top / bottom", width),
		boardHelpLine(theme, "enter", "toggle issue detail", width),
		boardHelpLine(theme, "?", "toggle help", width),
		boardHelpLine(theme, "q", "quit", width),
	}
	for len(lines) < height {
		lines = append(lines, padRight("", width))
	}
	return lines[:height]
}

type boardSearchMatch struct {
	lane boardLane
	row  boardIssueRow
}

func boardSearchPanel(model boardTUIModel, theme boardTheme, width, height int) []string {
	lines := make([]string, 0, height)
	lines = append(lines, theme.paintLine(theme.accentFG, theme.panelBG, true, padRight(" SEARCH ", width)))
	prompt := "/"
	if model.searchQuery != "" {
		prompt += model.searchQuery
	}
	lines = append(lines, theme.paintLine(theme.detailFG, theme.panelAltBG, true, padRight(" "+prompt+" ", width)))

	results := boardSearchResults(model)
	if len(results) == 0 {
		lines = append(lines, theme.paintLine(theme.mutedFG, "", false, padRight("  no issue id matches this query", width)))
		for len(lines) < height {
			lines = append(lines, padRight("", width))
		}
		return lines[:height]
	}

	visible := maxInt(height-2, 1)
	start := 0
	if model.searchIndex >= visible {
		start = model.searchIndex - visible + 1
	}
	if start > len(results)-visible {
		start = maxInt(len(results)-visible, 0)
	}
	end := minInt(start+visible, len(results))
	for idx := start; idx < end; idx++ {
		result := results[idx]
		line := truncateBoardLine(
			fmt.Sprintf(" %-7s %-8s %s", strings.ToUpper(boardLaneTitle(result.lane)), boardDisplayIssueID(result.row.Issue.ID, width), result.row.Issue.Title),
			width,
		)
		if idx == model.searchIndex {
			line = theme.paintLine(theme.selectedFG, theme.selectedBG, true, line)
		} else {
			bg := ""
			if idx%2 == 1 {
				bg = theme.panelAltBG
			}
			line = theme.paintLine(theme.detailFG, bg, false, line)
		}
		lines = append(lines, line)
	}
	for len(lines) < height {
		lines = append(lines, padRight("", width))
	}
	return lines[:height]
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

func boardFooterLine(model boardTUIModel, theme boardTheme, width int) string {
	if model.searchOpen {
		footer := fmt.Sprintf(" Search /%s  |  enter jump  j/k results  backspace edit  esc cancel ", model.searchQuery)
		return theme.paintLine(theme.mutedFG, theme.panelAltBG, false, padRight(truncateBoardLine(footer, width), width))
	}
	row, ok := model.selectedRow()
	if !ok {
		return theme.paintLine(theme.mutedFG, "", false, padRight("No selectable issues", width))
	}
	if width < 40 {
		footer := fmt.Sprintf(" %s %s ", boardDisplayIssueID(row.Issue.ID, width), truncateBoardLine(row.Issue.Title, maxInt(width-12, 8)))
		return theme.paintLine(theme.mutedFG, theme.panelAltBG, false, padRight(truncateBoardLine(footer, width), width))
	}
	footer := fmt.Sprintf(" Selected %s  |  %s  |  %s ", row.Issue.ID, row.Issue.Status, truncateBoardLine(row.Issue.Title, maxInt(width/2, 20)))
	return theme.paintLine(theme.mutedFG, theme.panelAltBG, false, padRight(truncateBoardLine(footer, width), width))
}

func boardJoinColumns(left, right []string, leftWidth, rightWidth int) []string {
	height := maxInt(len(left), len(right))
	lines := make([]string, 0, height)
	for i := 0; i < height; i++ {
		l := padRight("", leftWidth)
		r := padRight("", rightWidth)
		if i < len(left) {
			l = left[i]
		}
		if i < len(right) {
			r = right[i]
		}
		lines = append(lines, padVisual(l, leftWidth)+" | "+padVisual(r, rightWidth))
	}
	return lines
}

func boardLaneTitle(lane boardLane) string {
	switch lane {
	case boardLaneNext:
		return "Next"
	case boardLaneActive:
		return "Active"
	case boardLaneBlocked:
		return "Blocked"
	case boardLaneReady:
		return "Ready"
	default:
		return "Lane"
	}
}

func boardStatusCode(status string) string {
	switch status {
	case "InProgress":
		return ">>"
	case "Blocked":
		return "!!"
	case "Done":
		return "OK"
	case "WontDo":
		return "NO"
	default:
		return ".."
	}
}

func boardDisplayIssueID(id string, width int) string {
	id = strings.TrimSpace(id)
	if width >= 48 || !strings.HasPrefix(id, "mem-") {
		return id
	}
	short := strings.TrimPrefix(id, "mem-")
	if width < 32 && len(short) > 6 {
		return short[:6]
	}
	return short
}

func boardHelpLine(theme boardTheme, key, desc string, width int) string {
	keyText := theme.paintLine(theme.keyFG, "", true, " "+padRight(key, 7)+" ")
	descText := theme.paintLine(theme.helpFG, "", false, desc)
	return padVisual(keyText+descText, width)
}

func boardMetaToken(theme boardTheme, value, fg, bg string) string {
	if bg == "" {
		bg = theme.panelAltBG
	}
	return theme.paintLine(fg, bg, true, " "+value+" ")
}

func boardStatusPalette(theme boardTheme, status string) string {
	switch status {
	case "InProgress":
		return theme.activeBG
	case "Blocked":
		return theme.blockedBG
	case "Done":
		return theme.readyBG
	case "WontDo":
		return theme.panelAltBG
	default:
		return theme.nextBG
	}
}

func formatBoardSummaryCompact(summary boardSummary) string {
	parts := []string{
		fmt.Sprintf("T%d", summary.Total),
		fmt.Sprintf("I%d", summary.InProgress),
		fmt.Sprintf("B%d", summary.Blocked),
		fmt.Sprintf("R%d", summary.Todo),
		fmt.Sprintf("W%d", summary.WontDo),
	}
	return strings.Join(parts, " ")
}

func formatBoardTabsCompact(model boardTUIModel) string {
	parts := []string{
		fmt.Sprintf("N%d", model.issueCountForLane(boardLaneNext)),
		fmt.Sprintf("A%d", model.issueCountForLane(boardLaneActive)),
		fmt.Sprintf("B%d", model.issueCountForLane(boardLaneBlocked)),
		fmt.Sprintf("R%d", model.issueCountForLane(boardLaneReady)),
	}
	line := strings.Join(parts, " ")
	return boardLaneTitle(model.lane) + " | " + line
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

func wrapText(value string, width int) []string {
	width = maxInt(width, 10)
	words := strings.Fields(value)
	if len(words) == 0 {
		return nil
	}
	lines := []string{words[0]}
	for _, word := range words[1:] {
		current := lines[len(lines)-1]
		if len(current)+1+len(word) <= width {
			lines[len(lines)-1] = current + " " + word
			continue
		}
		lines = append(lines, word)
	}
	return lines
}

func padRight(value string, width int) string {
	if width <= 0 {
		return ""
	}
	if len(value) >= width {
		return value[:width]
	}
	return value + strings.Repeat(" ", width-len(value))
}

func padVisual(value string, width int) string {
	raw := stripANSI(value)
	if len(raw) >= width {
		return value
	}
	return value + strings.Repeat(" ", width-len(raw))
}

func stripANSI(value string) string {
	var out strings.Builder
	inEscape := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if inEscape {
			if ch == 'm' {
				inEscape = false
			}
			continue
		}
		if ch == 0x1b {
			inEscape = true
			continue
		}
		out.WriteByte(ch)
	}
	return out.String()
}

func replaceSegment(line string, start int, segment string) string {
	raw := stripANSI(line)
	if start >= len(raw) {
		return line
	}
	prefix := raw[:start]
	suffixStart := start + len(stripANSI(segment))
	if suffixStart > len(raw) {
		suffixStart = len(raw)
	}
	return prefix + segment + raw[suffixStart:]
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

func boardHierarchyToggleToken(expanded bool) string {
	if expanded {
		return "[-] "
	}
	return "[+] "
}

func (theme boardTheme) paintLine(fg, bg string, bold bool, value string) string {
	if !theme.colors {
		return value
	}
	codes := make([]string, 0, 3)
	if bold {
		codes = append(codes, "1")
	}
	if fg != "" {
		codes = append(codes, "38;2;"+fg)
	}
	if bg != "" {
		codes = append(codes, "48;2;"+bg)
	}
	if len(codes) == 0 {
		return value
	}
	return "\x1b[" + strings.Join(codes, ";") + "m" + value + "\x1b[0m"
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
