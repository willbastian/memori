package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"memori/internal/store"
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
	boardActionUp
	boardActionDown
	boardActionPrevLane
	boardActionNextLane
	boardActionTop
	boardActionBottom
	boardActionToggleDetail
	boardActionToggleHelp
	boardActionQuit
)

type boardTUIModel struct {
	snapshot      boardSnapshot
	width         int
	height        int
	lane          boardLane
	index         int
	detailOpen    bool
	helpOpen      bool
	selectedIssue string
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

func runBoardTUI(ctx context.Context, s *store.Store, agent string, interval time.Duration, out io.Writer) error {
	restore, err := boardEnterRawMode()
	if err != nil {
		return err
	}
	defer restore()

	_, _ = io.WriteString(out, "\x1b[?1049h\x1b[?25l")
	defer func() {
		_, _ = io.WriteString(out, "\x1b[?25h\x1b[?1049l")
	}()

	width, height := boardTerminalSize(out)
	snapshot, err := buildBoardSnapshot(ctx, s, agent, time.Now().UTC())
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

	keyCh := make(chan boardAction, 8)
	errCh := make(chan error, 1)
	go readBoardActions(bufio.NewReader(boardInput()), keyCh, errCh)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			if err == io.EOF {
				return nil
			}
			return err
		case action := <-keyCh:
			model = boardReduce(model, action)
			if action == boardActionQuit {
				return nil
			}
			if err := renderFrame(); err != nil {
				return err
			}
		case <-ticker.C:
			width, height = boardTerminalSize(out)
			snapshot, err = buildBoardSnapshot(ctx, s, agent, time.Now().UTC())
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
	}
	return boardNormalizeModel(model)
}

func boardApplySnapshot(model boardTUIModel, snapshot boardSnapshot, width, height int) boardTUIModel {
	selectedIssue := model.selectedIssue
	model.snapshot = snapshot
	model.width = maxInt(width, 24)
	model.height = maxInt(height, 10)
	model = boardNormalizeModel(model)
	if selectedIssue == "" {
		return model
	}
	for _, lane := range model.availableLanes() {
		rows := model.rowsForLane(lane)
		for idx, row := range rows {
			if row.Issue.ID == selectedIssue {
				model.lane = lane
				model.index = idx
				return boardClampSelection(model)
			}
		}
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
	case boardActionQuit:
		return model
	}
	return boardNormalizeModel(model)
}

func boardNormalizeModel(model boardTUIModel) boardTUIModel {
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
		label := fmt.Sprintf(" %s %d ", strings.ToUpper(boardLaneTitle(lane)), len(model.rowsForLane(lane)))
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
	help := theme.paintLine(theme.mutedFG, "", false, " h/l lanes  j/k move  enter detail  ? help  q quit ")
	line := strings.Join(tabs, " ")
	if len(stripANSI(line))+len(stripANSI(help))+1 <= width {
		line += padRight("", width-len(stripANSI(line))-len(stripANSI(help))) + help
	}
	return padVisual(line, width)
}

func boardListPanel(model boardTUIModel, theme boardTheme, width, height int) []string {
	lines := make([]string, 0, height)
	title := fmt.Sprintf(" %s ", strings.ToUpper(boardLaneTitle(model.lane)))
	subtitle := fmt.Sprintf(" %d ", len(model.rows()))
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
		line := boardListRow(row, model.lane == boardLaneNext, width)
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
	chip := boardStatusCode(row.Issue.Status)
	issueID := boardDisplayIssueID(row.Issue.ID, width)
	switch {
	case width < 28:
		return truncateBoardLine(fmt.Sprintf(" %s %s", chip, row.Issue.Title), width)
	case width < 40:
		return truncateBoardLine(fmt.Sprintf(" %s %s %s", chip, issueID, row.Issue.Title), width)
	case showScore && row.Score > 0 && width >= 52:
		return truncateBoardLine(fmt.Sprintf(" %-3s %-8s %s · s%d", chip, issueID, row.Issue.Title, row.Score), width)
	default:
		return truncateBoardLine(fmt.Sprintf(" %-3s %-8s %s", chip, issueID, row.Issue.Title), width)
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

	descriptionLabel, description := boardWrappedSection("Description", row.Issue.Description, width)
	acceptanceLabel, acceptance := boardWrappedSection("Acceptance", row.Issue.Acceptance, width)
	reasonsLabel, reasons := boardWrappedSection("Reasons", strings.Join(orderBoardReasons(row.Reasons), "; "), width)
	referencesLabel, references := boardReferenceSection(row.Issue.References, width)

	if compact {
		appendSection(descriptionLabel, description, false)
		appendSection(acceptanceLabel, acceptance, false)
		appendSection(referencesLabel, references, true)
		appendSection(reasonsLabel, reasons, false)
		return sections
	}

	appendSection(reasonsLabel, reasons, false)
	appendSection(descriptionLabel, description, false)
	appendSection(acceptanceLabel, acceptance, false)
	appendSection(referencesLabel, references, true)
	return sections
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

func boardFooterLine(model boardTUIModel, theme boardTheme, width int) string {
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
	}
	return strings.Join(parts, " ")
}

func formatBoardTabsCompact(model boardTUIModel) string {
	parts := []string{
		fmt.Sprintf("N%d", len(model.snapshot.LikelyNext)),
		fmt.Sprintf("A%d", len(model.snapshot.Active)),
		fmt.Sprintf("B%d", len(model.snapshot.Blocked)),
		fmt.Sprintf("R%d", len(model.snapshot.Ready)),
	}
	line := strings.Join(parts, " ")
	return boardLaneTitle(model.lane) + " | " + line
}

func readBoardActions(reader *bufio.Reader, actions chan<- boardAction, errCh chan<- error) {
	for {
		action, err := readBoardAction(reader)
		if err != nil {
			errCh <- err
			return
		}
		if action == boardActionNone {
			continue
		}
		actions <- action
	}
}

func readBoardAction(reader *bufio.Reader) (boardAction, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return boardActionNone, err
	}
	switch b {
	case 'q':
		return boardActionQuit, nil
	case 'j':
		return boardActionDown, nil
	case 'k':
		return boardActionUp, nil
	case 'h':
		return boardActionPrevLane, nil
	case 'l':
		return boardActionNextLane, nil
	case 'g':
		return boardActionTop, nil
	case 'G':
		return boardActionBottom, nil
	case '?':
		return boardActionToggleHelp, nil
	case '\r', '\n', ' ':
		return boardActionToggleDetail, nil
	case 27:
		next, err := reader.ReadByte()
		if err != nil {
			return boardActionQuit, nil
		}
		if next != '[' {
			return boardActionNone, nil
		}
		arrow, err := reader.ReadByte()
		if err != nil {
			return boardActionNone, err
		}
		switch arrow {
		case 'A':
			return boardActionUp, nil
		case 'B':
			return boardActionDown, nil
		case 'C':
			return boardActionNextLane, nil
		case 'D':
			return boardActionPrevLane, nil
		default:
			return boardActionNone, nil
		}
	default:
		return boardActionNone, nil
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
