package cli

import (
	"fmt"
	"strings"
)

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

type boardDetailSection struct {
	label string
	lines []string
	muted bool
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
