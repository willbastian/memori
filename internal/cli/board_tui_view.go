package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type boardTheme struct {
	colors      bool
	name        string
	titleFG     string
	titleBG     string
	titleAltBG  string
	titleMetaBG string
	accentFG    string
	accentBG    string
	mutedFG     string
	borderFG    string
	selectedFG  string
	selectedBG  string
	panelBG     string
	panelAltBG  string
	panelHeadFG string
	panelHeadBG string
	panelMetaFG string
	panelMetaBG string
	helpBG      string
	helpFG      string
	detailFG    string
	epicFG      string
	storyFG     string
	taskFG      string
	bugFG       string
	activeFG    string
	activeBG    string
	blockedFG   string
	blockedBG   string
	readyFG     string
	readyBG     string
	doneFG      string
	doneBG      string
	wontDoFG    string
	wontDoBG    string
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

func defaultBoardTheme(colors bool) boardTheme {
	return boardTheme{
		colors:      colors,
		name:        "signal deck",
		titleFG:     "248;250;252",
		titleBG:     "12;18;32",
		titleAltBG:  "18;27;46",
		titleMetaBG: "31;41;72",
		accentFG:    "103;232;249",
		accentBG:    "10;36;58",
		mutedFG:     "148;163;184",
		borderFG:    "71;85;105",
		selectedFG:  "248;250;252",
		selectedBG:  "37;99;235",
		panelBG:     "12;18;32",
		panelAltBG:  "17;24;39",
		panelHeadFG: "248;250;252",
		panelHeadBG: "23;37;84",
		panelMetaFG: "186;230;253",
		panelMetaBG: "14;52;84",
		helpBG:      "30;41;59",
		helpFG:      "226;232;240",
		detailFG:    "226;232;240",
		epicFG:      "251;191;36",
		storyFG:     "125;211;252",
		taskFG:      "134;239;172",
		bugFG:       "253;164;175",
		activeFG:    "17;24;39",
		activeBG:    "250;204;21",
		blockedFG:   "255;241;242",
		blockedBG:   "225;29;72",
		readyFG:     "8;47;73",
		readyBG:     "45;212;191",
		doneFG:      "6;78;59",
		doneBG:      "134;239;172",
		wontDoFG:    "249;250;251",
		wontDoBG:    "71;85;105",
		nextFG:      "30;27;75",
		nextBG:      "196;181;253",
		metaFG:      "125;211;252",
		keyFG:       "251;191;36",
		chromeFG:    "30;41;59",
	}
}

func renderBoardTUI(model boardTUIModel, colors bool) string {
	theme := defaultBoardTheme(colors)

	width := maxInt(model.width, 24)
	height := maxInt(model.height, 10)
	lines := make([]string, 0, height)
	lines = append(lines, boardHeaderLine(model, theme, width))
	lines = append(lines, boardTabsLine(model, theme, width))

	bodyHeight := maxInt(height-4, 5)
	if model.toast.message != "" {
		bodyHeight = maxInt(bodyHeight-1, 4)
	}
	if model.helpOpen {
		if width >= 100 {
			panelWidth := minInt(maxInt(width-18, 52), 76)
			panelHeight := minInt(maxInt(bodyHeight-4, 10), 16)
			help := boardHelpPanel(model, theme, maxInt(panelWidth-2, 1), maxInt(panelHeight-2, 1))
			lines = append(lines, boardOverlayPanel(theme, help, width, bodyHeight, panelWidth, panelHeight)...)
		} else {
			help := boardHelpPanel(model, theme, maxInt(width-2, 1), maxInt(bodyHeight-2, 1))
			lines = append(lines, boardFramePanel(theme, help, width, bodyHeight)...)
		}
	} else if model.searchOpen {
		if width >= 100 {
			panelWidth := minInt(maxInt(width-16, 56), 82)
			panelHeight := minInt(maxInt(bodyHeight-3, 10), 15)
			search := boardSearchPanel(model, theme, maxInt(panelWidth-2, 1), maxInt(panelHeight-2, 1))
			lines = append(lines, boardOverlayPanel(theme, search, width, bodyHeight, panelWidth, panelHeight)...)
		} else {
			listHeight := maxInt(bodyHeight/2, 6)
			searchHeight := maxInt(bodyHeight-listHeight-1, 6)
			lines = append(lines, boardFramePanel(theme, boardListPanel(model, theme, maxInt(width-2, 1), maxInt(listHeight-2, 1)), width, listHeight)...)
			lines = append(lines, theme.rule(width))
			lines = append(lines, boardFramePanel(theme, boardSearchPanel(model, theme, maxInt(width-2, 1), maxInt(searchHeight-2, 1)), width, searchHeight)...)
		}
	} else if width >= 100 && model.detailOpen {
		rightWidth := minInt(maxInt(width/3+6, 38), 52)
		leftWidth := width - rightWidth - 1
		left := boardFramePanel(theme, boardListPanel(model, theme, maxInt(leftWidth-2, 1), maxInt(bodyHeight-2, 1)), leftWidth, bodyHeight)
		right := boardFramePanel(theme, boardSidePanel(model, theme, maxInt(rightWidth-2, 1), maxInt(bodyHeight-2, 1)), rightWidth, bodyHeight)
		lines = append(lines, boardJoinColumns(left, right, leftWidth, rightWidth)...)
	} else {
		if model.detailOpen {
			lines = append(lines, boardFramePanel(theme, boardSidePanel(model, theme, maxInt(width-2, 1), maxInt(bodyHeight-2, 1)), width, bodyHeight)...)
			lines = append(lines, boardFooterLine(model, theme, width))
			return lipgloss.JoinVertical(lipgloss.Left, lines...)
		}

		listHeight := bodyHeight
		lines = append(lines, boardFramePanel(theme, boardListPanel(model, theme, maxInt(width-2, 1), maxInt(listHeight-2, 1)), width, listHeight)...)
	}

	if model.toast.message != "" {
		lines = append(lines, boardToastLine(model, theme, width))
	}
	lines = append(lines, boardFooterLine(model, theme, width))
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

func boardHeaderLine(model boardTUIModel, theme boardTheme, width int) string {
	if width < 36 {
		return theme.lineStyle(theme.titleFG, theme.titleBG, true, false).Width(width).MaxWidth(width).Render(truncateBoardLine(" MEMORI BOARD ", width))
	}
	title := " MEMORI BOARD "
	left := theme.lineStyle(theme.titleFG, theme.titleBG, true, false).Render(title)
	meta := boardHeaderMeta(model, theme, width)
	if strings.TrimSpace(meta) == "" {
		return theme.lineStyle(theme.titleFG, theme.titleBG, true, false).Width(width).MaxWidth(width).Render(title)
	}
	spacer := lipgloss.NewStyle().Width(maxInt(width-visualWidth(left)-visualWidth(meta), 0)).Render("")
	line := lipgloss.JoinHorizontal(lipgloss.Top, left, spacer, meta)
	return theme.lineStyle(theme.titleFG, theme.titleBG, true, false).Width(width).MaxWidth(width).Render(line)
}

func boardTabsLine(model boardTUIModel, theme boardTheme, width int) string {
	if width < 56 {
		line := " lanes / " + formatBoardTabsCompact(model, width)
		return theme.lineStyle(theme.mutedFG, theme.panelAltBG, false, false).Width(width).MaxWidth(width).Render(truncateBoardLine(line, width))
	}
	tabs := make([]string, 0, len(model.availableLanes()))
	for _, lane := range model.availableLanes() {
		label := fmt.Sprintf(" %s %d ", strings.ToUpper(boardLaneTitle(lane)), model.issueCountForLane(lane))
		fg, bg := theme.mutedFG, theme.panelAltBG
		bold := false
		if lane == model.lane {
			bold = true
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
			case boardLaneDone:
				bg = theme.doneBG
				fg = theme.doneFG
			case boardLaneWontDo:
				bg = theme.wontDoBG
				fg = theme.wontDoFG
			}
		}
		tabs = append(tabs, theme.paintLine(fg, bg, bold, label))
	}
	return theme.lineStyle(theme.mutedFG, theme.panelBG, false, false).Width(width).MaxWidth(width).Render(truncateBoardLine(strings.Join(tabs, " "), width))
}

func boardFooterLine(model boardTUIModel, theme boardTheme, width int) string {
	if model.searchOpen {
		scope := "actionable"
		if model.showHistory {
			scope = "all"
		}
		results := boardSearchResults(model)
		resultLabel := "no matches"
		if len(results) > 0 {
			resultLabel = fmt.Sprintf("%d/%d match", minInt(model.searchIndex+1, len(results)), len(results))
			if len(results) > 1 {
				resultLabel += "es"
			}
		}
		footer := fmt.Sprintf(" /%s  · %s  [enter jump] [j/k results] [backspace edit] [f scope:%s] [esc cancel] ", model.searchQuery, resultLabel, scope)
		return theme.paintLine(theme.mutedFG, theme.panelAltBG, false, padRight(truncateBoardLine(footer, width), width))
	}
	row, ok := model.selectedRow()
	if !ok {
		return theme.paintLine(theme.mutedFG, "", false, padRight("No selectable issues", width))
	}
	if width < 40 {
		footer := fmt.Sprintf(" %s %s %s ", boardDisplayIssueID(row.Issue.ID, width), boardCompactStatusLabel(row.Issue.Status), row.Issue.Title)
		return theme.paintLine(theme.mutedFG, theme.panelAltBG, false, padRight(truncateBoardLine(footer, width), width))
	}
	historyHint := "f history"
	if model.showHistory {
		historyHint = "f actionable"
	}
	panelHint := "enter detail  c continuity"
	if model.detailOpen {
		if model.panelMode == boardPanelModeContinuity {
			panelHint = "enter close  c detail"
		} else {
			panelHint = "enter close  c continuity"
		}
	}
	footer := fmt.Sprintf(" %s  · %s · %s  ·  %s  %s  ? help ", row.Issue.ID, strings.ToLower(row.Issue.Type), truncateBoardLine(row.Issue.Title, maxInt(width/3, 18)), panelHint, historyHint)
	return theme.paintLine(theme.mutedFG, theme.panelAltBG, false, padRight(truncateBoardLine(footer, width), width))
}

func boardToastLine(model boardTUIModel, theme boardTheme, width int) string {
	if strings.TrimSpace(model.toast.message) == "" {
		return padRight("", width)
	}
	fg := theme.helpFG
	bg := theme.panelAltBG
	switch model.toast.tone {
	case boardToastToneSuccess:
		fg = theme.doneFG
		bg = theme.doneBG
	case boardToastToneWarn:
		fg = theme.blockedFG
		bg = theme.blockedBG
	}
	return theme.paintLine(fg, bg, true, padRight(truncateBoardLine(" "+model.toast.message+" ", width), width))
}

func boardPanelModeTitle(mode boardPanelMode) string {
	switch mode {
	case boardPanelModeContinuity:
		return "continuity"
	default:
		return "detail"
	}
}

func boardHeaderMeta(model boardTUIModel, theme boardTheme, width int) string {
	if model.snapshot.Agent != "" {
		agent := " AGENT " + strings.ToUpper(model.snapshot.Agent) + " "
		return theme.lineStyle(theme.keyFG, theme.titleAltBG, true, false).Render(truncateBoardLine(agent, maxInt(width/3, 14)))
	}
	return ""
}

func boardPanelHeader(theme boardTheme, label, subtitle string, width int) string {
	label = strings.ToUpper(strings.TrimSpace(label))
	if label == "" {
		return padRight("", width)
	}
	head := theme.lineStyle(theme.panelHeadFG, theme.panelHeadBG, true, false).Render(" " + label + " ")
	parts := []string{head}
	if subtitle != "" {
		meta := theme.lineStyle(theme.panelMetaFG, theme.panelMetaBG, false, false).Render(" " + strings.TrimSpace(subtitle) + " ")
		spacer := lipgloss.NewStyle().Width(maxInt(width-visualWidth(head)-visualWidth(meta), 0)).Render("")
		parts = append(parts, spacer, meta)
	} else {
		parts = append(parts, lipgloss.NewStyle().Width(maxInt(width-visualWidth(head), 0)).Render(""))
	}
	line := lipgloss.JoinHorizontal(lipgloss.Top, parts...)
	return theme.lineStyle(theme.detailFG, theme.panelBG, false, false).Width(width).MaxWidth(width).Render(line)
}

func boardFramePanel(theme boardTheme, lines []string, width, height int) []string {
	if width <= 2 || height <= 2 {
		return lines
	}
	innerWidth := width - 2
	innerHeight := height - 2
	body := make([]string, 0, innerHeight)
	for _, line := range lines {
		if len(body) >= innerHeight {
			break
		}
		body = append(body, padRight(line, innerWidth))
	}
	for len(body) < innerHeight {
		body = append(body, padRight("", innerWidth))
	}

	top := theme.borderFrameLine("╭" + strings.Repeat("─", innerWidth) + "╮")
	bottom := theme.borderFrameLine("╰" + strings.Repeat("─", innerWidth) + "╯")
	framed := make([]string, 0, height)
	framed = append(framed, top)
	for _, line := range body {
		content := line
		if theme.colors && theme.panelBG != "" {
			content = lipgloss.NewStyle().
				Background(lipgloss.Color(boardLipGlossColor(theme.panelBG))).
				Render(padRight(line, innerWidth))
		} else {
			content = padRight(line, innerWidth)
		}
		framed = append(framed, lipgloss.JoinHorizontal(
			lipgloss.Top,
			theme.borderFrameToken("│"),
			content,
			theme.borderFrameToken("│"),
		))
	}
	framed = append(framed, bottom)
	return framed
}

func boardOverlayPanel(theme boardTheme, lines []string, outerWidth, outerHeight, panelWidth, panelHeight int) []string {
	if outerWidth <= 0 || outerHeight <= 0 {
		return nil
	}
	panelWidth = minInt(maxInt(panelWidth, 12), outerWidth)
	panelHeight = minInt(maxInt(panelHeight, 4), outerHeight)
	panel := boardFramePanel(theme, lines, panelWidth, panelHeight)
	leftPad := maxInt((outerWidth-panelWidth)/2, 0)
	rightPad := maxInt(outerWidth-panelWidth-leftPad, 0)
	topPad := maxInt((outerHeight-panelHeight)/2, 0)

	out := make([]string, 0, outerHeight)
	blank := strings.Repeat(" ", outerWidth)
	for i := 0; i < topPad; i++ {
		out = append(out, blank)
	}
	for _, line := range panel {
		framed := strings.Repeat(" ", leftPad) + line + strings.Repeat(" ", rightPad)
		out = append(out, padRight(framed, outerWidth))
	}
	for len(out) < outerHeight {
		out = append(out, blank)
	}
	return out[:outerHeight]
}

func boardJoinColumns(left, right []string, leftWidth, rightWidth int) []string {
	leftBlock := lipgloss.JoinVertical(lipgloss.Left, left...)
	rightBlock := lipgloss.JoinVertical(lipgloss.Left, right...)
	joined := lipgloss.JoinHorizontal(
		lipgloss.Top,
		lipgloss.NewStyle().Width(leftWidth).MaxWidth(leftWidth).Render(leftBlock),
		" ",
		lipgloss.NewStyle().Width(rightWidth).MaxWidth(rightWidth).Render(rightBlock),
	)
	return strings.Split(joined, "\n")
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
	case boardLaneDone:
		return "Done"
	case boardLaneWontDo:
		return "WontDo"
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

func formatBoardTabsCompact(model boardTUIModel, width int) string {
	parts := []string{
		fmt.Sprintf("N%d", model.issueCountForLane(boardLaneNext)),
		fmt.Sprintf("A%d", model.issueCountForLane(boardLaneActive)),
		fmt.Sprintf("B%d", model.issueCountForLane(boardLaneBlocked)),
		fmt.Sprintf("R%d", model.issueCountForLane(boardLaneReady)),
	}
	if model.showHistory {
		parts = append(parts,
			fmt.Sprintf("D%d", model.issueCountForLane(boardLaneDone)),
			fmt.Sprintf("W%d", model.issueCountForLane(boardLaneWontDo)),
		)
	}
	line := strings.Join(parts, " ")
	scope := "act"
	if model.showHistory {
		scope = "all"
	}
	if width < 32 {
		return fmt.Sprintf("%s %d | %s", strings.ToUpper(boardLaneTitle(model.lane)), model.issueCountForLane(model.lane), strings.Join(parts, " "))
	}
	return boardLaneTitle(model.lane) + " | " + scope + " | " + line
}

func boardCompactStatusLabel(status string) string {
	switch status {
	case "InProgress":
		return "IP"
	case "Blocked":
		return "BLK"
	case "Done":
		return "DONE"
	case "WontDo":
		return "NO"
	default:
		return "TODO"
	}
}

func boardHierarchyToggleToken(expanded bool) string {
	if expanded {
		return "[-] "
	}
	return "[+] "
}

func (theme boardTheme) paintLine(fg, bg string, bold bool, value string) string {
	return theme.paintLineStyled(fg, bg, bold, false, value)
}

func (theme boardTheme) paintLineStyled(fg, bg string, bold, dim bool, value string) string {
	return theme.lineStyle(fg, bg, bold, dim).Render(value)
}

func (theme boardTheme) lineStyle(fg, bg string, bold, dim bool) lipgloss.Style {
	style := lipgloss.NewStyle()
	if theme.colors && fg != "" {
		style = style.Foreground(lipgloss.Color(boardLipGlossColor(fg)))
	}
	if theme.colors && bg != "" {
		style = style.Background(lipgloss.Color(boardLipGlossColor(bg)))
	}
	if bold {
		style = style.Bold(true)
	}
	if dim {
		style = style.Faint(true)
	}
	return style
}

func (theme boardTheme) rule(width int) string {
	style := lipgloss.NewStyle().Width(width).MaxWidth(width)
	if theme.colors && theme.borderFG != "" {
		style = style.Foreground(lipgloss.Color(boardLipGlossColor(theme.borderFG)))
	}
	return style.Render(strings.Repeat("·", maxInt(width, 0)))
}

func (theme boardTheme) borderFrameLine(value string) string {
	style := lipgloss.NewStyle()
	if theme.colors && theme.borderFG != "" {
		style = style.Foreground(lipgloss.Color(boardLipGlossColor(theme.borderFG)))
	}
	if theme.colors && theme.panelBG != "" {
		style = style.Background(lipgloss.Color(boardLipGlossColor(theme.panelBG)))
	}
	return style.Render(value)
}

func (theme boardTheme) borderFrameToken(value string) string {
	return theme.borderFrameLine(value)
}

func boardLipGlossColor(value string) string {
	parts := strings.Split(strings.TrimSpace(value), ";")
	if len(parts) != 3 {
		return value
	}
	component := func(raw string) string {
		n, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || n < 0 {
			return "00"
		}
		if n > 255 {
			n = 255
		}
		return fmt.Sprintf("%02x", n)
	}
	return "#" + component(parts[0]) + component(parts[1]) + component(parts[2])
}
