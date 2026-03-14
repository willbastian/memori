package cli

import (
	"fmt"
	"strings"
)

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
