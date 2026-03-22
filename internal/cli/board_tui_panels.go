package cli

import (
	"fmt"
	"strings"
)

func boardListPanel(model boardTUIModel, theme boardTheme, width, height int) []string {
	lines := make([]string, 0, height)
	visibleCount := len(model.rows())
	totalCount := model.issueCountForLane(model.lane)
	subtitle := fmt.Sprintf("%d visible", totalCount)
	if visibleCount != totalCount {
		if boardLaneSupportsHierarchy(model.lane) {
			subtitle = fmt.Sprintf("%d rows / %d %s", visibleCount, totalCount, strings.ToLower(boardLaneTitle(model.lane)))
		} else {
			subtitle = fmt.Sprintf("%d/%d visible", visibleCount, totalCount)
		}
	}
	lines = append(lines, boardPanelHeader(theme, boardLaneTitle(model.lane), subtitle, width))

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
		line = boardApplySelectedLineToken(line, idx == model.index, theme.colors)
		line = truncateBoardRowLine(line, width)
		line = padRight(line, width)
		if idx == model.index {
			line = theme.paintLine(theme.selectedFG, theme.selectedBG, true, line)
		} else {
			fg, bg, bold, dim := boardLaneRowStyle(theme, model.lane, row)
			if bg == "" && idx%2 == 1 {
				bg = theme.panelAltBG
			}
			line = theme.paintLineStyled(fg, bg, bold, dim, line)
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
	compactLead := boardCompactHierarchyLead(model, row)
	if compactLead != "" {
		compactLead += " "
	}

	title := strings.TrimSpace(row.Issue.Title)
	if title == "" {
		title = row.Issue.ID
	}

	metaParts := make([]string, 0, 4)
	if boardLaneSupportsHierarchy(model.lane) && !boardRowMatchesLaneStatus(model.lane, row) {
		metaParts = append(metaParts, "["+strings.ToLower(boardExpandedStatusLabel(row.Issue.Status))+"]")
	}
	if !boardLaneSupportsHierarchy(model.lane) || width >= 48 {
		metaParts = append(metaParts, issueID)
	}
	if kind := strings.TrimSpace(row.Issue.Type); kind != "" && width >= 44 {
		metaParts = append(metaParts, strings.ToLower(kind))
	}
	if showScore && row.Score > 0 && width >= 64 {
		metaParts = append(metaParts, fmt.Sprintf("s%d", row.Score))
	}

	switch {
	case width < 28:
		return truncateBoardRowLine(fmt.Sprintf(" %s%s %s", compactLead, issueID, title), width)
	case width < 40:
		return truncateBoardRowLine(fmt.Sprintf(" %s%-8s %s", compactLead, issueID, title), width)
	default:
		body := title
		if boardLaneSupportsHierarchy(model.lane) {
			body = lead + title
		}
		if len(metaParts) > 0 {
			body += "  · " + strings.Join(metaParts, " · ")
		}
		return truncateBoardRowLine(" "+body, width)
	}
}

func boardExpandedStatusLabel(status string) string {
	switch status {
	case "InProgress":
		return "in progress"
	case "Blocked":
		return "blocked"
	case "Done":
		return "done"
	case "WontDo":
		return "won't do"
	default:
		return "todo"
	}
}

func boardLaneMembershipToken(lane boardLane, row boardIssueRow) string {
	switch lane {
	case boardLaneReady:
		if row.Issue.Status == "Todo" {
			return "R"
		}
		return "."
	case boardLaneActive:
		if row.Issue.Status == "InProgress" {
			return "A"
		}
		return "."
	default:
		return " "
	}
}

func boardRowForeground(theme boardTheme, row boardIssueRow) string {
	switch strings.ToLower(strings.TrimSpace(row.Issue.Type)) {
	case "epic":
		return theme.epicFG
	case "story":
		return theme.storyFG
	case "task":
		return theme.taskFG
	case "bug":
		return theme.bugFG
	default:
		return theme.detailFG
	}
}

func boardLaneRowForeground(theme boardTheme, lane boardLane, row boardIssueRow) string {
	if boardLaneSupportsHierarchy(lane) && !boardRowMatchesLaneStatus(lane, row) {
		switch row.Issue.Status {
		case "WontDo":
			return theme.wontDoFG
		}
	}
	return boardRowForeground(theme, row)
}

func boardLaneRowStyle(theme boardTheme, lane boardLane, row boardIssueRow) (fg, bg string, bold, dim bool) {
	fg = boardLaneRowForeground(theme, lane, row)
	bold = boardRowMatchesLaneStatus(lane, row)
	if boardLaneSupportsHierarchy(lane) && !boardRowMatchesLaneStatus(lane, row) {
		switch row.Issue.Status {
		case "Done":
			return fg, "", false, true
		case "WontDo":
			return fg, theme.wontDoBG, false, false
		}
	}
	return fg, "", bold, false
}

func boardHelpPanel(theme boardTheme, width, height int) []string {
	lines := []string{boardPanelHeader(theme, "Keyboard", "Quick reference", width)}
	for _, binding := range boardHelpBindings() {
		lines = append(lines, boardHelpLine(theme, binding.label, binding.description, width))
	}
	for len(lines) < height {
		lines = append(lines, padRight("", width))
	}
	return lines[:height]
}

func boardSearchPanel(model boardTUIModel, theme boardTheme, width, height int) []string {
	lines := make([]string, 0, height)
	results := boardSearchResults(model)
	subtitle := "Issue ids"
	if len(results) == 0 {
		subtitle += " · no matches"
	} else {
		subtitle += fmt.Sprintf(" · %d/%d", minInt(model.searchIndex+1, len(results)), len(results))
	}
	lines = append(lines, boardPanelHeader(theme, "Search", subtitle, width))
	prompt := "/"
	if model.searchQuery != "" {
		prompt += model.searchQuery
	}
	lines = append(lines, theme.paintLine(theme.detailFG, theme.panelAltBG, true, padRight(" "+prompt+" ", width)))
	hint := " short hash, full id, or mem- prefix; press f to include history "
	lines = append(lines, theme.paintLine(theme.mutedFG, "", false, padRight(truncateBoardLine(hint, width), width)))
	if len(results) == 0 {
		lines = append(lines, theme.paintLine(theme.mutedFG, "", false, padRight("  no issue id matches this query yet", width)))
		lines = append(lines, theme.paintLine(theme.mutedFG, "", false, padRight("  try a short hash, the mem- prefix, or toggle history with f", width)))
		for len(lines) < height {
			lines = append(lines, padRight("", width))
		}
		return lines[:height]
	}

	visible := maxInt(height-3, 1)
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
		issueID := boardSearchHighlightedID(result.row.Issue.ID, model.searchQuery, theme)
		line := truncateBoardLine(
			fmt.Sprintf(" %-7s %s  · %s", strings.ToUpper(boardLaneTitle(result.lane)), result.row.Issue.Title, issueID),
			width,
		)
		line = boardApplySelectedLineToken(line, idx == model.searchIndex, theme.colors)
		line = truncateBoardRowLine(line, width)
		line = padRight(line, width)
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
	keyText := theme.paintLine(theme.keyFG, "", true, " ["+padRight(key, 14)+"] ")
	descText := theme.paintLine(theme.helpFG, "", false, desc)
	return padVisual(keyText+descText, width)
}

func boardSearchHighlightedID(issueID, query string, theme boardTheme) string {
	issueID = boardDisplayIssueID(issueID, 80)
	query = strings.ToLower(strings.TrimSpace(query))
	if !theme.colors || query == "" {
		return issueID
	}
	lower := strings.ToLower(issueID)
	start := strings.Index(lower, query)
	if start < 0 {
		return issueID
	}
	end := minInt(start+len(query), len(issueID))
	return issueID[:start] + theme.paintLine(theme.accentFG, theme.titleMetaBG, true, issueID[start:end]) + issueID[end:]
}

func boardApplySelectedLineToken(line string, selected bool, colors bool) string {
	if !selected {
		return line
	}
	marker := ">"
	if colors {
		marker = "|"
	}
	if strings.HasPrefix(line, " ") {
		return marker + line
	}
	return marker + " " + line
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
		return theme.doneBG
	case "WontDo":
		return theme.wontDoBG
	default:
		return theme.nextBG
	}
}

func boardCompactHierarchyLead(model boardTUIModel, row boardIssueRow) string {
	if !boardLaneSupportsHierarchy(model.lane) {
		return ""
	}

	parts := make([]string, 0, 2)
	if row.Hierarchy.HasChildren {
		if toggle := strings.TrimSpace(boardHierarchyToggleToken(model.expanded[row.Issue.ID])); toggle != "" {
			parts = append(parts, toggle)
		}
	}
	if branch := strings.TrimSpace(boardListHierarchyPrefix(model, row)); branch != "" {
		parts = append(parts, branch)
	}
	return strings.Join(parts, " ")
}
