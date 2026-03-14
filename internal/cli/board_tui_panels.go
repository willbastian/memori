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
