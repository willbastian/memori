package cli

import (
	"fmt"
	"strings"
)

func boardDetailPanel(model boardTUIModel, theme boardTheme, width, height int) []string {
	lines := make([]string, 0, height)
	if !model.detailOpen {
		lines = append(lines, boardPanelHeader(theme, "Issue Detail", "Context", width))
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

	lines = append(lines, boardPanelHeader(theme, "Issue Detail", "Context", width))
	for _, line := range boardDetailIntroLines(row, theme, width) {
		lines = append(lines, line)
	}
	lines = append(lines, theme.paintLine(theme.borderFG, "", false, strings.Repeat(".", width)))

	sections := boardDetailSections(row, width, width < 100)
	for _, section := range sections {
		lines = append(lines, boardDetailHeaderLine(theme, section.label, width, section.muted))
		for _, line := range section.lines {
			fg := theme.detailFG
			if section.muted {
				fg = theme.mutedFG
			}
			lines = append(lines, theme.paintLine(fg, "", false, padRight(line, width)))
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
	compact := width < 56
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
		appendWrapped("path", boardCompactHierarchyPath(row.Hierarchy.Path, width, compact))
	}
	if row.Hierarchy.ParentID != "" {
		parent := row.Hierarchy.ParentID
		if row.Hierarchy.ParentTitle != "" && !compact {
			parent += " (" + row.Hierarchy.ParentTitle + ")"
		}
		appendWrapped("parent", parent)
	}
	if len(row.Hierarchy.ChildIDs) > 0 {
		if compact {
			appendWrapped("children", fmt.Sprintf("%d", row.Hierarchy.ChildCount))
		} else {
			appendWrapped("children", strings.Join(row.Hierarchy.ChildIDs, ", "))
		}
	}
	if row.Hierarchy.Depth > 0 || row.Hierarchy.DescendantCount > 0 {
		if compact {
			appendWrapped("shape", fmt.Sprintf("d%d desc%d", row.Hierarchy.Depth, row.Hierarchy.DescendantCount))
		} else {
			appendWrapped("shape", fmt.Sprintf("depth %d, descendants %d", row.Hierarchy.Depth, row.Hierarchy.DescendantCount))
		}
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
	rule := theme.paintLine(theme.borderFG, "", false, strings.Repeat(".", maxInt(width-visualWidth(chip), 0)))
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

type boardMetaPart struct {
	label string
	fg    string
	bg    string
}

func boardDetailIntroLines(row boardIssueRow, theme boardTheme, width int) []string {
	lines := []string{
		theme.paintLine(theme.detailFG, theme.panelAltBG, true, padRight(truncateBoardLine(" "+boardDetailHeadline(row, width)+" ", width), width)),
	}
	maxLines := 1
	if width < 56 {
		maxLines = 2
	}
	return append(lines, boardRenderMetaLines(theme, boardDetailMetaParts(row, theme, width), width, maxLines)...)
}

func boardDetailHeadline(row boardIssueRow, width int) string {
	issueID := row.Issue.ID
	if width < 56 {
		issueID = boardDisplayIssueID(row.Issue.ID, width)
	}
	return issueID + " · " + row.Issue.Title
}

func boardDetailMetaParts(row boardIssueRow, theme boardTheme, width int) []boardMetaPart {
	parts := []boardMetaPart{
		{label: row.Issue.Type, fg: theme.metaFG, bg: ""},
		{label: row.Issue.Status, fg: boardStatusPalette(theme, row.Issue.Status), bg: ""},
	}
	if row.Issue.Priority != "" {
		parts = append(parts, boardMetaPart{label: row.Issue.Priority, fg: theme.keyFG, bg: ""})
	}
	parentID := row.Issue.ParentID
	if parentID == "" {
		parentID = row.Hierarchy.ParentID
	}
	if parentID != "" {
		label := "parent " + parentID
		if width < 56 {
			label = "p:" + boardDisplayIssueID(parentID, width)
		}
		parts = append(parts, boardMetaPart{label: label, fg: theme.mutedFG, bg: ""})
	}
	if row.Hierarchy.HasChildren {
		parts = append(parts, boardMetaPart{label: fmt.Sprintf("%d child", row.Hierarchy.ChildCount), fg: theme.accentFG, bg: ""})
	}
	return parts
}

func boardRenderMetaLines(theme boardTheme, parts []boardMetaPart, width, maxLines int) []string {
	if len(parts) == 0 || maxLines <= 0 {
		return nil
	}
	lines := make([]string, 0, maxLines)
	current := ""
	for idx, part := range parts {
		token := boardMetaToken(theme, part.label, part.fg, part.bg)
		if current == "" {
			current = token
			continue
		}
		candidate := current + " " + token
		if visualWidth(candidate) <= width {
			current = candidate
			continue
		}
		lines = append(lines, padVisual(current, width))
		if len(lines) == maxLines {
			lines[len(lines)-1] = padVisual(boardMetaOverflowToken(theme, len(parts)-idx+1), width)
			return lines
		}
		current = token
	}
	if current != "" && len(lines) < maxLines {
		lines = append(lines, padVisual(current, width))
	}
	return lines
}

func boardMetaOverflowToken(theme boardTheme, remaining int) string {
	if remaining <= 1 {
		return boardMetaToken(theme, "+1 more", theme.mutedFG, "")
	}
	return boardMetaToken(theme, fmt.Sprintf("+%d more", remaining), theme.mutedFG, "")
}

func boardCompactHierarchyPath(path []string, width int, compact bool) string {
	if len(path) == 0 {
		return ""
	}
	full := strings.Join(path, " > ")
	if !compact {
		return full
	}
	if len(path) == 1 {
		return path[0]
	}
	rootLeaf := path[0] + " > ... > " + path[len(path)-1]
	if visualWidth(rootLeaf) <= maxInt(width-10, 16) {
		return rootLeaf
	}
	return "... > " + path[len(path)-1]
}
