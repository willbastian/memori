package cli

import (
	"fmt"
	"strings"
)

func boardDetailPanel(model boardTUIModel, theme boardTheme, width, height int) []string {
	return boardRenderViewportPanel(model, boardDetailPanelContent(model, theme, width), theme, width, height)
}

func boardDetailPanelContent(model boardTUIModel, theme boardTheme, width int) boardPanelContent {
	if !model.detailOpen {
		return boardPanelContent{
			title:    "Issue Detail",
			subtitle: "Context",
			body: []string{
				theme.paintLine(theme.mutedFG, theme.panelAltBG, false, padRight(" press <enter> to expand the selected issue ", width)),
			},
		}
	}

	row, ok := model.selectedRow()
	if !ok {
		return boardPanelContent{
			title:    "Issue Detail",
			subtitle: "Context",
			body: []string{
				theme.paintLine(theme.mutedFG, "", false, padRight(" no issue selected", width)),
			},
		}
	}

	lines := make([]string, 0, 24)
	for _, line := range boardDetailIntroLines(row, theme, width) {
		lines = append(lines, line)
	}
	lines = append(lines, theme.paintLine(theme.mutedFG, theme.panelAltBG, false, padRight(truncateBoardLine(" "+boardDetailActionLine(row, width)+" ", width), width)))
	lines = append(lines, boardInspectorLeadLines(model, theme, width)...)
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
	return boardPanelContent{
		title:    "Issue Detail",
		subtitle: "Context",
		body:     lines,
	}
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
	workspaceLabel, workspace := boardWorkspaceSection(row, width)
	descriptionLabel, description := boardWrappedSection("Description", row.Issue.Description, width)
	acceptanceLabel, acceptance := boardWrappedSection("Acceptance", row.Issue.Acceptance, width)
	reasonsLabel, reasons := boardWrappedSection("Reasons", strings.Join(orderBoardReasons(row.Reasons), "; "), width)
	referencesLabel, references := boardReferenceSection(row.Issue.References, width)

	if compact {
		appendSection(workspaceLabel, workspace, false)
		appendSection(descriptionLabel, description, false)
		appendSection(acceptanceLabel, acceptance, false)
		appendSection(hierarchyLabel, hierarchy, false)
		appendSection(referencesLabel, references, true)
		appendSection(reasonsLabel, reasons, false)
		return sections
	}

	appendSection(workspaceLabel, workspace, false)
	appendSection(descriptionLabel, description, false)
	appendSection(acceptanceLabel, acceptance, false)
	appendSection(hierarchyLabel, hierarchy, false)
	appendSection(referencesLabel, references, true)
	appendSection(reasonsLabel, reasons, false)
	return sections
}

func boardDetailActionLine(row boardIssueRow, width int) string {
	if len(row.Reasons) > 0 {
		return "next: " + truncateBoardLine(orderBoardReasons(row.Reasons)[0], maxInt(width-8, 12))
	}
	switch row.Issue.Status {
	case "InProgress":
		return "next: keep continuity current while this work is active"
	case "Blocked":
		return "next: inspect the blocker and the required follow-up"
	case "Done":
		return "status: done and kept here for historical context"
	case "WontDo":
		return "status: won't do and kept here for historical context"
	default:
		return "next: review the scope and acceptance before starting"
	}
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
	if compact {
		shapeParts := make([]string, 0, 2)
		if row.Hierarchy.Depth > 0 || row.Hierarchy.DescendantCount > 0 {
			shapeParts = append(shapeParts, fmt.Sprintf("d%d desc%d", row.Hierarchy.Depth, row.Hierarchy.DescendantCount))
		}
		if len(row.Hierarchy.ChildIDs) > 0 {
			shapeParts = append(shapeParts, fmt.Sprintf("child%d", row.Hierarchy.ChildCount))
		}
		if len(shapeParts) > 0 {
			appendWrapped("shape", strings.Join(shapeParts, " "))
		}
	} else {
		if len(row.Hierarchy.ChildIDs) > 0 {
			appendWrapped("children", strings.Join(row.Hierarchy.ChildIDs, ", "))
		}
		if row.Hierarchy.Depth > 0 || row.Hierarchy.DescendantCount > 0 {
			appendWrapped("shape", fmt.Sprintf("depth %d, descendants %d", row.Hierarchy.Depth, row.Hierarchy.DescendantCount))
		}
	}
	if len(lines) == 0 {
		return "", nil
	}
	return "Hierarchy", lines
}

func boardWorkspaceSection(row boardIssueRow, width int) (string, []string) {
	if row.Workspace == nil {
		return "", nil
	}
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

	appendWrapped("path", row.Workspace.Path)
	appendWrapped("health", row.Workspace.Health)
	appendWrapped("branch", row.Workspace.Branch)
	appendWrapped("worktree", row.Workspace.WorktreeID)
	if len(lines) == 0 {
		return "", nil
	}
	return "Workspace", lines
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

func boardInspectorLeadLines(model boardTUIModel, theme boardTheme, width int) []string {
	lines := make([]string, 0, 2)
	if message := boardSnapshotStatusLine(model.snapshotLoad); message != "" {
		lines = append(lines, theme.paintLine(theme.mutedFG, theme.panelAltBG, false, padRight(truncateBoardLine(" "+message+" ", width), width)))
	}
	if model.panelMode == boardPanelModeContinuity {
		if message := boardAuditStatusLine(model.auditLoad); message != "" {
			lines = append(lines, theme.paintLine(theme.mutedFG, theme.panelAltBG, false, padRight(truncateBoardLine(" "+message+" ", width), width)))
		}
	}
	return lines
}

func boardSnapshotStatusLine(state boardAsyncLoadState) string {
	switch {
	case state.loading && state.stale:
		return "board refresh is in progress; inspector is showing the last successful snapshot"
	case state.loading:
		return "board refresh is in progress"
	case strings.TrimSpace(state.err) != "" && state.stale:
		return "the latest board refresh failed; inspector is showing the last successful snapshot"
	case strings.TrimSpace(state.err) != "":
		return "the latest board refresh failed; a new retry will happen automatically"
	default:
		return ""
	}
}

func boardAuditStatusLine(state boardAsyncLoadState) string {
	switch {
	case state.loading && state.stale:
		return "continuity refresh is in progress; showing the last successful audit"
	case state.loading:
		return "loading continuity evidence for the selected issue"
	case strings.TrimSpace(state.err) != "" && state.stale:
		return "the latest continuity refresh failed; showing the last successful audit"
	case strings.TrimSpace(state.err) != "":
		return "the latest continuity refresh failed; continuity evidence is unavailable right now"
	default:
		return ""
	}
}
