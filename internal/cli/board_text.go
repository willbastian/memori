package cli

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

func renderBoardSnapshot(snapshot boardSnapshot, opts boardRenderOptions) (string, error) {
	var out bytes.Buffer
	ui := newTextUI(&out)
	width := opts.Width
	if width <= 0 {
		width = 80
	}

	header := "memori board"
	if opts.Watch {
		header = fmt.Sprintf("%s [%s]", header, snapshot.GeneratedAt)
	}
	ui.heading(header)
	boardField(ui, "Summary", formatBoardSummary(snapshot.Summary, ui.colors), width)
	if snapshot.Agent != "" {
		boardField(ui, "Agent", snapshot.Agent, width)
	}
	if opts.Watch {
		boardField(ui, "Refresh", opts.Interval.String()+" (change-only)", width)
	}
	ui.blank()

	renderBoardNext(ui, snapshot.LikelyNext, width)
	if snapshot.Agent != "" && len(snapshot.LikelyNext) > 0 && !continuitySignalsPresent(snapshot.LikelyNext[0].Reasons) {
		ui.section("Continuity")
		ui.bullet(continuityBootstrapMessage(snapshot.Agent))
		for _, step := range continuityBootstrapSteps(snapshot.LikelyNext[0].Issue.ID) {
			ui.bullet(step)
		}
		ui.blank()
	}
	renderBoardSection(ui, "Active", snapshot.Active, boardSectionLimit(width), width)
	renderBoardSection(ui, "Blocked", snapshot.Blocked, boardSectionLimit(width), width)
	renderBoardSection(ui, "Ready", snapshot.Ready, boardSectionLimit(width), width)

	if !opts.Watch {
		nextCommand := "memori issue next"
		if snapshot.Agent != "" {
			nextCommand += " --agent " + snapshot.Agent
		}
		ui.nextSteps(
			"memori board --watch",
			nextCommand,
		)
	}
	return out.String(), nil
}

func renderBoardNext(ui textUI, rows []boardIssueRow, width int) {
	ui.section("Next")
	if len(rows) == 0 {
		ui.bullet("No continuity-ranked work is ready yet.")
		ui.blank()
		return
	}
	for _, row := range rows[:minInt(len(rows), boardLikelyNextLimit(width))] {
		ui.bullet(truncateBoardLine(formatBoardNextRow(row), width-2))
	}
	ui.blank()
}

func renderBoardSection(ui textUI, label string, rows []boardIssueRow, limit, width int) {
	ui.section(fmt.Sprintf("%s (%d)", label, len(rows)))
	if len(rows) == 0 {
		ui.bullet("none")
		ui.blank()
		return
	}
	show := minInt(len(rows), limit)
	for _, row := range rows[:show] {
		ui.bullet(truncateBoardLine(formatBoardIssueRow(row), width-2))
	}
	if len(rows) > show {
		ui.bullet(fmt.Sprintf("+%d more", len(rows)-show))
	}
	ui.blank()
}

func formatBoardIssueRow(row boardIssueRow) string {
	return fmt.Sprintf("%s %s", row.Issue.ID, row.Issue.Title)
}

func formatBoardNextRow(row boardIssueRow) string {
	line := formatBoardIssueRow(row)
	tags := boardReasonTags(row.Reasons)
	if row.Score > 0 {
		tags = append([]string{fmt.Sprintf("s%d", row.Score)}, tags...)
	}
	if len(tags) > 0 {
		line += " [" + strings.Join(tags, ",") + "]"
	}
	return line
}

func compactReasons(reasons []string, limit int) []string {
	if limit <= 0 || len(reasons) <= limit {
		return append([]string(nil), reasons...)
	}
	trimmed := append([]string(nil), reasons[:limit]...)
	trimmed = append(trimmed, fmt.Sprintf("+%d more", len(reasons)-limit))
	return trimmed
}

func orderBoardReasons(reasons []string) []string {
	ordered := append([]string(nil), reasons...)
	sort.SliceStable(ordered, func(i, j int) bool {
		leftWeight := boardReasonWeight(ordered[i])
		rightWeight := boardReasonWeight(ordered[j])
		if leftWeight != rightWeight {
			return leftWeight > rightWeight
		}
		return i < j
	})
	return ordered
}

func boardReasonWeight(reason string) int {
	for _, rule := range boardReasonRules {
		if rule.weight == 0 {
			continue
		}
		if strings.Contains(reason, rule.contains) {
			return rule.weight
		}
	}
	return 10
}

func boardReasonTags(reasons []string) []string {
	ordered := orderBoardReasons(reasons)
	tags := make([]string, 0, len(ordered))
	seen := make(map[string]struct{}, len(ordered))
	for _, reason := range ordered {
		tag := boardReasonTag(reason)
		if tag == "" {
			continue
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		tags = append(tags, tag)
	}
	return compactReasons(tags, 3)
}

func boardReasonTag(reason string) string {
	for _, rule := range boardReasonRules {
		if rule.tag == "" {
			continue
		}
		if strings.Contains(reason, rule.contains) {
			return rule.tag
		}
	}
	return ""
}

func formatBoardSummary(summary boardSummary, colors bool) string {
	parts := []string{
		fmt.Sprintf("total=%d", summary.Total),
		fmt.Sprintf("%s=%d", colorize(colors, colorForStatus("InProgress"), "ip"), summary.InProgress),
		fmt.Sprintf("%s=%d", colorize(colors, colorForStatus("Blocked"), "blocked"), summary.Blocked),
		fmt.Sprintf("%s=%d", colorize(colors, colorForStatus("Todo"), "todo"), summary.Todo),
		fmt.Sprintf("%s=%d", colorize(colors, colorForStatus("Done"), "done"), summary.Done),
		fmt.Sprintf("%s=%d", colorize(colors, colorForStatus("WontDo"), "wontdo"), summary.WontDo),
	}
	return strings.Join(parts, ", ")
}

func boardField(ui textUI, label, value string, width int) {
	available := width - len(label) - 2
	if available < 8 {
		available = 8
	}
	ui.field(label, truncateBoardLine(value, available))
}
