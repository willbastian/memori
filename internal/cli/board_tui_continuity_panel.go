package cli

import (
	"fmt"
	"strings"

	"github.com/willbastian/memori/internal/store"
)

func boardContinuityPanel(model boardTUIModel, theme boardTheme, width, height int) []string {
	lines := make([]string, 0, height)
	lines = append(lines, boardPanelHeader(theme, "Continuity", "Audit", width))

	row, ok := model.selectedRow()
	if !ok {
		lines = append(lines, theme.paintLine(theme.mutedFG, "", false, padRight(" no issue selected", width)))
		for len(lines) < height {
			lines = append(lines, padRight("", width))
		}
		return lines[:height]
	}

	audit := model.audit
	lines = append(lines, theme.paintLine(theme.detailFG, theme.panelAltBG, true, padRight(truncateBoardLine(" "+boardDetailHeadline(row, width)+" ", width), width)))
	lines = append(lines, boardRenderMetaLines(theme, boardContinuityMetaParts(audit, theme), width, 2)...)
	lines = append(lines, theme.paintLine(theme.mutedFG, theme.panelAltBG, false, padRight(truncateBoardLine(" "+boardContinuityActionSummary(audit)+" ", width), width)))
	lines = append(lines, theme.paintLine(theme.borderFG, "", false, strings.Repeat(".", width)))

	for _, section := range boardContinuitySections(audit, width) {
		lines = append(lines, boardDetailHeaderLine(theme, section.label, width, section.muted))
		for _, line := range section.lines {
			fg := theme.detailFG
			if section.muted {
				fg = theme.mutedFG
			}
			lines = append(lines, theme.paintLine(fg, "", false, padRight(truncateBoardLine(line, width), width)))
		}
	}

	for len(lines) < height {
		lines = append(lines, padRight("", width))
	}
	return lines[:minInt(len(lines), height)]
}

func boardContinuityMetaParts(audit store.ContinuityAuditSnapshot, theme boardTheme) []boardMetaPart {
	parts := []boardMetaPart{
		{label: strings.ToUpper(boardContinuityStatusLabel(audit.Resolution.Status)), fg: theme.keyFG, bg: theme.panelAltBG},
	}
	if source := strings.TrimSpace(audit.Resolution.Source); source != "" {
		parts = append(parts, boardMetaPart{label: source, fg: theme.metaFG, bg: ""})
	}
	if sessionID := strings.TrimSpace(audit.Resolution.SessionID); sessionID != "" {
		parts = append(parts, boardMetaPart{label: "sess " + sessionID, fg: theme.accentFG, bg: ""})
	}
	if packetID := strings.TrimSpace(audit.Resolution.PacketID); packetID != "" {
		scope := audit.Resolution.PacketScope
		if scope == "" {
			scope = "pkt"
		}
		parts = append(parts, boardMetaPart{label: scope + " " + packetID, fg: theme.readyFG, bg: ""})
	}
	return parts
}

func boardContinuitySections(audit store.ContinuityAuditSnapshot, width int) []boardDetailSection {
	sections := make([]boardDetailSection, 0, 4)
	sections = append(sections, boardDetailSection{label: "Decision", lines: boardContinuityDecisionLines(audit, width)})
	sections = append(sections, boardDetailSection{label: "Next Step", lines: boardContinuityNextStepLines(audit, width)})
	sections = append(sections, boardDetailSection{label: "Current Session", lines: boardContinuityCurrentSessionLines(audit, width)})
	sections = append(sections, boardDetailSection{label: "Evidence", lines: boardContinuityEvidenceLines(audit, width), muted: true})
	return sections
}

func boardContinuityDecisionLines(audit store.ContinuityAuditSnapshot, width int) []string {
	lines := []string{
		"  " + boardContinuityVerdict(audit),
	}
	if source := boardContinuityResumeSource(audit); source != "" {
		lines = append(lines, "  memori will resume from "+source)
	}
	if consequence := boardContinuityStopNowLine(audit); consequence != "" {
		lines = append(lines, "  if you stop now: "+consequence)
	}
	return boardClampSectionLines(lines, width)
}

func boardContinuityCurrentSessionLines(audit store.ContinuityAuditSnapshot, width int) []string {
	selected, ok := boardSelectedContinuitySession(audit)
	if !ok {
		if audit.Issue.HasPacket {
			return boardClampSectionLines([]string{
				"  no issue-scoped session is selected right now",
				fmt.Sprintf("  saved issue packet: %s", audit.Issue.LatestPacket.PacketID),
			}, width)
		}
		return []string{"  no issue-scoped session is available yet"}
	}

	lines := []string{
		fmt.Sprintf("  session: %s (%s)", selected.Session.SessionID, selected.Lifecycle),
		fmt.Sprintf("  handoff state: summary %s, session packet %s", boardYesNoWord(selected.HasSummary), boardYesNoWord(selected.HasPacket)),
	}
	if note := boardContinuitySessionValueLine(selected); note != "" {
		lines = append(lines, "  "+note)
	}
	if started := strings.TrimSpace(selected.Session.StartedAt); started != "" {
		lines = append(lines, "  started: "+boardContinuityTime(started))
	}
	return boardClampSectionLines(lines, width)
}

func boardContinuityNextStepLines(audit store.ContinuityAuditSnapshot, width int) []string {
	lines := []string{
		"  " + boardContinuityActionSummary(audit),
	}
	for _, step := range boardContinuityRecommendedCommands(audit) {
		lines = append(lines, "  "+step)
	}
	return boardClampSectionLines(lines, width)
}

func boardContinuityEvidenceLines(audit store.ContinuityAuditSnapshot, width int) []string {
	lines := make([]string, 0, 8)
	if len(audit.Alerts) == 0 {
		lines = append(lines, "  alerts: none")
	} else {
		for idx, alert := range audit.Alerts {
			if idx >= 2 {
				lines = append(lines, fmt.Sprintf("  alerts: +%d more", len(audit.Alerts)-idx))
				break
			}
			lines = append(lines, fmt.Sprintf("  alert: %s", alert.Message))
		}
	}

	if len(audit.Sessions) > 1 {
		lines = append(lines, fmt.Sprintf("  candidate sessions: %d", len(audit.Sessions)))
		for _, session := range audit.Sessions {
			if session.IsSelected {
				continue
			}
			lines = append(lines, fmt.Sprintf("  alternate: %s (%s)", session.Session.SessionID, session.Lifecycle))
			break
		}
	}

	if len(audit.IssuePackets) > 0 {
		packet := audit.IssuePackets[0]
		lines = append(lines, fmt.Sprintf("  issue packet: %s (%s)", packet.Packet.PacketID, packet.Status))
	}
	if len(audit.SessionPackets) > 0 {
		packet := audit.SessionPackets[0]
		lines = append(lines, fmt.Sprintf("  session packet: %s (%s)", packet.Packet.PacketID, packet.Status))
	}

	for idx, write := range audit.RecentWrites {
		if idx >= 2 {
			break
		}
		lines = append(lines, fmt.Sprintf("  recent: %s %s", write.EventType, boardContinuityTime(write.CreatedAt)))
	}

	if len(lines) == 0 {
		lines = append(lines, "  no extra continuity evidence")
	}
	return boardClampSectionLines(lines, width)
}

func boardContinuityVerdict(audit store.ContinuityAuditSnapshot) string {
	switch {
	case boardContinuityHasAlert(audit, "multiple-open-sessions"):
		return "Resume is possible, but session selection is ambiguous."
	case boardContinuityHasAlert(audit, "session-unsaved"):
		return "Resume is available, but handoff is weak."
	case boardContinuityHasAlert(audit, "issue-packet-stale"):
		return "Resume exists, but the issue packet is stale."
	case strings.TrimSpace(audit.Resolution.Status) == "missing":
		return "Continuity is thin; memori has little saved state to resume from."
	case strings.TrimSpace(audit.Resolution.Status) == "fallback":
		return "Resume works, but it depends on fallback context instead of a clean saved packet."
	default:
		return "Resume looks healthy for this issue."
	}
}

func boardContinuityResumeSource(audit store.ContinuityAuditSnapshot) string {
	switch strings.TrimSpace(audit.Resolution.Source) {
	case "agent-focus-session":
		return "the session already tied to this agent's focus"
	case "agent-focus-issue-open":
		return "the latest open session for this focused issue"
	case "agent-focus-issue-latest":
		return "the latest historical session for this focused issue"
	case "latest-open-issue":
		return "the latest open session for this issue"
	case "latest-session-issue":
		return "the latest historical session for this issue"
	default:
		if audit.Issue.HasPacket {
			return "the saved issue packet"
		}
		return ""
	}
}

func boardContinuityStopNowLine(audit store.ContinuityAuditSnapshot) string {
	selected, ok := boardSelectedContinuitySession(audit)
	if !ok {
		if audit.Issue.HasPacket {
			return "the next worker will rely on the saved issue packet"
		}
		return "the next worker will have to reconstruct context manually"
	}
	switch {
	case selected.HasPacket:
		return "the next worker can resume from a saved session packet"
	case selected.HasSummary:
		return "the next worker will rely on a session summary instead of a saved session packet"
	case selected.Lifecycle == "active":
		return "the next worker will resume from raw context chunks"
	default:
		return "the next worker will reconstruct context from historical session state"
	}
}

func boardContinuityActionSummary(audit store.ContinuityAuditSnapshot) string {
	switch {
	case boardContinuityHasAlert(audit, "session-unsaved"):
		return "Best next step: save this session before handing it off."
	case boardContinuityHasAlert(audit, "multiple-open-sessions"):
		return "Best next step: keep resume explicit while more than one session is open."
	case boardContinuityHasAlert(audit, "issue-packet-stale"):
		return "Best next step: refresh the issue packet so ticket-level continuity is current."
	default:
		return "No urgent continuity repair is needed right now."
	}
}

func boardContinuityRecommendedCommands(audit store.ContinuityAuditSnapshot) []string {
	selected, hasSelected := boardSelectedContinuitySession(audit)
	commands := make([]string, 0, 2)
	switch {
	case hasSelected && !selected.HasSummary:
		commands = append(commands, fmt.Sprintf("memori context summarize --session %s", selected.Session.SessionID))
	case hasSelected && !selected.HasPacket:
		commands = append(commands, fmt.Sprintf("memori context packet build --scope session --id %s", selected.Session.SessionID))
	case boardContinuityHasAlert(audit, "issue-packet-stale") && audit.Issue.IssueID != "":
		commands = append(commands, fmt.Sprintf("memori context packet build --scope issue --id %s", audit.Issue.IssueID))
	case hasSelected:
		commands = append(commands, fmt.Sprintf("memori context resume --session %s", selected.Session.SessionID))
	}
	if boardContinuityHasAlert(audit, "multiple-open-sessions") && hasSelected {
		commands = append(commands, fmt.Sprintf("memori context resume --session %s", selected.Session.SessionID))
	}
	return dedupeStrings(commands)
}

func boardSelectedContinuitySession(audit store.ContinuityAuditSnapshot) (store.ContinuitySessionCandidate, bool) {
	for _, session := range audit.Sessions {
		if session.IsSelected {
			return session, true
		}
	}
	if audit.Session.HasSession {
		return store.ContinuitySessionCandidate{
			Session:    audit.Session.Session,
			Lifecycle:  boardSessionLifecycle(audit.Session.Session),
			HasSummary: strings.TrimSpace(audit.Session.Session.SummaryEventID) != "",
			HasPacket:  audit.Session.HasPacket,
			IsSelected: true,
		}, true
	}
	return store.ContinuitySessionCandidate{}, false
}

func boardContinuitySessionValueLine(session store.ContinuitySessionCandidate) string {
	switch {
	case session.HasPacket:
		return "handoff-ready: a saved session packet already exists"
	case session.HasSummary:
		return "resume-ready enough: a summary exists, but no saved session packet yet"
	case session.Lifecycle == "active":
		return "handoff is weak: work exists, but only raw session context has been saved"
	default:
		return ""
	}
}

func boardContinuityHasAlert(audit store.ContinuityAuditSnapshot, code string) bool {
	for _, alert := range audit.Alerts {
		if strings.TrimSpace(alert.Code) == code {
			return true
		}
	}
	return false
}

func boardSessionLifecycle(session store.Session) string {
	if strings.TrimSpace(session.EndedAt) != "" {
		return "closed"
	}
	return "active"
}

func boardContinuityStatusLabel(status string) string {
	switch strings.TrimSpace(status) {
	case "fallback":
		return "fallback"
	case "stale":
		return "stale"
	case "ambiguous":
		return "amb"
	case "missing":
		return "miss"
	default:
		return "fresh"
	}
}

func boardContinuityTime(ts string) string {
	ts = strings.TrimSpace(ts)
	if len(ts) >= 16 {
		return ts[:16]
	}
	return ts
}

func boardClampSectionLines(lines []string, width int) []string {
	clamped := make([]string, 0, len(lines))
	for _, line := range lines {
		clamped = append(clamped, truncateBoardLine(line, width))
	}
	return clamped
}

func boardYesNo(value bool) string {
	if value {
		return "Y"
	}
	return "N"
}

func boardYesNoWord(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func dedupeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
