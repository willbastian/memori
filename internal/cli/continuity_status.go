package cli

import (
	"fmt"
	"strings"

	"github.com/willbastian/memori/internal/store"
)

func continuityStatusLines(snapshot store.ContinuitySnapshot) []string {
	lines := make([]string, 0, 4)

	if strings.TrimSpace(snapshot.Agent.AgentID) != "" {
		lines = append(lines, continuityAgentStatusLine(snapshot.Agent))
	}
	lines = append(lines, continuitySessionStatusLine(snapshot.Session))
	if strings.TrimSpace(snapshot.Issue.IssueID) != "" {
		lines = append(lines, continuityIssueStatusLines(snapshot.Issue)...)
	}
	return filterNonEmpty(lines)
}

func continuityAgentStatusLine(snapshot store.AgentContinuitySnapshot) string {
	if !snapshot.HasFocus {
		return fmt.Sprintf("Agent %s has no saved focus yet.", snapshot.AgentID)
	}

	line := fmt.Sprintf("Agent %s focus points to %s", snapshot.AgentID, snapshot.Focus.ActiveIssueID)
	if snapshot.Focus.ActiveCycleNo > 0 {
		line += fmt.Sprintf(" cycle %d", snapshot.Focus.ActiveCycleNo)
	}
	if snapshot.HasLastPacket {
		line += fmt.Sprintf(" via packet %s", snapshot.LastPacket.PacketID)
	}
	return line + "."
}

func continuitySessionStatusLine(snapshot store.SessionContinuitySnapshot) string {
	if !snapshot.HasSession {
		return "No open or historical session is available yet."
	}

	label := "Latest open session"
	if snapshot.Source == "latest-session" {
		label = "Latest session"
	}
	state := "has no saved summary"
	if strings.TrimSpace(snapshot.Session.SummaryEventID) != "" {
		state = fmt.Sprintf("has summary %s", snapshot.Session.SummaryEventID)
	}
	if strings.TrimSpace(snapshot.Session.EndedAt) != "" {
		state = "is closed and " + state
	}
	if snapshot.HasPacket {
		return fmt.Sprintf("%s %s %s with session packet %s.", label, snapshot.Session.SessionID, state, snapshot.Packet.PacketID)
	}
	return fmt.Sprintf("%s %s %s and no saved session packet yet.", label, snapshot.Session.SessionID, state)
}

func continuityIssueStatusLines(snapshot store.IssueContinuitySnapshot) []string {
	lines := make([]string, 0, 2)
	cycleLabel := ""
	if snapshot.CurrentCycleNo > 0 {
		cycleLabel = fmt.Sprintf(" cycle %d", snapshot.CurrentCycleNo)
	}

	switch {
	case snapshot.HasPacket && snapshot.PacketFresh:
		lines = append(lines, fmt.Sprintf("Latest issue packet %s is fresh for %s%s.", snapshot.LatestPacket.PacketID, snapshot.IssueID, cycleLabel))
	case snapshot.HasPacket && snapshot.PacketStale:
		lines = append(lines, fmt.Sprintf("Latest issue packet %s is stale for %s%s.", snapshot.LatestPacket.PacketID, snapshot.IssueID, cycleLabel))
	default:
		lines = append(lines, fmt.Sprintf("No saved issue packet exists for %s%s yet.", snapshot.IssueID, cycleLabel))
	}

	if snapshot.OpenLoopCount > 0 {
		lines = append(lines, fmt.Sprintf("%s has %d open loop(s)%s.", snapshot.IssueID, snapshot.OpenLoopCount, cycleLabel))
	}
	return lines
}

func continuityPressureLines(issue store.Issue, snapshot store.ContinuitySnapshot, agentID string) []string {
	issueID := strings.TrimSpace(issue.ID)
	status := strings.ToLower(strings.TrimSpace(issue.Status))
	if issueID == "" || strings.EqualFold(strings.TrimSpace(issue.Type), "epic") {
		return nil
	}

	lines := make([]string, 0, 3)
	if snapshot.Issue.PacketStale {
		lines = append(lines, fmt.Sprintf("%s is %s and its saved issue packet is stale; rebuild it before the next handoff.", issueID, continuityPressureStatusLabel(status)))
	}
	if shouldPressureMissingIssuePacket(status) && !snapshot.Issue.HasPacket {
		lines = append(lines, fmt.Sprintf("%s is %s and has no saved issue packet yet; capture one before the next handoff.", issueID, continuityPressureStatusLabel(status)))
	}
	if shouldPressureOpenSession(status, snapshot.Session) {
		lines = append(lines, fmt.Sprintf("Open session %s has no saved session packet yet; save continuity before you pause or close it.", snapshot.Session.Session.SessionID))
	}
	if shouldPressureMissingFocus(status, issueID, snapshot.Agent, agentID) {
		lines = append(lines, fmt.Sprintf("Agent %s has no saved focus for %s yet; resume will stay broad until focus is refreshed.", strings.TrimSpace(agentID), issueID))
	}
	if len(lines) > 0 {
		return filterNonEmpty(lines)
	}

	if helpful := continuityHelpfulLine(issueID, snapshot, agentID); helpful != "" {
		lines = append(lines, helpful)
	}
	return filterNonEmpty(lines)
}

func continuityPressureStatusLabel(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "inprogress":
		return "active"
	case "blocked":
		return "blocked"
	case "todo":
		return "ready to resume"
	default:
		return "open"
	}
}

func shouldPressureMissingIssuePacket(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "inprogress", "blocked":
		return true
	default:
		return false
	}
}

func shouldPressureOpenSession(status string, snapshot store.SessionContinuitySnapshot) bool {
	if strings.TrimSpace(snapshot.Session.EndedAt) != "" || !snapshot.HasSession || snapshot.HasPacket {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "inprogress", "blocked":
		return true
	default:
		return false
	}
}

func shouldPressureMissingFocus(status, issueID string, snapshot store.AgentContinuitySnapshot, agentID string) bool {
	if strings.TrimSpace(agentID) == "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "inprogress", "blocked":
		if !snapshot.HasFocus {
			return true
		}
		return strings.TrimSpace(snapshot.Focus.ActiveIssueID) != issueID
	default:
		return false
	}
}

func continuityHelpfulLine(issueID string, snapshot store.ContinuitySnapshot, agentID string) string {
	if !snapshot.Issue.HasPacket || !snapshot.Issue.PacketFresh {
		return ""
	}

	line := fmt.Sprintf("%s is resume-ready with a fresh issue packet", issueID)
	agentID = strings.TrimSpace(agentID)
	if agentID != "" && snapshot.Agent.HasFocus && strings.TrimSpace(snapshot.Agent.Focus.ActiveIssueID) == issueID {
		line += fmt.Sprintf(" and saved focus for %s", agentID)
	}
	if snapshot.Session.HasSession && snapshot.Session.HasPacket && strings.TrimSpace(snapshot.Session.Session.SessionID) != "" {
		line += fmt.Sprintf("; session %s already has a saved packet too", snapshot.Session.Session.SessionID)
	}
	return line + "."
}

func filterNonEmpty(lines []string) []string {
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		filtered = append(filtered, line)
	}
	return filtered
}
