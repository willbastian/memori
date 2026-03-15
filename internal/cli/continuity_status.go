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
