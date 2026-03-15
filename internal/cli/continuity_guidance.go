package cli

import (
	"fmt"
	"strings"

	"github.com/willbastian/memori/internal/store"
)

func continuitySignalsPresent(reasons []string) bool {
	for _, reason := range reasons {
		normalized := strings.ToLower(strings.TrimSpace(reason))
		if normalized == "" {
			continue
		}
		if strings.Contains(normalized, "focus") ||
			strings.Contains(normalized, "packet") ||
			strings.Contains(normalized, "continuity") ||
			strings.Contains(normalized, "resume") {
			return true
		}
	}
	return false
}

func continuityBootstrapMessage(agent string) string {
	agent = strings.TrimSpace(agent)
	if agent == "" {
		return "No saved focus, recovery packet, or open-loop continuity is shaping this recommendation yet."
	}
	return fmt.Sprintf("No saved focus, recovery packet, or open-loop continuity is shaping recommendations for %s yet.", agent)
}

func continuityBootstrapSteps(issueID string) []string {
	issueID = strings.TrimSpace(issueID)
	if issueID == "" {
		return nil
	}
	return []string{
		"memori context checkpoint",
		fmt.Sprintf("memori context packet build --scope issue --id %s", issueID),
		fmt.Sprintf("memori context loops --issue %s", issueID),
	}
}

func issueContinuityGuidance(issue store.Issue, command, issueSessionID string) (string, []string) {
	command = strings.TrimSpace(command)
	issueSessionID = strings.TrimSpace(issueSessionID)
	issueType := strings.ToLower(strings.TrimSpace(issue.Type))
	status := strings.ToLower(strings.TrimSpace(issue.Status))
	issueID := strings.TrimSpace(issue.ID)
	if issueID == "" || issueType == "epic" {
		return "", nil
	}

	switch status {
	case "todo":
		message := "Start continuity when execution or handoff begins so this issue can resume cleanly."
		if command == "create" {
			message = "Capture continuity in-product as soon as work starts or you hand this issue to another worker."
		}
		return message, []string{
			"memori context checkpoint",
			fmt.Sprintf("memori context packet build --scope issue --id %s", issueID),
		}
	case "inprogress":
		steps := []string{
			continuityCheckpointStep(issueSessionID),
			continuitySummarizeStep(issueSessionID),
			fmt.Sprintf("memori context packet build --scope issue --id %s", issueID),
		}
		if command == "show" && issueSessionID != "" {
			steps = append([]string{fmt.Sprintf("memori context resume --session %s", issueSessionID)}, steps...)
		}
		return "This issue is active work; keep continuity current so pause, resume, and handoff stay lightweight.", steps
	case "blocked":
		steps := []string{
			continuitySummarizeStep(issueSessionID),
			fmt.Sprintf("memori context packet build --scope issue --id %s", issueID),
			fmt.Sprintf("memori context loops --issue %s", issueID),
		}
		if command == "show" && issueSessionID != "" {
			steps = append([]string{fmt.Sprintf("memori context resume --session %s", issueSessionID)}, steps...)
		}
		return "This issue is blocked; preserve the current state before waiting or handing it off.", steps
	default:
		return "", nil
	}
}

func continuityCheckpointStep(sessionID string) string {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return "memori context checkpoint"
	}
	return fmt.Sprintf("memori context checkpoint --session %s", sessionID)
}

func continuitySummarizeStep(sessionID string) string {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return "memori context summarize"
	}
	return fmt.Sprintf("memori context summarize --session %s", sessionID)
}

func issueResumeSteps(issue store.Issue, issueSessionID string) []string {
	status := strings.ToLower(strings.TrimSpace(issue.Status))
	if status != "inprogress" && status != "blocked" {
		return nil
	}
	issueSessionID = strings.TrimSpace(issueSessionID)
	if issueSessionID == "" {
		return nil
	}
	return []string{
		fmt.Sprintf("memori context resume --session %s", issueSessionID),
		fmt.Sprintf("memori context resume --session %s --agent <agent-id>", issueSessionID),
	}
}

func packetBuildNextSteps(packetID, scope, issueID, sessionID string) []string {
	packetID = strings.TrimSpace(packetID)
	scope = strings.TrimSpace(scope)
	issueID = strings.TrimSpace(issueID)
	sessionID = strings.TrimSpace(sessionID)

	steps := make([]string, 0, 3)
	if packetID != "" {
		steps = append(steps, fmt.Sprintf("memori context packet show --packet %s", packetID))
	}
	switch scope {
	case "issue":
		if packetID != "" {
			steps = append(steps, fmt.Sprintf("memori context packet use --agent <agent-id> --packet %s", packetID))
		}
		if issueID != "" {
			steps = append(steps, fmt.Sprintf("memori issue show --key %s", issueID))
		}
	case "session":
		if sessionID != "" {
			steps = append(steps, fmt.Sprintf("memori context rehydrate --session %s", sessionID))
		}
		if packetID != "" {
			steps = append(steps, fmt.Sprintf("memori context packet use --agent <agent-id> --packet %s", packetID))
		}
	}
	return steps
}

func packetUseNextSteps(agentID, packetID, issueID, sessionID string) []string {
	agentID = strings.TrimSpace(agentID)
	packetID = strings.TrimSpace(packetID)
	issueID = strings.TrimSpace(issueID)
	sessionID = strings.TrimSpace(sessionID)

	steps := make([]string, 0, 3)
	if issueID != "" {
		steps = append(steps,
			fmt.Sprintf("memori issue next --agent %s", agentID),
			fmt.Sprintf("memori board --agent %s", agentID),
			fmt.Sprintf("memori issue show --key %s", issueID),
		)
		return steps
	}
	if sessionID != "" {
		steps = append(steps, fmt.Sprintf("memori context rehydrate --session %s", sessionID))
	}
	if agentID != "" {
		steps = append(steps,
			fmt.Sprintf("memori issue next --agent %s", agentID),
			fmt.Sprintf("memori board --agent %s", agentID),
		)
	}
	if packetID != "" {
		steps = append(steps, fmt.Sprintf("memori context packet show --packet %s", packetID))
	}
	return steps
}

func rehydrateSourceMessage(source string) string {
	switch strings.TrimSpace(source) {
	case "packet":
		return "Using the latest saved recovery packet."
	case "relevant-chunks-fallback":
		return "No saved session packet was available; synthesized resume context from recent session chunks."
	case "closed-session-summary":
		return "No saved closed-session packet was available; synthesized a closure-aware summary instead."
	default:
		return ""
	}
}

func rehydrateNextSteps(sessionID, source, packetID, issueID string) []string {
	sessionID = strings.TrimSpace(sessionID)
	source = strings.TrimSpace(source)
	packetID = strings.TrimSpace(packetID)
	issueID = strings.TrimSpace(issueID)

	steps := make([]string, 0, 3)
	if issueID != "" {
		steps = append(steps, fmt.Sprintf("memori issue show --key %s", issueID))
	}
	if packetID != "" {
		steps = append(steps, fmt.Sprintf("memori context packet show --packet %s", packetID))
	}
	if sessionID != "" && source != "packet" {
		steps = append(steps, fmt.Sprintf("memori context packet build --scope session --id %s", sessionID))
	}
	if sessionID != "" {
		steps = append(steps, fmt.Sprintf("memori context summarize --session %s", sessionID))
	}
	return steps
}

func packetIssueIDForCLI(packet store.RehydratePacket) string {
	if strings.TrimSpace(packet.IssueID) != "" {
		return strings.TrimSpace(packet.IssueID)
	}
	if strings.TrimSpace(packet.Scope) == "issue" && strings.TrimSpace(packet.ScopeID) != "" {
		return strings.TrimSpace(packet.ScopeID)
	}
	if stateRaw, ok := packet.Packet["state"].(map[string]any); ok {
		if issueID := strings.TrimSpace(fmt.Sprint(stateRaw["issue_id"])); issueID != "" && issueID != "<nil>" {
			return issueID
		}
	}
	return ""
}

func packetSessionIDForCLI(packet store.RehydratePacket) string {
	if strings.TrimSpace(packet.SessionID) != "" {
		return strings.TrimSpace(packet.SessionID)
	}
	if strings.TrimSpace(packet.Scope) == "session" && strings.TrimSpace(packet.ScopeID) != "" {
		return strings.TrimSpace(packet.ScopeID)
	}
	if stateRaw, ok := packet.Packet["state"].(map[string]any); ok {
		if sessionID := strings.TrimSpace(fmt.Sprint(stateRaw["session_id"])); sessionID != "" && sessionID != "<nil>" {
			return sessionID
		}
	}
	return ""
}
