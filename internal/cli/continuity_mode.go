package cli

import (
	"fmt"
	"os"
	"strings"
)

type continuityMode string

const (
	continuityModeManual continuityMode = "manual"
	continuityModeAssist continuityMode = "assist"
	continuityModeAuto   continuityMode = "auto"
)

func resolveContinuityMode(modeHint string) (continuityMode, error) {
	candidate := strings.ToLower(strings.TrimSpace(modeHint))
	if candidate == "" {
		candidate = strings.ToLower(strings.TrimSpace(os.Getenv("MEMORI_CONTINUITY_MODE")))
	}
	if candidate == "" {
		return continuityModeAuto, nil
	}
	switch continuityMode(candidate) {
	case continuityModeManual, continuityModeAssist, continuityModeAuto:
		return continuityMode(candidate), nil
	default:
		return "", fmt.Errorf("--continuity must be manual|assist|auto")
	}
}

func issueUpdateContinuityAssistSteps(issueID, requestedStatus, agentID, note, reason string) []string {
	issueID = strings.TrimSpace(issueID)
	requestedStatus = strings.ToLower(strings.TrimSpace(requestedStatus))
	agentID = strings.TrimSpace(agentID)
	note = strings.TrimSpace(note)
	reason = strings.TrimSpace(reason)

	switch requestedStatus {
	case "inprogress":
		cmd := fmt.Sprintf("memori context start --issue %s", issueID)
		if agentID != "" {
			cmd += " --agent " + agentID
		}
		return []string{cmd}
	case "blocked":
		cmd := "memori context save"
		if note != "" {
			cmd += " --note " + shellQuote(note)
		}
		return []string{cmd}
	case "done":
		cmd := "memori context save --close"
		if note != "" {
			cmd += " --note " + shellQuote(note)
		}
		if reason != "" {
			cmd += " --reason " + shellQuote(reason)
		}
		return []string{cmd}
	default:
		return nil
	}
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	if !strings.ContainsAny(value, " \t'\"") {
		return value
	}
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}
