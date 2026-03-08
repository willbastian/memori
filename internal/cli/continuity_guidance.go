package cli

import (
	"fmt"
	"strings"
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
		fmt.Sprintf("memori context packet build --scope issue --id %s", issueID),
		fmt.Sprintf("memori context loops --issue %s", issueID),
	}
}
