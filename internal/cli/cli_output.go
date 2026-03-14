package cli

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/willbastian/memori/internal/store"
)

func shouldUseColor(out io.Writer) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("MEMORI_COLOR"))) {
	case "always":
		return true
	case "never":
		return false
	}
	if os.Getenv("NO_COLOR") != "" || strings.TrimSpace(os.Getenv("CLICOLOR")) == "0" {
		return false
	}
	if force := strings.TrimSpace(os.Getenv("CLICOLOR_FORCE")); force != "" && force != "0" {
		return true
	}
	if force := strings.TrimSpace(os.Getenv("FORCE_COLOR")); force != "" && force != "0" {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("TERM")), "dumb") {
		return false
	}
	file, ok := out.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func colorForType(issueType string) string {
	switch issueType {
	case "Epic":
		return "34" // blue
	case "Story":
		return "36" // cyan
	case "Task":
		return "33" // yellow
	case "Bug":
		return "31" // red
	default:
		return "37" // white
	}
}

func colorForStatus(status string) string {
	switch status {
	case "Todo":
		return "90" // gray
	case "InProgress":
		return "33" // yellow
	case "Blocked":
		return "31" // red
	case "Done":
		return "32" // green
	case "WontDo":
		return "35" // magenta
	default:
		return "37" // white
	}
}

func colorForGateResult(result string) string {
	switch result {
	case "PASS":
		return "32" // green
	case "FAIL":
		return "31" // red
	case "BLOCKED":
		return "33" // yellow
	case "MISSING":
		return "90" // gray
	default:
		return "37" // white
	}
}

func colorize(enabled bool, colorCode, value string) string {
	if !enabled {
		return value
	}
	return "\033[" + colorCode + "m" + value + "\033[0m"
}

func summarizeGateResults(gates []store.GateStatusItem, colors bool) string {
	if len(gates) == 0 {
		return "no gates"
	}

	counts := make(map[string]int, 4)
	order := []string{"PASS", "FAIL", "BLOCKED", "MISSING"}
	for _, gate := range gates {
		counts[gate.Result]++
	}

	parts := make([]string, 0, len(order))
	for _, result := range order {
		if counts[result] == 0 {
			continue
		}
		label := colorize(colors, colorForGateResult(result), result)
		parts = append(parts, fmt.Sprintf("%s=%d", label, counts[result]))
	}
	if len(parts) == 0 {
		return "no results"
	}
	return strings.Join(parts, ", ")
}

func hasIncompleteRequiredGate(gates []store.GateStatusItem) bool {
	for _, gate := range gates {
		if gate.Required && gate.Result != "PASS" {
			return true
		}
	}
	return false
}
