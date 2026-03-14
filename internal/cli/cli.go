package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/willbastian/memori/internal/store"
)

func runEvent(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("event subcommand required: log")
	}
	if args[0] != "log" {
		return fmt.Errorf("unknown event subcommand %q", args[0])
	}

	fs := flag.NewFlagSet("event log", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	entity := fs.String("entity", "", "entity reference: entityType:id or id (defaults to issue)")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if strings.TrimSpace(*entity) == "" {
		return errors.New("--entity is required")
	}

	entityType, entityID, err := parseEntityRef(*entity)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	events, err := s.ListEventsForEntity(ctx, entityType, entityID)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "event log",
			Data: eventLogData{
				EntityType: entityType,
				EntityID:   entityID,
				Events:     events,
			},
		})
	}

	if len(events) == 0 {
		_, _ = fmt.Fprintf(out, "No events for %s:%s\n", entityType, entityID)
		return nil
	}
	_, _ = fmt.Fprintf(out, "Events for %s:%s\n", entityType, entityID)
	for _, event := range events {
		line := fmt.Sprintf("- #%d %s %s actor=%s command_id=%s", event.EventOrder, event.EventType, event.CreatedAt, event.Actor, event.CommandID)
		if strings.TrimSpace(event.CausationID) != "" {
			line += " causation_id=" + event.CausationID
		}
		if strings.TrimSpace(event.CorrelationID) != "" {
			line += " correlation_id=" + event.CorrelationID
		}
		_, _ = fmt.Fprintln(out, line)
	}
	return nil
}

func parseEntityRef(raw string) (entityType, entityID string, err error) {
	parts := strings.SplitN(strings.TrimSpace(raw), ":", 2)
	if len(parts) == 1 {
		if parts[0] == "" {
			return "", "", errors.New("entity id cannot be empty")
		}
		return "issue", parts[0], nil
	}
	if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("invalid entity reference %q", raw)
	}
	entityType = strings.ToLower(strings.TrimSpace(parts[0]))
	entityID = strings.TrimSpace(parts[1])
	switch entityType {
	case "issue", "session", "packet", "focus":
		return entityType, entityID, nil
	case "gate-template", "gate_template":
		return "gate_template", entityID, nil
	case "gate-set", "gate_set":
		return "gate_set", entityID, nil
	default:
		return "", "", fmt.Errorf("invalid entity type %q (expected issue|session|packet|focus|gate-template|gate-set)", parts[0])
	}
}

type eventLogData struct {
	EntityType string        `json:"entity_type"`
	EntityID   string        `json:"entity_id"`
	Events     []store.Event `json:"events"`
}

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
