package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"memori/internal/store"
)

const responseSchemaVersion = 1

func Run(args []string, stdout, stderr io.Writer) error {
	_ = stderr
	if len(args) == 0 {
		printHelp(stdout)
		return nil
	}

	switch args[0] {
	case "help", "--help", "-h":
		return runHelp(args[1:], stdout)
	case "init":
		return runInit(args[1:], stdout)
	case "issue":
		return runIssue(args[1:], stdout)
	case "event":
		return runEvent(args[1:], stdout)
	case "db":
		return runDB(args[1:], stdout)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runHelp(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("help", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *jsonOut {
		commands := []string{
			"memori help [--json]",
			"memori init [--db <path>] [--json]",
			"memori issue create --type epic|story|task|bug --title <title> [--parent <id>] [--id <id>] [--actor <actor>] [--command-id <id>] [--json]",
			"memori issue show --id <id> [--json]",
			"memori event log --entity <entityType:id|id> [--json]",
			"memori db replay [--json]",
		}
		sort.Strings(commands)
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       0,
			Command:               "help",
			Data:                  helpData{Commands: commands},
		})
	}

	printHelp(out)
	return nil
}

func runInit(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	if err := s.Initialize(ctx); err != nil {
		return err
	}

	dbVersion, err := s.SchemaVersion(ctx)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "init",
			Data: initData{
				DBPath: *dbPath,
				Status: "initialized",
			},
		})
	}

	_, _ = fmt.Fprintf(out, "Initialized memori database at %s (schema v%d)\n", *dbPath, dbVersion)
	return nil
}

func runIssue(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("issue subcommand required: create|show")
	}

	switch args[0] {
	case "create":
		return runIssueCreate(args[1:], out)
	case "show":
		return runIssueShow(args[1:], out)
	default:
		return fmt.Errorf("unknown issue subcommand %q", args[0])
	}
}

func runIssueCreate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("issue create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issueType := fs.String("type", "", "issue type: epic|story|task|bug")
	title := fs.String("title", "", "issue title")
	parent := fs.String("parent", "", "parent issue id")
	id := fs.String("id", "", "explicit issue id (optional)")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "idempotency command id")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	issue, event, idempotent, err := s.CreateIssue(ctx, store.CreateIssueParams{
		IssueID:   *id,
		Type:      *issueType,
		Title:     *title,
		ParentID:  *parent,
		Actor:     *actor,
		CommandID: *commandID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "issue create",
			Data: issueCreateData{
				Issue:      issue,
				Event:      event,
				Idempotent: idempotent,
			},
		})
	}

	if idempotent {
		_, _ = fmt.Fprintf(out, "Issue %s already exists from previous command replay.\n", issue.ID)
	} else {
		_, _ = fmt.Fprintf(out, "Created issue %s (%s): %s\n", issue.ID, issue.Type, issue.Title)
	}
	_, _ = fmt.Fprintf(out, "Status: %s\n", issue.Status)
	_, _ = fmt.Fprintf(out, "Event: %s (%s #%d)\n", event.EventID, event.EventType, event.EventOrder)
	return nil
}

func runIssueShow(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("issue show", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	id := fs.String("id", "", "issue id")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*id) == "" {
		return errors.New("--id is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	issue, err := s.GetIssue(ctx, *id)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "issue show",
			Data:                  issueShowData{Issue: issue},
		})
	}

	_, _ = fmt.Fprintf(out, "ID: %s\n", issue.ID)
	_, _ = fmt.Fprintf(out, "Type: %s\n", issue.Type)
	_, _ = fmt.Fprintf(out, "Title: %s\n", issue.Title)
	if issue.ParentID != "" {
		_, _ = fmt.Fprintf(out, "Parent: %s\n", issue.ParentID)
	}
	_, _ = fmt.Fprintf(out, "Status: %s\n", issue.Status)
	_, _ = fmt.Fprintf(out, "Created: %s\n", issue.CreatedAt)
	_, _ = fmt.Fprintf(out, "Updated: %s\n", issue.UpdatedAt)
	return nil
}

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
		_, _ = fmt.Fprintf(out, "- #%d %s %s actor=%s command_id=%s\n", event.EventOrder, event.EventType, event.CreatedAt, event.Actor, event.CommandID)
	}
	return nil
}

func runDB(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("db subcommand required: replay")
	}

	switch args[0] {
	case "replay":
		return runDBReplay(args[1:], out)
	default:
		return fmt.Errorf("unknown db subcommand %q", args[0])
	}
}

func runDBReplay(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("db replay", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	result, err := s.ReplayProjections(ctx)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "db replay",
			Data:                  result,
		})
	}

	_, _ = fmt.Fprintf(out, "Rebuilt projections from %d events\n", result.EventsApplied)
	return nil
}

func defaultDBPath() string {
	if fromEnv := strings.TrimSpace(os.Getenv("MEMORI_DB_PATH")); fromEnv != "" {
		return fromEnv
	}
	return ".memori/memori.db"
}

func openInitializedStore(ctx context.Context, dbPath string) (*store.Store, int, error) {
	s, err := store.Open(dbPath)
	if err != nil {
		return nil, 0, err
	}
	v, err := s.SchemaVersion(ctx)
	if err != nil {
		_ = s.Close()
		return nil, 0, err
	}
	if v == 0 {
		_ = s.Close()
		return nil, 0, fmt.Errorf("database is not initialized at %s (run: memori init --db %s)", dbPath, dbPath)
	}
	return s, v, nil
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
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}

type jsonEnvelope struct {
	ResponseSchemaVersion int    `json:"response_schema_version"`
	DBSchemaVersion       int    `json:"db_schema_version"`
	Command               string `json:"command"`
	Data                  any    `json:"data"`
}

type helpData struct {
	Commands []string `json:"commands"`
}

type initData struct {
	DBPath string `json:"db_path"`
	Status string `json:"status"`
}

type issueCreateData struct {
	Issue      store.Issue `json:"issue"`
	Event      store.Event `json:"event"`
	Idempotent bool        `json:"idempotent"`
}

type issueShowData struct {
	Issue store.Issue `json:"issue"`
}

type eventLogData struct {
	EntityType string        `json:"entity_type"`
	EntityID   string        `json:"entity_id"`
	Events     []store.Event `json:"events"`
}

func printJSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func printHelp(out io.Writer) {
	_, _ = fmt.Fprintln(out, "memori - local context bridge + agile issue ledger")
	_, _ = fmt.Fprintln(out, "")
	_, _ = fmt.Fprintln(out, "Commands:")
	_, _ = fmt.Fprintln(out, "  memori help [--json]")
	_, _ = fmt.Fprintln(out, "  memori init [--db <path>] [--json]")
	_, _ = fmt.Fprintln(out, "  memori issue create --type epic|story|task|bug --title <title> [--parent <id>] [--id <id>] [--actor <actor>] [--command-id <id>] [--json]")
	_, _ = fmt.Fprintln(out, "  memori issue show --id <id> [--json]")
	_, _ = fmt.Fprintln(out, "  memori event log --entity <entityType:id|id> [--json]")
	_, _ = fmt.Fprintln(out, "  memori db replay [--json]")
}
