package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/willbastian/memori/internal/store"
)

func runGateTemplate(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("gate template subcommand required: create|approve|list|pending")
	}
	switch args[0] {
	case "create":
		return runGateTemplateCreate(args[1:], out)
	case "approve":
		return runGateTemplateApprove(args[1:], out)
	case "list":
		return runGateTemplateList(args[1:], out)
	case "pending":
		return runGateTemplatePending(args[1:], out)
	default:
		return fmt.Errorf("unknown gate template subcommand %q", args[0])
	}
}

func runGateTemplateCreate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate template create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	templateID := fs.String("id", "", "template id")
	version := fs.Int("version", 0, "template version (> 0)")
	var appliesTo stringSliceFlag
	fs.Var(&appliesTo, "applies-to", "issue type this template applies to: epic|story|task|bug (repeatable)")
	filePath := fs.String("file", "", "path to JSON definition file")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "stable idempotency key")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*templateID) == "" {
		return errors.New("--id is required")
	}
	if *version <= 0 {
		return errors.New("--version must be > 0")
	}
	if strings.TrimSpace(*filePath) == "" {
		return errors.New("--file is required")
	}

	definitionBytes, err := os.ReadFile(*filePath)
	if err != nil {
		return fmt.Errorf("read --file %s: %w", *filePath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "gate-template-create", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	template, idempotent, err := s.CreateGateTemplate(ctx, store.CreateGateTemplateParams{
		TemplateID:     *templateID,
		Version:        *version,
		AppliesTo:      appliesTo,
		DefinitionJSON: string(definitionBytes),
		Actor:          identity.Actor,
		CommandID:      identity.CommandID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate template create",
			Data: gateTemplateCreateData{
				Template:   template,
				Idempotent: idempotent,
			},
		})
	}

	if idempotent {
		_, _ = fmt.Fprintf(out, "Gate template %s@%d already exists.\n", template.TemplateID, template.Version)
	} else {
		_, _ = fmt.Fprintf(out, "Created gate template %s@%d\n", template.TemplateID, template.Version)
	}
	_, _ = fmt.Fprintf(out, "Applies To: %s\n", strings.Join(template.AppliesTo, ", "))
	_, _ = fmt.Fprintf(out, "Definition Hash: %s\n", template.DefinitionHash)
	if template.Executable {
		if strings.TrimSpace(template.ApprovedBy) == "" {
			_, _ = fmt.Fprintln(out, "Approval: pending human approval before instantiate/verify")
		} else {
			_, _ = fmt.Fprintf(out, "Approval: approved by %s at %s\n", template.ApprovedBy, template.ApprovedAt)
		}
	}
	return nil
}

func runGateTemplateApprove(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate template approve", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	templateID := fs.String("id", "", "template id")
	version := fs.Int("version", 0, "template version (> 0)")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "stable idempotency key")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*templateID) == "" {
		return errors.New("--id is required")
	}
	if *version <= 0 {
		return errors.New("--version must be > 0")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "gate-template-approve", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	template, idempotent, err := s.ApproveGateTemplate(ctx, store.ApproveGateTemplateParams{
		TemplateID: *templateID,
		Version:    *version,
		Actor:      identity.Actor,
		CommandID:  identity.CommandID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate template approve",
			Data: gateTemplateApproveData{
				Template:   template,
				Idempotent: idempotent,
			},
		})
	}

	if idempotent {
		_, _ = fmt.Fprintf(out, "Gate template %s@%d is already approved.\n", template.TemplateID, template.Version)
	} else {
		_, _ = fmt.Fprintf(out, "Approved gate template %s@%d\n", template.TemplateID, template.Version)
	}
	_, _ = fmt.Fprintf(out, "Approved By: %s\n", template.ApprovedBy)
	_, _ = fmt.Fprintf(out, "Approved At: %s\n", template.ApprovedAt)
	return nil
}

func runGateTemplateList(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate template list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issueType := fs.String("type", "", "filter by issue type: epic|story|task|bug")
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

	templates, err := s.ListGateTemplates(ctx, store.ListGateTemplatesParams{IssueType: *issueType})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate template list",
			Data: gateTemplateListData{
				Count:     len(templates),
				Templates: templates,
			},
		})
	}

	if len(templates) == 0 {
		_, _ = fmt.Fprintln(out, "No gate templates matched.")
		return nil
	}
	for _, template := range templates {
		_, _ = fmt.Fprintf(
			out,
			"- %s@%d applies_to=%s hash=%s\n",
			template.TemplateID,
			template.Version,
			strings.Join(template.AppliesTo, ","),
			template.DefinitionHash,
		)
	}
	return nil
}

func runGateTemplatePending(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate template pending", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
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

	templates, err := s.ListPendingExecutableGateTemplates(ctx)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate template pending",
			Data: gateTemplateListData{
				Count:     len(templates),
				Templates: templates,
			},
		})
	}

	if len(templates) == 0 {
		_, _ = fmt.Fprintln(out, "No pending executable gate templates.")
		return nil
	}
	for _, template := range templates {
		_, _ = fmt.Fprintf(
			out,
			"- %s@%d applies_to=%s hash=%s created_by=%s created_at=%s approval=pending-human-review\n",
			template.TemplateID,
			template.Version,
			strings.Join(template.AppliesTo, ","),
			template.DefinitionHash,
			template.CreatedBy,
			template.CreatedAt,
		)
	}
	return nil
}

func gateDefinitionHasExecutableCommand(definitionJSON string) (bool, error) {
	raw := strings.TrimSpace(definitionJSON)
	if raw == "" {
		return false, errors.New("--file must contain JSON")
	}

	var parsed struct {
		Gates []struct {
			Criteria map[string]any `json:"criteria"`
		} `json:"gates"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return false, fmt.Errorf("invalid gate definition JSON: %w", err)
	}
	for _, gate := range parsed.Gates {
		command, _ := gate.Criteria["command"].(string)
		if strings.TrimSpace(command) != "" {
			return true, nil
		}
	}
	return false, nil
}

type gateTemplateCreateData struct {
	Template   store.GateTemplate `json:"template"`
	Idempotent bool               `json:"idempotent"`
}

type gateTemplateApproveData struct {
	Template   store.GateTemplate `json:"template"`
	Idempotent bool               `json:"idempotent"`
}

type gateTemplateListData struct {
	Count     int                  `json:"count"`
	Templates []store.GateTemplate `json:"templates"`
}
