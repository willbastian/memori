package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/willbastian/memori/internal/store"
)

func runGate(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("gate subcommand required: template|set|evaluate|verify|status")
	}
	switch args[0] {
	case "template":
		return runGateTemplate(args[1:], out)
	case "set":
		return runGateSet(args[1:], out)
	case "evaluate":
		return runGateEvaluate(args[1:], out)
	case "verify":
		return runGateVerify(args[1:], out)
	case "status":
		return runGateStatus(args[1:], out)
	default:
		return fmt.Errorf("unknown gate subcommand %q", args[0])
	}
}

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

func runGateSet(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("gate set subcommand required: instantiate|lock")
	}
	switch args[0] {
	case "instantiate":
		return runGateSetInstantiate(args[1:], out)
	case "lock":
		return runGateSetLock(args[1:], out)
	default:
		return fmt.Errorf("unknown gate set subcommand %q", args[0])
	}
}

func runGateSetInstantiate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate set instantiate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issue := fs.String("issue", "", "issue key")
	var templates stringSliceFlag
	fs.Var(&templates, "template", "template ref: <template_id>@<version> (repeatable)")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "stable idempotency key")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*issue) == "" {
		return errors.New("--issue is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "gate-set-instantiate", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	templateRefs, autoSelected, err := resolveGateSetInstantiateTemplates(ctx, s, *issue, templates)
	if err != nil {
		return err
	}

	gateSet, idempotent, err := s.InstantiateGateSet(ctx, store.InstantiateGateSetParams{
		IssueID:      *issue,
		TemplateRefs: templateRefs,
		Actor:        identity.Actor,
		CommandID:    identity.CommandID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate set instantiate",
			Data: gateSetInstantiateData{
				GateSet:      gateSet,
				Idempotent:   idempotent,
				AutoSelected: autoSelected,
			},
		})
	}

	if autoSelected {
		_, _ = fmt.Fprintf(out, "Auto-selected templates: %s\n", strings.Join(gateSet.TemplateRefs, ", "))
	}
	if idempotent {
		_, _ = fmt.Fprintf(out, "Gate set already exists for issue %s cycle %d: %s\n", gateSet.IssueID, gateSet.CycleNo, gateSet.GateSetID)
	} else {
		_, _ = fmt.Fprintf(out, "Instantiated gate set %s for issue %s cycle %d\n", gateSet.GateSetID, gateSet.IssueID, gateSet.CycleNo)
	}
	_, _ = fmt.Fprintf(out, "Templates: %s\n", strings.Join(gateSet.TemplateRefs, ", "))
	_, _ = fmt.Fprintf(out, "Gate Set Hash: %s\n", gateSet.GateSetHash)
	return nil
}

func resolveGateSetInstantiateTemplates(ctx context.Context, s *store.Store, issueID string, explicit []string) ([]string, bool, error) {
	if len(explicit) > 0 {
		return explicit, false, nil
	}

	issue, err := s.GetIssue(ctx, issueID)
	if err != nil {
		return nil, false, err
	}
	templates, err := s.ListGateTemplates(ctx, store.ListGateTemplatesParams{IssueType: issue.Type})
	if err != nil {
		return nil, false, err
	}

	latestEligible := make(map[string]store.GateTemplate)
	pendingApproval := make([]string, 0)
	for _, template := range templates {
		ref := fmt.Sprintf("%s@%d", template.TemplateID, template.Version)
		if template.Executable && !isHumanGovernedActor(template.ApprovedBy) {
			pendingApproval = append(pendingApproval, ref)
			continue
		}
		current, found := latestEligible[template.TemplateID]
		if !found || template.Version > current.Version {
			latestEligible[template.TemplateID] = template
		}
	}

	if len(latestEligible) == 0 {
		sort.Strings(pendingApproval)
		if len(pendingApproval) > 0 {
			return nil, false, fmt.Errorf(
				"no eligible gate templates apply to issue type %s; pending approval: %s",
				issue.Type,
				strings.Join(pendingApproval, ", "),
			)
		}
		return nil, false, fmt.Errorf("no eligible gate templates apply to issue type %s", issue.Type)
	}

	resolved := make([]string, 0, len(latestEligible))
	for _, template := range latestEligible {
		resolved = append(resolved, fmt.Sprintf("%s@%d", template.TemplateID, template.Version))
	}
	sort.Strings(resolved)
	if len(resolved) > 1 {
		return nil, false, fmt.Errorf(
			"multiple eligible gate templates apply to issue type %s; specify --template explicitly: %s",
			issue.Type,
			strings.Join(resolved, ", "),
		)
	}

	return resolved, true, nil
}

func isHumanGovernedActor(actor string) bool {
	actor = strings.TrimSpace(strings.ToLower(actor))
	return actor != "" && !strings.HasPrefix(actor, "llm:")
}

func runGateSetLock(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate set lock", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issue := fs.String("issue", "", "issue key")
	cycle := fs.Int("cycle", 0, "issue cycle to lock (defaults to current cycle)")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "stable idempotency key")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*issue) == "" {
		return errors.New("--issue is required")
	}

	var cyclePtr *int
	if hasFlag(args, "cycle") {
		cyclePtr = cycle
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "gate-set-lock", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	gateSet, lockedNow, err := s.LockGateSet(ctx, store.LockGateSetParams{
		IssueID:   *issue,
		CycleNo:   cyclePtr,
		Actor:     identity.Actor,
		CommandID: identity.CommandID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate set lock",
			Data: gateSetLockData{
				GateSet:   gateSet,
				LockedNow: lockedNow,
			},
		})
	}

	if lockedNow {
		_, _ = fmt.Fprintf(out, "Locked gate set %s for issue %s cycle %d at %s\n", gateSet.GateSetID, gateSet.IssueID, gateSet.CycleNo, gateSet.LockedAt)
	} else {
		_, _ = fmt.Fprintf(out, "Gate set %s is already locked at %s\n", gateSet.GateSetID, gateSet.LockedAt)
	}
	return nil
}

func runGateEvaluate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate evaluate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issue := fs.String("issue", "", "issue key")
	gate := fs.String("gate", "", "gate id")
	result := fs.String("result", "", "evaluation result: PASS|FAIL|BLOCKED")
	var evidence stringSliceFlag
	fs.Var(&evidence, "evidence", "evidence reference (repeatable)")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "idempotency command id")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*issue) == "" {
		return errors.New("--issue is required")
	}
	if strings.TrimSpace(*gate) == "" {
		return errors.New("--gate is required")
	}
	if strings.TrimSpace(*result) == "" {
		return errors.New("--result is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "gate-evaluate", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	evaluation, event, idempotent, err := s.EvaluateGate(ctx, store.EvaluateGateParams{
		IssueID:      *issue,
		GateID:       *gate,
		Result:       *result,
		EvidenceRefs: evidence,
		Actor:        identity.Actor,
		CommandID:    identity.CommandID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate evaluate",
			Data: gateEvaluateData{
				Evaluation: evaluation,
				Event:      event,
				Idempotent: idempotent,
			},
		})
	}

	if idempotent {
		_, _ = fmt.Fprintf(out, "Gate evaluation already recorded for issue %s gate %s.\n", evaluation.IssueID, evaluation.GateID)
	} else {
		_, _ = fmt.Fprintf(out, "Recorded gate evaluation for issue %s gate %s: %s\n", evaluation.IssueID, evaluation.GateID, evaluation.Result)
	}
	_, _ = fmt.Fprintf(out, "Gate Set: %s\n", evaluation.GateSetID)
	_, _ = fmt.Fprintf(out, "Evaluated At: %s\n", evaluation.EvaluatedAt)
	_, _ = fmt.Fprintf(out, "Event: %s (%s #%d)\n", event.EventID, event.EventType, event.EventOrder)
	return nil
}

func runGateVerify(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate verify", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issue := fs.String("issue", "", "issue key")
	gate := fs.String("gate", "", "gate id")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "idempotency command id")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*issue) == "" {
		return errors.New("--issue is required")
	}
	if strings.TrimSpace(*gate) == "" {
		return errors.New("--gate is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "gate-verify", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	if evaluation, event, found, err := s.LookupGateEvaluationByCommand(ctx, identity.Actor, identity.CommandID); err != nil {
		return err
	} else if found {
		replay, err := gateVerifyReplayFromEvaluation(evaluation)
		if err != nil {
			return err
		}
		return printGateVerifyResult(out, dbVersion, evaluation, event, replay.Command, replay.ExitCode, replay.OutputSHA, true, *jsonOut)
	}

	spec, err := s.LookupGateVerificationSpec(ctx, *issue, *gate)
	if err != nil {
		return err
	}

	startedAt := time.Now().UTC().Format(time.RFC3339Nano)
	cmd := exec.CommandContext(ctx, "sh", "-lc", spec.Command)
	output, runErr := cmd.CombinedOutput()
	finishedAt := time.Now().UTC().Format(time.RFC3339Nano)
	exitCode := 0
	if runErr != nil {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			return fmt.Errorf("run verifier command for gate %s: %w", spec.GateID, runErr)
		}
	}
	result := "PASS"
	if exitCode != 0 {
		result = "FAIL"
	}
	outputDigest := sha256.Sum256(output)
	outputHash := hex.EncodeToString(outputDigest[:])
	evidence := []string{
		"verifier:memori-cli-gate-verify",
		"command:" + spec.Command,
		fmt.Sprintf("exit:%d", exitCode),
		"output_sha256:" + outputHash,
	}

	evaluation, event, idempotent, err := s.EvaluateGate(ctx, store.EvaluateGateParams{
		IssueID:      spec.IssueID,
		GateID:       spec.GateID,
		Result:       result,
		EvidenceRefs: evidence,
		Proof: &store.GateEvaluationProof{
			Verifier:      "memori-cli-gate-verify",
			Runner:        "sh",
			RunnerVersion: "1",
			ExitCode:      exitCode,
			StartedAt:     startedAt,
			FinishedAt:    finishedAt,
			GateSetHash:   spec.GateSetHash,
		},
		Actor:     identity.Actor,
		CommandID: identity.CommandID,
	})
	if err != nil {
		return err
	}

	if idempotent {
		replay, err := gateVerifyReplayFromEvaluation(evaluation)
		if err != nil {
			return err
		}
		return printGateVerifyResult(out, dbVersion, evaluation, event, replay.Command, replay.ExitCode, replay.OutputSHA, true, *jsonOut)
	}

	return printGateVerifyResult(out, dbVersion, evaluation, event, spec.Command, exitCode, outputHash, false, *jsonOut)
}

func runGateStatus(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issue := fs.String("issue", "", "issue key")
	cycle := fs.Int("cycle", 0, "issue cycle to inspect (defaults to current cycle)")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*issue) == "" {
		return errors.New("--issue is required")
	}
	var cyclePtr *int
	if hasFlag(args, "cycle") {
		if *cycle <= 0 {
			return errors.New("--cycle must be > 0")
		}
		cyclePtr = cycle
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	status, err := s.GetGateStatusForCycle(ctx, store.GetGateStatusParams{
		IssueID: *issue,
		CycleNo: cyclePtr,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate status",
			Data: gateStatusData{
				Status: status,
			},
		})
	}

	ui := newTextUI(out)
	ui.heading("Gate Status")
	ui.field("Issue", status.IssueID)
	ui.field("Gate Set", fmt.Sprintf("%s (cycle %d)", status.GateSetID, status.CycleNo))
	if strings.TrimSpace(status.LockedAt) != "" {
		ui.field("Locked At", status.LockedAt)
	}
	ui.field("Summary", summarizeGateResults(status.Gates, ui.colors))
	ui.blank()
	ui.section("Checks")
	for _, gate := range status.Gates {
		required := "optional"
		if gate.Required {
			required = "required"
		}
		resultValue := colorize(shouldUseColor(out), colorForGateResult(gate.Result), gate.Result)
		_, _ = fmt.Fprintf(out, "- %s [%s/%s] %s", gate.GateID, required, gate.Kind, resultValue)
		if gate.EvaluatedAt != "" {
			_, _ = fmt.Fprintf(out, " at %s", gate.EvaluatedAt)
		}
		if len(gate.EvidenceRefs) > 0 {
			_, _ = fmt.Fprintf(out, " evidence=%s", strings.Join(gate.EvidenceRefs, ","))
		}
		_, _ = fmt.Fprintln(out)
	}
	if hasIncompleteRequiredGate(status.Gates) {
		ui.nextSteps(
			fmt.Sprintf("memori gate evaluate --issue %s --gate <gate-id> --result PASS|FAIL|BLOCKED --evidence <ref>", status.IssueID),
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

type gateEvaluateData struct {
	Evaluation store.GateEvaluation `json:"evaluation"`
	Event      store.Event          `json:"event"`
	Idempotent bool                 `json:"idempotent"`
}

type gateVerifyData struct {
	Evaluation store.GateEvaluation `json:"evaluation"`
	Event      store.Event          `json:"event"`
	Command    string               `json:"command"`
	ExitCode   int                  `json:"exit_code"`
	OutputSHA  string               `json:"output_sha256"`
	Idempotent bool                 `json:"idempotent"`
}

type gateVerifyReplay struct {
	Command   string
	ExitCode  int
	OutputSHA string
}

func gateVerifyReplayFromEvaluation(evaluation store.GateEvaluation) (gateVerifyReplay, error) {
	replay := gateVerifyReplay{}
	if evaluation.Proof != nil {
		replay.ExitCode = evaluation.Proof.ExitCode
	}

	for _, ref := range evaluation.EvidenceRefs {
		if value, ok := strings.CutPrefix(ref, "command:"); ok {
			replay.Command = strings.TrimSpace(value)
			continue
		}
		if value, ok := strings.CutPrefix(ref, "output_sha256:"); ok {
			replay.OutputSHA = strings.TrimSpace(value)
			continue
		}
		if value, ok := strings.CutPrefix(ref, "exit:"); ok && evaluation.Proof == nil {
			exitCode, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				return gateVerifyReplay{}, fmt.Errorf("decode persisted verifier exit code for gate %s: %w", evaluation.GateID, err)
			}
			replay.ExitCode = exitCode
		}
	}

	if replay.Command == "" {
		return gateVerifyReplay{}, fmt.Errorf("persisted verifier command is missing for gate %s", evaluation.GateID)
	}
	if replay.OutputSHA == "" {
		return gateVerifyReplay{}, fmt.Errorf("persisted verifier output_sha256 is missing for gate %s", evaluation.GateID)
	}
	return replay, nil
}

func printGateVerifyResult(
	out io.Writer,
	dbVersion int,
	evaluation store.GateEvaluation,
	event store.Event,
	command string,
	exitCode int,
	outputSHA string,
	idempotent bool,
	jsonOut bool,
) error {
	if jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate verify",
			Data: gateVerifyData{
				Evaluation: evaluation,
				Event:      event,
				Command:    command,
				ExitCode:   exitCode,
				OutputSHA:  outputSHA,
				Idempotent: idempotent,
			},
		})
	}

	if idempotent {
		_, _ = fmt.Fprintf(out, "Gate verification already recorded for issue %s gate %s.\n", evaluation.IssueID, evaluation.GateID)
	} else {
		_, _ = fmt.Fprintf(out, "Verified gate %s for issue %s -> %s (exit=%d)\n", evaluation.GateID, evaluation.IssueID, evaluation.Result, exitCode)
	}
	_, _ = fmt.Fprintf(out, "Gate Set: %s\n", evaluation.GateSetID)
	_, _ = fmt.Fprintf(out, "Output SHA256: %s\n", outputSHA)
	return nil
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

type gateSetInstantiateData struct {
	GateSet      store.GateSet `json:"gate_set"`
	Idempotent   bool          `json:"idempotent"`
	AutoSelected bool          `json:"auto_selected"`
}

type gateSetLockData struct {
	GateSet   store.GateSet `json:"gate_set"`
	LockedNow bool          `json:"locked_now"`
}

type gateStatusData struct {
	Status store.GateStatus `json:"status"`
}
