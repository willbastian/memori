package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/willbastian/memori/internal/store"
)

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

type gateSetInstantiateData struct {
	GateSet      store.GateSet `json:"gate_set"`
	Idempotent   bool          `json:"idempotent"`
	AutoSelected bool          `json:"auto_selected"`
}

type gateSetLockData struct {
	GateSet   store.GateSet `json:"gate_set"`
	LockedNow bool          `json:"locked_now"`
}
