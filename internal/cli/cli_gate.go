package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
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

type gateEvaluateData struct {
	Evaluation store.GateEvaluation `json:"evaluation"`
	Event      store.Event          `json:"event"`
	Idempotent bool                 `json:"idempotent"`
}
