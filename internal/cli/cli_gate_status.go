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

type gateStatusData struct {
	Status store.GateStatus `json:"status"`
}
