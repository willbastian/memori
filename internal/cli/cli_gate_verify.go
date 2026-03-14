package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/willbastian/memori/internal/store"
)

func runGateVerify(args []string, out io.Writer) error {
	req, err := parseGateVerifyRequest(args)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, req.dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	identity, err := resolveMutationIdentity(ctx, s, req.dbPath, "gate-verify", req.actor, req.commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	if replay, found, err := replayGateVerifyResult(ctx, s, identity.Actor, identity.CommandID); err != nil {
		return err
	} else if found {
		return printGateVerifyResult(out, dbVersion, replay.evaluation, replay.event, replay.command, replay.exitCode, replay.outputSHA, true, req.jsonOut)
	}

	spec, err := s.LookupGateVerificationSpec(ctx, req.issue, req.gate)
	if err != nil {
		return err
	}

	execution, err := executeGateVerifier(ctx, spec)
	if err != nil {
		return err
	}

	result, err := persistGateVerifyExecution(ctx, s, spec, identity, execution)
	if err != nil {
		return err
	}
	if result.idempotent {
		replay, err := gateVerifyReplayFromEvaluation(result.evaluation)
		if err != nil {
			return err
		}
		return printGateVerifyResult(out, dbVersion, result.evaluation, result.event, replay.Command, replay.ExitCode, replay.OutputSHA, true, req.jsonOut)
	}

	return printGateVerifyResult(
		out,
		dbVersion,
		result.evaluation,
		result.event,
		spec.Command,
		execution.exitCode,
		execution.outputSHA,
		false,
		req.jsonOut,
	)
}

type gateVerifyRequest struct {
	dbPath    string
	issue     string
	gate      string
	actor     string
	commandID string
	jsonOut   bool
}

func parseGateVerifyRequest(args []string) (gateVerifyRequest, error) {
	fs := flag.NewFlagSet("gate verify", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issue := fs.String("issue", "", "issue key")
	gate := fs.String("gate", "", "gate id")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "idempotency command id")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return gateVerifyRequest{}, err
	}
	if strings.TrimSpace(*issue) == "" {
		return gateVerifyRequest{}, errors.New("--issue is required")
	}
	if strings.TrimSpace(*gate) == "" {
		return gateVerifyRequest{}, errors.New("--gate is required")
	}
	return gateVerifyRequest{
		dbPath:    *dbPath,
		issue:     *issue,
		gate:      *gate,
		actor:     *actor,
		commandID: *commandID,
		jsonOut:   *jsonOut,
	}, nil
}

type gateVerifyExecution struct {
	exitCode  int
	outputSHA string
	evidence  []string
	proof     *store.GateEvaluationProof
}

func executeGateVerifier(ctx context.Context, spec store.GateVerificationSpec) (gateVerifyExecution, error) {
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
			return gateVerifyExecution{}, fmt.Errorf("run verifier command for gate %s: %w", spec.GateID, runErr)
		}
	}

	outputDigest := sha256.Sum256(output)
	outputHash := hex.EncodeToString(outputDigest[:])
	return gateVerifyExecution{
		exitCode:  exitCode,
		outputSHA: outputHash,
		evidence: []string{
			"verifier:memori-cli-gate-verify",
			"command:" + spec.Command,
			fmt.Sprintf("exit:%d", exitCode),
			"output_sha256:" + outputHash,
		},
		proof: &store.GateEvaluationProof{
			Verifier:      "memori-cli-gate-verify",
			Runner:        "sh",
			RunnerVersion: "1",
			ExitCode:      exitCode,
			StartedAt:     startedAt,
			FinishedAt:    finishedAt,
			GateSetHash:   spec.GateSetHash,
		},
	}, nil
}

type persistedGateVerifyResult struct {
	evaluation store.GateEvaluation
	event      store.Event
	idempotent bool
}

func persistGateVerifyExecution(
	ctx context.Context,
	s *store.Store,
	spec store.GateVerificationSpec,
	identity mutationIdentity,
	execution gateVerifyExecution,
) (persistedGateVerifyResult, error) {
	result := "PASS"
	if execution.exitCode != 0 {
		result = "FAIL"
	}

	evaluation, event, idempotent, err := s.EvaluateGate(ctx, store.EvaluateGateParams{
		IssueID:      spec.IssueID,
		GateID:       spec.GateID,
		Result:       result,
		EvidenceRefs: execution.evidence,
		Proof:        execution.proof,
		Actor:        identity.Actor,
		CommandID:    identity.CommandID,
	})
	if err != nil {
		return persistedGateVerifyResult{}, err
	}

	return persistedGateVerifyResult{
		evaluation: evaluation,
		event:      event,
		idempotent: idempotent,
	}, nil
}

type replayedGateVerifyResult struct {
	evaluation store.GateEvaluation
	event      store.Event
	command    string
	exitCode   int
	outputSHA  string
}

func replayGateVerifyResult(ctx context.Context, s *store.Store, actor, commandID string) (replayedGateVerifyResult, bool, error) {
	evaluation, event, found, err := s.LookupGateEvaluationByCommand(ctx, actor, commandID)
	if err != nil {
		return replayedGateVerifyResult{}, false, err
	}
	if !found {
		return replayedGateVerifyResult{}, false, nil
	}

	replay, err := gateVerifyReplayFromEvaluation(evaluation)
	if err != nil {
		return replayedGateVerifyResult{}, false, err
	}
	return replayedGateVerifyResult{
		evaluation: evaluation,
		event:      event,
		command:    replay.Command,
		exitCode:   replay.ExitCode,
		outputSHA:  replay.OutputSHA,
	}, true, nil
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
