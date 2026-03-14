package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/willbastian/memori/internal/store"
)

func runContext(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("context subcommand required: checkpoint|summarize|close|rehydrate|packet|loops")
	}
	switch args[0] {
	case "checkpoint":
		return runContextCheckpoint(args[1:], out)
	case "summarize":
		return runContextSummarize(args[1:], out)
	case "close":
		return runContextClose(args[1:], out)
	case "rehydrate":
		return runContextRehydrate(args[1:], out)
	case "packet":
		return runContextPacket(args[1:], out)
	case "loops":
		return runContextLoops(args[1:], out)
	default:
		return fmt.Errorf("unknown context subcommand %q", args[0])
	}
}

func runContextCheckpoint(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context checkpoint", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	sessionID := fs.String("session", "", "session id")
	trigger := fs.String("trigger", "manual", "checkpoint trigger reason")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "context-checkpoint", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	resolution, err := resolveCheckpointSession(ctx, s, *sessionID, identity.CommandID)
	if err != nil {
		return err
	}

	session, created, err := s.CheckpointSession(ctx, store.CheckpointSessionParams{
		SessionID: resolution.sessionID,
		Trigger:   *trigger,
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
			Command:               "context checkpoint",
			Data: contextCheckpointData{
				Session: session,
				Created: created,
			},
		})
	}

	ui := newTextUI(out)
	if msg := sessionResolutionMessage("checkpoint", resolution); msg != "" {
		ui.note(msg)
	}
	if created {
		ui.success(fmt.Sprintf("Created session checkpoint %s", session.SessionID))
	} else {
		ui.success(fmt.Sprintf("Updated session checkpoint %s", session.SessionID))
	}
	ui.field("Trigger", session.Trigger)
	ui.nextSteps(
		fmt.Sprintf("memori context rehydrate --session %s", session.SessionID),
		fmt.Sprintf("memori context summarize --session %s", session.SessionID),
		fmt.Sprintf("memori context packet build --scope session --id %s", session.SessionID),
	)
	return nil
}

func runContextSummarize(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context summarize", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	sessionID := fs.String("session", "", "session id")
	note := fs.String("note", "", "summary note")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "context-summarize", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	resolution, err := resolveOpenSessionForMutation(ctx, s, *sessionID, identity.CommandID)
	if err != nil {
		return err
	}

	session, err := s.SummarizeSession(ctx, store.SummarizeSessionParams{
		SessionID: resolution.sessionID,
		Note:      *note,
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
			Command:               "context summarize",
			Data: contextSessionData{
				Session: session,
			},
		})
	}

	ui := newTextUI(out)
	if msg := sessionResolutionMessage("summarize", resolution); msg != "" {
		ui.note(msg)
	}
	ui.success(fmt.Sprintf("Summarized session %s", session.SessionID))
	ui.field("Summary Event", session.SummaryEventID)
	ui.nextSteps(
		fmt.Sprintf("memori context rehydrate --session %s", session.SessionID),
		fmt.Sprintf("memori context packet build --scope session --id %s", session.SessionID),
		fmt.Sprintf("memori context close --session %s", session.SessionID),
	)
	return nil
}

func runContextClose(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context close", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	sessionID := fs.String("session", "", "session id")
	reason := fs.String("reason", "", "close reason")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "context-close", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	resolution, err := resolveOpenSessionForMutation(ctx, s, *sessionID, identity.CommandID)
	if err != nil {
		return err
	}

	session, err := s.CloseSession(ctx, store.CloseSessionParams{
		SessionID: resolution.sessionID,
		Reason:    *reason,
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
			Command:               "context close",
			Data: contextSessionData{
				Session: session,
			},
		})
	}

	ui := newTextUI(out)
	if msg := sessionResolutionMessage("close", resolution); msg != "" {
		ui.note(msg)
	}
	ui.success(fmt.Sprintf("Closed session %s", session.SessionID))
	ui.field("Ended At", session.EndedAt)
	ui.nextSteps(
		fmt.Sprintf("memori context rehydrate --session %s", session.SessionID),
		fmt.Sprintf("memori context packet build --scope session --id %s", session.SessionID),
		"memori context checkpoint",
	)
	return nil
}

func runContextRehydrate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context rehydrate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	sessionID := fs.String("session", "", "session id")
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

	resolution, err := resolveSessionForRehydrate(ctx, s, *sessionID)
	if err != nil {
		return err
	}

	result, err := s.RehydrateSession(ctx, store.RehydrateSessionParams{
		SessionID: resolution.sessionID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "context rehydrate",
			Data: contextRehydrateData{
				SessionID: result.SessionID,
				Source:    result.Source,
				Packet:    result.Packet,
			},
		})
	}

	ui := newTextUI(out)
	if msg := sessionResolutionMessage("rehydrate", resolution); msg != "" {
		ui.note(msg)
	}
	ui.success(fmt.Sprintf("Rehydrated session %s via %s", result.SessionID, result.Source))
	if sourceMsg := rehydrateSourceMessage(result.Source); sourceMsg != "" {
		ui.note(sourceMsg)
	}
	ui.field("Packet Scope", result.Packet.Scope)
	if strings.TrimSpace(result.Packet.PacketID) != "" {
		ui.field("Packet ID", result.Packet.PacketID)
	}
	if issueID := packetIssueIDForCLI(result.Packet); issueID != "" {
		ui.field("Issue", issueID)
	}
	ui.nextSteps(rehydrateNextSteps(result.SessionID, result.Source, result.Packet.PacketID, packetIssueIDForCLI(result.Packet))...)
	return nil
}

func runContextPacket(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("context packet subcommand required: build|show|use")
	}
	switch args[0] {
	case "build":
		return runContextPacketBuild(args[1:], out)
	case "show":
		return runContextPacketShow(args[1:], out)
	case "use":
		return runContextPacketUse(args[1:], out)
	default:
		return fmt.Errorf("unknown context packet subcommand %q", args[0])
	}
}

func runContextPacketBuild(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context packet build", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	scope := fs.String("scope", "", "packet scope: issue|session")
	scopeID := fs.String("id", "", "scope id")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "context-packet-build", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	packet, err := s.BuildRehydratePacket(ctx, store.BuildPacketParams{
		Scope:     *scope,
		ScopeID:   *scopeID,
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
			Command:               "context packet build",
			Data: contextPacketData{
				Packet: packet,
			},
		})
	}

	ui := newTextUI(out)
	ui.success(fmt.Sprintf("Built packet %s (%s)", packet.PacketID, packet.Scope))
	if issueID := packetIssueIDForCLI(packet); issueID != "" {
		ui.field("Issue", issueID)
	}
	if packet.Scope == "session" {
		ui.field("Session", packetSessionIDForCLI(packet))
	}
	ui.nextSteps(packetBuildNextSteps(packet.PacketID, packet.Scope, packetIssueIDForCLI(packet), packetSessionIDForCLI(packet))...)
	return nil
}

func runContextPacketShow(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context packet show", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	packetID := fs.String("packet", "", "packet id")
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

	packet, err := s.GetRehydratePacket(ctx, store.GetPacketParams{PacketID: *packetID})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "context packet show",
			Data: contextPacketData{
				Packet: packet,
			},
		})
	}

	_, _ = fmt.Fprintf(out, "Packet %s (%s)\n", packet.PacketID, packet.Scope)
	return nil
}

func runContextPacketUse(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context packet use", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	agentID := fs.String("agent", "", "agent id")
	packetID := fs.String("packet", "", "packet id")
	actor := fs.String("actor", "", "actor id (optional)")
	commandID := fs.String("command-id", "", "command id (optional; requires MEMORI_ALLOW_MANUAL_COMMAND_ID=1)")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "context-packet-use", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	focus, packet, idempotent, err := s.UseRehydratePacket(ctx, store.UsePacketParams{
		AgentID:   *agentID,
		PacketID:  *packetID,
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
			Command:               "context packet use",
			Data: contextPacketUseData{
				Focus:      focus,
				Packet:     packet,
				Idempotent: idempotent,
			},
		})
	}

	ui := newTextUI(out)
	if idempotent {
		ui.note(fmt.Sprintf("Agent focus for %s already points at packet %s.", focus.AgentID, packet.PacketID))
	} else {
		ui.success(fmt.Sprintf("Updated agent focus for %s using packet %s", focus.AgentID, packet.PacketID))
	}
	ui.field("Packet Scope", packet.Scope)
	if strings.TrimSpace(focus.ActiveIssueID) != "" {
		ui.field("Active Issue", focus.ActiveIssueID)
	}
	if focus.ActiveCycleNo > 0 {
		ui.field("Active Cycle", strconv.Itoa(focus.ActiveCycleNo))
	}
	if packet.Scope == "session" {
		ui.field("Session", packetSessionIDForCLI(packet))
	}
	ui.nextSteps(packetUseNextSteps(focus.AgentID, packet.PacketID, focus.ActiveIssueID, packetSessionIDForCLI(packet))...)
	return nil
}

func runContextLoops(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context loops", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issue := fs.String("issue", "", "optional issue key filter")
	cycle := fs.Int("cycle", 0, "optional cycle filter (> 0)")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
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

	loops, err := s.ListOpenLoops(ctx, store.ListOpenLoopsParams{
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
			Command:               "context loops",
			Data: contextLoopsData{
				Count: len(loops),
				Loops: loops,
			},
		})
	}

	if len(loops) == 0 {
		_, _ = fmt.Fprintln(out, "No context loops matched the filters.")
		return nil
	}
	for _, loop := range loops {
		_, _ = fmt.Fprintf(out, "- %s [%s/%s] issue=%s cycle=%d", loop.LoopID, loop.LoopType, loop.Status, loop.IssueID, loop.CycleNo)
		if strings.TrimSpace(loop.Priority) != "" {
			_, _ = fmt.Fprintf(out, " priority=%s", loop.Priority)
		}
		if strings.TrimSpace(loop.SourceEventID) != "" {
			_, _ = fmt.Fprintf(out, " source=%s", loop.SourceEventID)
		}
		_, _ = fmt.Fprintln(out)
	}
	return nil
}

type contextCheckpointData struct {
	Session store.Session `json:"session"`
	Created bool          `json:"created"`
}

type contextSessionData struct {
	Session store.Session `json:"session"`
}

type contextRehydrateData struct {
	SessionID string                `json:"session_id"`
	Source    string                `json:"source"`
	Packet    store.RehydratePacket `json:"packet"`
}

type contextPacketData struct {
	Packet store.RehydratePacket `json:"packet"`
}

type contextPacketUseData struct {
	Focus      store.AgentFocus      `json:"focus"`
	Packet     store.RehydratePacket `json:"packet"`
	Idempotent bool                  `json:"idempotent"`
}

type contextLoopsData struct {
	Count int              `json:"count"`
	Loops []store.OpenLoop `json:"loops"`
}
