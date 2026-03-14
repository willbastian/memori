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

type contextPacketData struct {
	Packet store.RehydratePacket `json:"packet"`
}

type contextPacketUseData struct {
	Focus      store.AgentFocus      `json:"focus"`
	Packet     store.RehydratePacket `json:"packet"`
	Idempotent bool                  `json:"idempotent"`
}
