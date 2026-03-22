package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/willbastian/memori/internal/store"
)

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

func runContextResume(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context resume", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	sessionID := fs.String("session", "", "session id")
	agentID := fs.String("agent", "", "optional agent id to focus on the resume packet")
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

	resolution, err := resolveSessionForResume(ctx, s, *sessionID, *agentID)
	if err != nil {
		return err
	}

	result, err := s.RehydrateSession(ctx, store.RehydrateSessionParams{
		SessionID: resolution.sessionID,
	})
	if err != nil {
		return err
	}

	data := contextResumeData{
		SessionID: result.SessionID,
		Source:    result.Source,
		Packet:    result.Packet,
	}
	if strings.TrimSpace(*agentID) != "" {
		identity, err := resolveMutationIdentity(ctx, s, *dbPath, "context-resume", *actor, *commandID, defaultMutationAuthDeps())
		if err != nil {
			return err
		}
		packetForFocus, err := resolveResumeFocusPacket(ctx, s, resolution.sessionID, result.Packet, identity.Actor, identity.CommandID)
		if err != nil {
			return err
		}
		focus, packet, focusIdempotent, err := s.UseRehydratePacket(ctx, store.UsePacketParams{
			AgentID:   *agentID,
			PacketID:  packetForFocus.PacketID,
			Actor:     identity.Actor,
			CommandID: identity.CommandID,
		})
		if err != nil {
			return err
		}
		if focusIdempotent && strings.TrimSpace(focus.LastPacketID) != "" && focus.LastPacketID != packet.PacketID {
			packet, err = s.GetRehydratePacket(ctx, store.GetPacketParams{PacketID: focus.LastPacketID})
			if err != nil {
				return err
			}
		}
		data.Packet = packet
		data.Focus = focus
		data.FocusUsed = true
		data.FocusIdempotent = focusIdempotent
	}
	workspace, err := resolveWorkspaceForPacket(ctx, s, data.Packet)
	if err != nil {
		return err
	}
	data.Workspace = workspace

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "context resume",
			Data:                  data,
		})
	}

	ui := newTextUI(out)
	if msg := sessionResolutionMessage("rehydrate", resolution); msg != "" {
		ui.note(msg)
	}
	if data.FocusUsed {
		if data.FocusIdempotent {
			ui.note(fmt.Sprintf("Agent focus for %s already points at packet %s.", data.Focus.AgentID, data.Packet.PacketID))
		} else {
			ui.success(fmt.Sprintf("Resumed session %s via %s and updated focus for %s", data.SessionID, data.Source, data.Focus.AgentID))
		}
	} else {
		ui.success(fmt.Sprintf("Resumed session %s via %s", data.SessionID, data.Source))
	}
	if sourceMsg := rehydrateSourceMessage(data.Source); sourceMsg != "" {
		ui.note(sourceMsg)
	}
	ui.field("Packet Scope", data.Packet.Scope)
	if strings.TrimSpace(data.Packet.PacketID) != "" {
		ui.field("Packet ID", data.Packet.PacketID)
	}
	if issueID := packetIssueIDForCLI(data.Packet); issueID != "" {
		ui.field("Issue", issueID)
	}
	if data.Workspace != nil {
		ui.field("Workspace", formatWorkspaceSummary(data.Workspace))
	}
	if data.FocusUsed {
		ui.field("Agent", data.Focus.AgentID)
		ui.nextSteps(packetUseNextSteps(data.Focus.AgentID, data.Packet.PacketID, packetIssueIDForCLI(data.Packet), packetSessionIDForCLI(data.Packet))...)
		return nil
	}
	ui.nextSteps(rehydrateNextSteps(data.SessionID, data.Source, data.Packet.PacketID, packetIssueIDForCLI(data.Packet))...)
	return nil
}

func resolveResumeFocusPacket(
	ctx context.Context,
	s *store.Store,
	sessionID string,
	resumePacket store.RehydratePacket,
	actor string,
	baseCommandID string,
) (store.RehydratePacket, error) {
	issueID, found, err := s.SessionIssueID(ctx, sessionID)
	if err != nil {
		return store.RehydratePacket{}, err
	}
	if !found {
		return store.RehydratePacket{}, fmt.Errorf("session %q not found", sessionID)
	}

	if issueID != "" {
		if strings.TrimSpace(resumePacket.PacketID) != "" &&
			strings.EqualFold(strings.TrimSpace(resumePacket.Scope), "issue") &&
			packetIssueIDForCLI(resumePacket) == issueID {
			return resumePacket, nil
		}
		return s.BuildRehydratePacket(ctx, store.BuildPacketParams{
			Scope:     "issue",
			ScopeID:   issueID,
			Actor:     actor,
			CommandID: derivedCompositeCommandID(baseCommandID, "packet"),
		})
	}

	if strings.TrimSpace(resumePacket.PacketID) != "" {
		return resumePacket, nil
	}

	scope := "session"
	scopeID := sessionID

	return s.BuildRehydratePacket(ctx, store.BuildPacketParams{
		Scope:     scope,
		ScopeID:   scopeID,
		Actor:     actor,
		CommandID: derivedCompositeCommandID(baseCommandID, "packet"),
	})
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

type contextResumeData struct {
	SessionID       string                `json:"session_id"`
	Source          string                `json:"source"`
	Packet          store.RehydratePacket `json:"packet"`
	Workspace       *workspaceContext     `json:"workspace,omitempty"`
	Focus           store.AgentFocus      `json:"focus,omitempty"`
	FocusUsed       bool                  `json:"focus_used"`
	FocusIdempotent bool                  `json:"focus_idempotent"`
}
