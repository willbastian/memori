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

func runContextStart(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context start", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issueID := fs.String("issue", "", "issue key")
	agentID := fs.String("agent", "", "agent id to focus on the issue packet")
	sessionID := fs.String("session", "", "session id")
	trigger := fs.String("trigger", "manual", "checkpoint trigger reason")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "idempotency command id")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*issueID) == "" {
		return errors.New("--issue is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "context-start", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	result, err := startIssueContinuity(ctx, s, *issueID, *agentID, *sessionID, *trigger, identity.Actor, identity.CommandID)
	if err != nil {
		return err
	}
	session := result.Data.Session
	packet := result.Data.Packet
	focus := result.Data.Focus
	focusUsed := result.Data.FocusUsed
	focusIdempotent := result.Data.FocusIdempotent

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "context start",
			Data:                  result.Data,
		})
	}

	ui := newTextUI(out)
	if msg := sessionResolutionMessage("checkpoint", result.Resolution); msg != "" {
		ui.note(msg)
	}
	if focusUsed {
		if focusIdempotent {
			ui.note(fmt.Sprintf("Agent focus for %s already points at issue %s.", focus.AgentID, packetIssueIDForCLI(packet)))
		} else {
			ui.success(fmt.Sprintf("Started continuity for issue %s and updated focus for %s", packetIssueIDForCLI(packet), focus.AgentID))
		}
	} else {
		ui.success(fmt.Sprintf("Started continuity for issue %s", packetIssueIDForCLI(packet)))
	}
	ui.field("Session", session.SessionID)
	ui.field("Issue Packet", packet.PacketID)
	if focusUsed {
		ui.field("Agent", focus.AgentID)
	}
	steps := []string{
		fmt.Sprintf("memori issue show --key %s", packetIssueIDForCLI(packet)),
		fmt.Sprintf("memori context save --session %s", session.SessionID),
		fmt.Sprintf("memori context rehydrate --session %s", session.SessionID),
	}
	if focusUsed {
		steps = []string{
			fmt.Sprintf("memori board --agent %s", focus.AgentID),
			fmt.Sprintf("memori issue next --agent %s", focus.AgentID),
			fmt.Sprintf("memori context save --session %s", session.SessionID),
		}
	} else {
		steps = append([]string{fmt.Sprintf("memori context packet use --agent <agent-id> --packet %s", packet.PacketID)}, steps...)
	}
	ui.nextSteps(steps...)
	return nil
}

type startIssueContinuityResult struct {
	Resolution sessionResolution
	Data       contextStartData
}

func startIssueContinuity(
	ctx context.Context,
	s *store.Store,
	issueID string,
	agentID string,
	sessionID string,
	trigger string,
	actor string,
	baseCommandID string,
) (startIssueContinuityResult, error) {
	checkpointCommandID := derivedCompositeCommandID(baseCommandID, "checkpoint")
	packetCommandID := derivedCompositeCommandID(baseCommandID, "packet")
	focusCommandID := derivedCompositeCommandID(baseCommandID, "focus")

	resolution, err := resolveCheckpointSession(ctx, s, sessionID, checkpointCommandID)
	if err != nil {
		return startIssueContinuityResult{}, err
	}
	session, created, err := s.CheckpointSession(ctx, store.CheckpointSessionParams{
		SessionID: resolution.sessionID,
		IssueID:   issueID,
		Trigger:   trigger,
		Actor:     actor,
		CommandID: checkpointCommandID,
	})
	if err != nil {
		return startIssueContinuityResult{}, err
	}

	packet, err := s.BuildRehydratePacket(ctx, store.BuildPacketParams{
		Scope:     "issue",
		ScopeID:   issueID,
		Actor:     actor,
		CommandID: packetCommandID,
	})
	if err != nil {
		return startIssueContinuityResult{}, err
	}

	result := startIssueContinuityResult{
		Resolution: resolution,
		Data: contextStartData{
			Session: session,
			Created: created,
			Packet:  packet,
		},
	}
	if strings.TrimSpace(agentID) == "" {
		return result, nil
	}

	focus, packet, focusIdempotent, err := s.UseRehydratePacket(ctx, store.UsePacketParams{
		AgentID:   agentID,
		PacketID:  packet.PacketID,
		Actor:     actor,
		CommandID: focusCommandID,
	})
	if err != nil {
		return startIssueContinuityResult{}, err
	}
	result.Data.Packet = packet
	result.Data.Focus = focus
	result.Data.FocusUsed = true
	result.Data.FocusIdempotent = focusIdempotent
	return result, nil
}

func runContextSave(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context save", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	sessionID := fs.String("session", "", "session id")
	note := fs.String("note", "", "summary note")
	closeSession := fs.Bool("close", false, "close the session after saving")
	reason := fs.String("reason", "", "optional close reason when --close is set")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "context-save", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	result, err := saveIssueContinuity(ctx, s, *sessionID, "", *note, *closeSession, *reason, identity.Actor, identity.CommandID)
	if err != nil {
		return err
	}
	session := result.Data.Session
	packet := result.Data.Packet

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "context save",
			Data:                  result.Data,
		})
	}

	ui := newTextUI(out)
	if msg := sessionResolutionMessage("summarize", result.Resolution); msg != "" {
		ui.note(msg)
	}
	if *closeSession {
		ui.success(fmt.Sprintf("Saved continuity and closed session %s", session.SessionID))
		ui.field("Ended At", session.EndedAt)
	} else {
		ui.success(fmt.Sprintf("Saved continuity for session %s", session.SessionID))
	}
	ui.field("Summary Event", session.SummaryEventID)
	ui.field("Session Packet", packet.PacketID)

	steps := []string{
		fmt.Sprintf("memori context rehydrate --session %s", session.SessionID),
		fmt.Sprintf("memori context packet show --packet %s", packet.PacketID),
	}
	if *closeSession {
		steps = append(steps, "memori context checkpoint")
	} else {
		steps = append(steps, fmt.Sprintf("memori context close --session %s", session.SessionID))
	}
	ui.nextSteps(steps...)
	return nil
}

type saveIssueContinuityResult struct {
	Resolution sessionResolution
	Data       contextSaveData
}

func saveIssueContinuity(
	ctx context.Context,
	s *store.Store,
	sessionID string,
	issueID string,
	note string,
	closeSession bool,
	reason string,
	actor string,
	baseCommandID string,
) (saveIssueContinuityResult, error) {
	summarizeCommandID := derivedCompositeCommandID(baseCommandID, "summarize")
	closeCommandID := derivedCompositeCommandID(baseCommandID, "close")
	packetCommandID := derivedCompositeCommandID(baseCommandID, "packet")

	resolution, err := resolveSessionForContinuitySave(ctx, s, sessionID, issueID, summarizeCommandID)
	if err != nil {
		return saveIssueContinuityResult{}, err
	}
	session, err := s.SummarizeSession(ctx, store.SummarizeSessionParams{
		SessionID: resolution.sessionID,
		Note:      note,
		Actor:     actor,
		CommandID: summarizeCommandID,
	})
	if err != nil {
		return saveIssueContinuityResult{}, err
	}

	if closeSession {
		session, err = s.CloseSession(ctx, store.CloseSessionParams{
			SessionID: resolution.sessionID,
			Reason:    reason,
			Actor:     actor,
			CommandID: closeCommandID,
		})
		if err != nil {
			return saveIssueContinuityResult{}, err
		}
	}

	packet, err := s.BuildRehydratePacket(ctx, store.BuildPacketParams{
		Scope:     "session",
		ScopeID:   resolution.sessionID,
		Actor:     actor,
		CommandID: packetCommandID,
	})
	if err != nil {
		return saveIssueContinuityResult{}, err
	}

	return saveIssueContinuityResult{
		Resolution: resolution,
		Data: contextSaveData{
			Session: session,
			Packet:  packet,
			Closed:  closeSession,
		},
	}, nil
}

func derivedCompositeCommandID(base, suffix string) string {
	base = strings.TrimSpace(base)
	suffix = strings.TrimSpace(suffix)
	if base == "" {
		return suffix
	}
	if suffix == "" {
		return base
	}
	return base + "-" + suffix
}

type contextStartData struct {
	Session         store.Session         `json:"session"`
	Created         bool                  `json:"created"`
	Packet          store.RehydratePacket `json:"packet"`
	Focus           store.AgentFocus      `json:"focus,omitempty"`
	FocusUsed       bool                  `json:"focus_used"`
	FocusIdempotent bool                  `json:"focus_idempotent"`
}

type contextSaveData struct {
	Session store.Session         `json:"session"`
	Packet  store.RehydratePacket `json:"packet"`
	Closed  bool                  `json:"closed"`
}
