package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/willbastian/memori/internal/store"
)

type sessionResolution struct {
	sessionID string
	source    string
}

func resolveCheckpointSession(ctx context.Context, s *store.Store, explicitSessionID, commandID string) (sessionResolution, error) {
	if sessionID := strings.TrimSpace(explicitSessionID); sessionID != "" {
		return sessionResolution{sessionID: sessionID, source: "explicit"}, nil
	}
	if session, found, err := s.SessionForCommand(ctx, commandID); err != nil {
		return sessionResolution{}, err
	} else if found {
		return sessionResolution{sessionID: session.SessionID, source: "command-replay"}, nil
	}
	if session, found, err := s.LatestOpenSession(ctx); err != nil {
		return sessionResolution{}, err
	} else if found {
		return sessionResolution{sessionID: session.SessionID, source: "latest-open"}, nil
	}
	return sessionResolution{
		sessionID: generatedSessionIDForCommand(commandID),
		source:    "generated-new",
	}, nil
}

func resolveCheckpointSessionForIssue(ctx context.Context, s *store.Store, explicitSessionID, issueID, commandID string) (sessionResolution, error) {
	if sessionID := strings.TrimSpace(explicitSessionID); sessionID != "" {
		return sessionResolution{sessionID: sessionID, source: "explicit"}, nil
	}
	if session, found, err := s.SessionForCommand(ctx, commandID); err != nil {
		return sessionResolution{}, err
	} else if found {
		return sessionResolution{sessionID: session.SessionID, source: "command-replay"}, nil
	}
	if session, found, err := s.LatestOpenSessionForIssue(ctx, issueID); err != nil {
		return sessionResolution{}, err
	} else if found {
		return sessionResolution{sessionID: session.SessionID, source: "latest-open-issue"}, nil
	}
	return sessionResolution{
		sessionID: generatedSessionIDForCommand(commandID),
		source:    "generated-new",
	}, nil
}

func resolveOpenSessionForMutation(ctx context.Context, s *store.Store, explicitSessionID, commandID string) (sessionResolution, error) {
	if sessionID := strings.TrimSpace(explicitSessionID); sessionID != "" {
		return sessionResolution{sessionID: sessionID, source: "explicit"}, nil
	}
	if session, found, err := s.SessionForCommand(ctx, commandID); err != nil {
		return sessionResolution{}, err
	} else if found {
		return sessionResolution{sessionID: session.SessionID, source: "command-replay"}, nil
	}
	if session, found, err := s.LatestOpenSession(ctx); err != nil {
		return sessionResolution{}, err
	} else if found {
		return sessionResolution{sessionID: session.SessionID, source: "latest-open"}, nil
	}
	return sessionResolution{}, fmt.Errorf("no open session found; start one with `memori context checkpoint` or pass --session <id>")
}

func resolveOpenSessionForIssueMutation(ctx context.Context, s *store.Store, issueID, commandID string) (sessionResolution, error) {
	if session, found, err := s.SessionForCommand(ctx, commandID); err != nil {
		return sessionResolution{}, err
	} else if found {
		return sessionResolution{sessionID: session.SessionID, source: "command-replay"}, nil
	}
	if session, found, err := s.LatestOpenSessionForIssue(ctx, issueID); err != nil {
		return sessionResolution{}, err
	} else if found {
		return sessionResolution{sessionID: session.SessionID, source: "latest-open-issue"}, nil
	}
	return sessionResolution{}, fmt.Errorf("no open session found for issue %s; start work first with `memori issue update --key %s --status inprogress` or pass --skip-continuity", strings.TrimSpace(issueID), strings.TrimSpace(issueID))
}

func latestOpenSessionIDForIssue(ctx context.Context, s *store.Store, issueID string) (string, bool, error) {
	issueID = strings.TrimSpace(issueID)
	if issueID == "" {
		return "", false, nil
	}
	session, found, err := s.LatestOpenSessionForIssue(ctx, issueID)
	if err != nil {
		return "", false, err
	}
	if !found {
		return "", false, nil
	}
	return session.SessionID, true, nil
}

func resolveSessionForContinuitySave(ctx context.Context, s *store.Store, explicitSessionID, issueID, commandID string) (sessionResolution, error) {
	if strings.TrimSpace(explicitSessionID) != "" {
		return resolveOpenSessionForMutation(ctx, s, explicitSessionID, commandID)
	}
	if strings.TrimSpace(issueID) != "" {
		return resolveOpenSessionForIssueMutation(ctx, s, issueID, commandID)
	}
	return resolveOpenSessionForMutation(ctx, s, "", commandID)
}

func resolveSessionForRehydrate(ctx context.Context, s *store.Store, explicitSessionID string) (sessionResolution, error) {
	if sessionID := strings.TrimSpace(explicitSessionID); sessionID != "" {
		return sessionResolution{sessionID: sessionID, source: "explicit"}, nil
	}
	if session, found, err := s.LatestOpenSession(ctx); err != nil {
		return sessionResolution{}, err
	} else if found {
		return sessionResolution{sessionID: session.SessionID, source: "latest-open"}, nil
	}
	if session, found, err := s.LatestSession(ctx); err != nil {
		return sessionResolution{}, err
	} else if found {
		return sessionResolution{sessionID: session.SessionID, source: "latest-session"}, nil
	}
	return sessionResolution{}, fmt.Errorf("no session found; start one with `memori context checkpoint` or pass --session <id>")
}

func resolveSessionForResume(ctx context.Context, s *store.Store, explicitSessionID, agentID string) (sessionResolution, error) {
	if sessionID := strings.TrimSpace(explicitSessionID); sessionID != "" {
		return sessionResolution{sessionID: sessionID, source: "explicit"}, nil
	}
	if resolution, found, err := resolveSessionFromAgentFocus(ctx, s, agentID); err != nil {
		return sessionResolution{}, err
	} else if found {
		return resolution, nil
	}
	return resolveSessionForRehydrate(ctx, s, "")
}

func resolveSessionFromAgentFocus(ctx context.Context, s *store.Store, agentID string) (sessionResolution, bool, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return sessionResolution{}, false, nil
	}

	focus, found, err := s.GetAgentFocus(ctx, agentID)
	if err != nil {
		return sessionResolution{}, false, err
	}
	if !found {
		return sessionResolution{}, false, nil
	}

	if packetID := strings.TrimSpace(focus.LastPacketID); packetID != "" {
		packet, err := s.GetRehydratePacket(ctx, store.GetPacketParams{PacketID: packetID})
		if err != nil {
			return sessionResolution{}, false, err
		}
		if sessionID := strings.TrimSpace(packet.SessionID); sessionID != "" {
			return sessionResolution{sessionID: sessionID, source: "agent-focus-session"}, true, nil
		}
		if issueID := strings.TrimSpace(packet.IssueID); issueID != "" {
			if session, found, err := s.LatestOpenSessionForIssue(ctx, issueID); err != nil {
				return sessionResolution{}, false, err
			} else if found {
				return sessionResolution{sessionID: session.SessionID, source: "agent-focus-issue-open"}, true, nil
			}
			if session, found, err := s.LatestSessionForIssue(ctx, issueID); err != nil {
				return sessionResolution{}, false, err
			} else if found {
				return sessionResolution{sessionID: session.SessionID, source: "agent-focus-issue-latest"}, true, nil
			}
		}
	}

	if issueID := strings.TrimSpace(focus.ActiveIssueID); issueID != "" {
		if session, found, err := s.LatestOpenSessionForIssue(ctx, issueID); err != nil {
			return sessionResolution{}, false, err
		} else if found {
			return sessionResolution{sessionID: session.SessionID, source: "agent-focus-issue-open"}, true, nil
		}
		if session, found, err := s.LatestSessionForIssue(ctx, issueID); err != nil {
			return sessionResolution{}, false, err
		} else if found {
			return sessionResolution{sessionID: session.SessionID, source: "agent-focus-issue-latest"}, true, nil
		}
	}

	return sessionResolution{}, false, nil
}

func generatedSessionIDForCommand(commandID string) string {
	commandID = strings.TrimSpace(commandID)
	if commandID == "" {
		return "sess-manual"
	}
	sum := sha256.Sum256([]byte(commandID))
	return fmt.Sprintf("sess_%s", hex.EncodeToString(sum[:])[:12])
}

func sessionResolutionMessage(action string, resolution sessionResolution) string {
	action = strings.TrimSpace(action)
	switch resolution.source {
	case "command-replay":
		return fmt.Sprintf("Command replay detected; reusing session %s.", resolution.sessionID)
	case "latest-open":
		switch action {
		case "checkpoint":
			return fmt.Sprintf("No --session supplied; continuing latest open session %s. Pass --session to start a parallel session.", resolution.sessionID)
		case "summarize":
			return fmt.Sprintf("No --session supplied; summarizing latest open session %s.", resolution.sessionID)
		case "close":
			return fmt.Sprintf("No --session supplied; closing latest open session %s.", resolution.sessionID)
		case "rehydrate":
			return fmt.Sprintf("No --session supplied; rehydrating latest open session %s.", resolution.sessionID)
		}
	case "latest-open-issue":
		if action == "checkpoint" {
			return fmt.Sprintf("No --session supplied; continuing the open session already tracking this issue (%s).", resolution.sessionID)
		}
		if action == "summarize" {
			return fmt.Sprintf("No --session supplied; summarizing the latest open session for this issue (%s).", resolution.sessionID)
		}
	case "agent-focus-session":
		return fmt.Sprintf("No --session supplied; rehydrating the session currently associated with this agent's saved focus (%s).", resolution.sessionID)
	case "agent-focus-issue-open":
		return fmt.Sprintf("No --session supplied; rehydrating the latest open session for this agent's focused issue (%s).", resolution.sessionID)
	case "agent-focus-issue-latest":
		return fmt.Sprintf("No --session supplied; rehydrating the latest session for this agent's focused issue (%s).", resolution.sessionID)
	case "latest-session":
		return fmt.Sprintf("No --session supplied; rehydrating latest session %s.", resolution.sessionID)
	case "generated-new":
		return fmt.Sprintf("No --session supplied; started new session %s.", resolution.sessionID)
	}
	return ""
}
