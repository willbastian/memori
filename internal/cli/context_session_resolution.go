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
	case "latest-session":
		return fmt.Sprintf("No --session supplied; rehydrating latest session %s.", resolution.sessionID)
	case "generated-new":
		return fmt.Sprintf("No --session supplied; started new session %s.", resolution.sessionID)
	}
	return ""
}
