package cli

import (
	"strings"
	"testing"
)

type contextCheckpointEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Created bool `json:"created"`
		Session struct {
			SessionID string `json:"session_id"`
		} `json:"session"`
	} `json:"data"`
}

type contextStartEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Created bool `json:"created"`
		Session struct {
			SessionID string `json:"session_id"`
		} `json:"session"`
		Packet struct {
			PacketID string `json:"packet_id"`
			Scope    string `json:"scope"`
		} `json:"packet"`
		Focus struct {
			AgentID       string `json:"agent_id"`
			ActiveIssueID string `json:"active_issue_id"`
			LastPacketID  string `json:"last_packet_id"`
		} `json:"focus"`
		FocusUsed       bool `json:"focus_used"`
		FocusIdempotent bool `json:"focus_idempotent"`
	} `json:"data"`
}

type contextSessionEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Session struct {
			SessionID      string `json:"session_id"`
			EndedAt        string `json:"ended_at"`
			SummaryEventID string `json:"summary_event_id"`
		} `json:"session"`
	} `json:"data"`
}

type contextSaveEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Closed  bool `json:"closed"`
		Session struct {
			SessionID      string `json:"session_id"`
			EndedAt        string `json:"ended_at"`
			SummaryEventID string `json:"summary_event_id"`
		} `json:"session"`
		Packet struct {
			PacketID string `json:"packet_id"`
			Scope    string `json:"scope"`
		} `json:"packet"`
	} `json:"data"`
}

type contextPacketEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Packet struct {
			PacketID string `json:"packet_id"`
			Scope    string `json:"scope"`
		} `json:"packet"`
	} `json:"data"`
}

type contextPacketUseEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Focus struct {
			AgentID       string `json:"agent_id"`
			LastPacketID  string `json:"last_packet_id"`
			ActiveIssueID string `json:"active_issue_id"`
		} `json:"focus"`
		Packet struct {
			PacketID string `json:"packet_id"`
			Scope    string `json:"scope"`
		} `json:"packet"`
		Idempotent bool `json:"idempotent"`
	} `json:"data"`
}

type contextRehydrateEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		SessionID string `json:"session_id"`
		Source    string `json:"source"`
		Packet    struct {
			PacketID string `json:"packet_id"`
			Scope    string `json:"scope"`
		} `json:"packet"`
	} `json:"data"`
}

type contextLoopsEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Count int `json:"count"`
		Loops []struct {
			IssueID string `json:"issue_id"`
			Status  string `json:"status"`
		} `json:"loops"`
	} `json:"data"`
}

type eventLogEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		EntityType string `json:"entity_type"`
		EntityID   string `json:"entity_id"`
		Events     []struct {
			EventID       string `json:"event_id"`
			EntityType    string `json:"entity_type"`
			EntityID      string `json:"entity_id"`
			EventType     string `json:"event_type"`
			CommandID     string `json:"command_id"`
			CausationID   string `json:"causation_id"`
			CorrelationID string `json:"correlation_id"`
		} `json:"events"`
	} `json:"data"`
}

func sessionIDFromHumanOutput(t *testing.T, stdout string) string {
	t.Helper()

	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		for _, prefix := range []string{"OK Created session checkpoint ", "OK Updated session checkpoint "} {
			if strings.HasPrefix(line, prefix) {
				rest := strings.TrimPrefix(line, prefix)
				if idx := strings.Index(rest, " "); idx > 0 {
					return rest[:idx]
				}
				return rest
			}
		}
	}
	t.Fatalf("expected session id in output, got:\n%s", stdout)
	return ""
}
