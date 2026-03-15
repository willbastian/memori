package store

import (
	"encoding/json"
	"testing"
)

func TestPacketIssueIDAndIssuePacketCycleNoFallbacks(t *testing.T) {
	t.Parallel()

	if got := packetScopeID(RehydratePacket{ScopeID: " mem-a1b2c3d "}); got != "mem-a1b2c3d" {
		t.Fatalf("expected direct packet scope id, got %q", got)
	}
	if got := packetScopeID(RehydratePacket{Packet: map[string]any{"scope_id": " mem-b2c3d4e "}}); got != "mem-b2c3d4e" {
		t.Fatalf("expected packet payload scope id fallback, got %q", got)
	}

	issueCases := []struct {
		name   string
		packet RehydratePacket
		wantID string
	}{
		{
			name:   "direct issue id wins",
			packet: RehydratePacket{IssueID: " mem-a1b2c3d ", ScopeID: "mem-ignored"},
			wantID: "mem-a1b2c3d",
		},
		{
			name:   "issue scope falls back to scope id",
			packet: RehydratePacket{Scope: "issue", ScopeID: " mem-b2c3d4e "},
			wantID: "mem-b2c3d4e",
		},
		{
			name:   "packet payload issue id fallback",
			packet: RehydratePacket{Packet: map[string]any{"issue_id": " mem-c3d4e5f "}},
			wantID: "mem-c3d4e5f",
		},
	}
	for _, tc := range issueCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := packetIssueID(tc.packet); got != tc.wantID {
				t.Fatalf("expected packetIssueID %q, got %q", tc.wantID, got)
			}
		})
	}

	cycleCases := []struct {
		name      string
		packet    RehydratePacket
		wantCycle int
	}{
		{
			name:      "direct issue cycle wins",
			packet:    RehydratePacket{IssueCycleNo: 3, Packet: map[string]any{"provenance": map[string]any{"issue_cycle_no": 9}}},
			wantCycle: 3,
		},
		{
			name:      "provenance cycle fallback",
			packet:    RehydratePacket{Packet: map[string]any{"provenance": map[string]any{"issue_cycle_no": json.Number("4")}}},
			wantCycle: 4,
		},
		{
			name:      "state cycle fallback",
			packet:    RehydratePacket{Packet: map[string]any{"state": map[string]any{"cycle_no": "5"}}},
			wantCycle: 5,
		},
		{
			name:      "missing packet returns zero",
			packet:    RehydratePacket{},
			wantCycle: 0,
		},
	}
	for _, tc := range cycleCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := issuePacketCycleNo(tc.packet); got != tc.wantCycle {
				t.Fatalf("expected issuePacketCycleNo %d, got %d", tc.wantCycle, got)
			}
		})
	}
}
