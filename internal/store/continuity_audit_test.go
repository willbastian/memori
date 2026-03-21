package store

import (
	"context"
	"testing"
)

func TestContinuityAuditSnapshotIncludesResolutionCandidatesWritesAndAlerts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-9191919"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Continuity audit story",
		Actor:     "agent-audit-1",
		CommandID: "cmd-audit-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	issuePacket, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "issue",
		ScopeID:   issueID,
		Actor:     "agent-audit-1",
		CommandID: "cmd-audit-issue-packet-1",
	})
	if err != nil {
		t.Fatalf("build issue packet: %v", err)
	}

	if _, _, err := s.CheckpointSession(ctx, CheckpointSessionParams{
		SessionID: "sess-audit-1",
		IssueID:   issueID,
		Trigger:   "manual",
		Actor:     "agent-audit-1",
		CommandID: "cmd-audit-checkpoint-1",
	}); err != nil {
		t.Fatalf("checkpoint session 1: %v", err)
	}
	if _, err := s.SummarizeSession(ctx, SummarizeSessionParams{
		SessionID: "sess-audit-1",
		Note:      "captured first handoff",
		Actor:     "agent-audit-1",
		CommandID: "cmd-audit-summarize-1",
	}); err != nil {
		t.Fatalf("summarize session 1: %v", err)
	}
	sessionPacket, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "session",
		ScopeID:   "sess-audit-1",
		Actor:     "agent-audit-1",
		CommandID: "cmd-audit-session-packet-1",
	})
	if err != nil {
		t.Fatalf("build session packet: %v", err)
	}
	if _, _, err := s.CheckpointSession(ctx, CheckpointSessionParams{
		SessionID: "sess-audit-2",
		IssueID:   issueID,
		Trigger:   "manual",
		Actor:     "agent-audit-1",
		CommandID: "cmd-audit-checkpoint-2",
	}); err != nil {
		t.Fatalf("checkpoint session 2: %v", err)
	}

	if _, _, _, err := s.UseRehydratePacket(ctx, UsePacketParams{
		AgentID:   "agent-audit-1",
		PacketID:  issuePacket.PacketID,
		Actor:     "agent-audit-1",
		CommandID: "cmd-audit-focus-1",
	}); err != nil {
		t.Fatalf("use issue packet for focus: %v", err)
	}

	snapshot, err := s.ContinuityAuditSnapshot(ctx, ContinuityAuditSnapshotParams{
		IssueID: issueID,
		AgentID: "agent-audit-1",
	})
	if err != nil {
		t.Fatalf("continuity audit snapshot: %v", err)
	}

	if snapshot.Resolution.Source != "agent-focus-issue-open" {
		t.Fatalf("expected agent-focus-issue-open resolution, got %+v", snapshot.Resolution)
	}
	if snapshot.Resolution.SessionID != "sess-audit-2" {
		t.Fatalf("expected latest open issue session to win, got %+v", snapshot.Resolution)
	}
	if snapshot.Resolution.Status != "ambiguous" {
		t.Fatalf("expected ambiguous status with two open sessions, got %+v", snapshot.Resolution)
	}
	if len(snapshot.Sessions) != 2 {
		t.Fatalf("expected two session candidates, got %+v", snapshot.Sessions)
	}
	if !snapshot.Sessions[0].IsSelected || snapshot.Sessions[0].Session.SessionID != "sess-audit-2" {
		t.Fatalf("expected sess-audit-2 to be selected, got %+v", snapshot.Sessions)
	}
	if len(snapshot.IssuePackets) != 1 || snapshot.IssuePackets[0].Packet.PacketID != issuePacket.PacketID {
		t.Fatalf("expected one issue packet candidate, got %+v", snapshot.IssuePackets)
	}
	if len(snapshot.SessionPackets) != 1 || snapshot.SessionPackets[0].Packet.PacketID != sessionPacket.PacketID {
		t.Fatalf("expected one saved session packet candidate, got %+v", snapshot.SessionPackets)
	}
	if len(snapshot.RecentWrites) == 0 {
		t.Fatalf("expected recent writes, got %+v", snapshot.RecentWrites)
	}
	if !hasContinuityAlertCode(snapshot.Alerts, "multiple-open-sessions") {
		t.Fatalf("expected multiple-open-sessions alert, got %+v", snapshot.Alerts)
	}
	if !hasContinuityAlertCode(snapshot.Alerts, "session-unsaved") {
		t.Fatalf("expected session-unsaved alert, got %+v", snapshot.Alerts)
	}
}

func TestContinuityAuditSnapshotFlagsClosedSessionPacketMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-a9a9a9a"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Closed session mismatch",
		Actor:     "agent-audit-2",
		CommandID: "cmd-audit-create-2",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, err := s.CheckpointSession(ctx, CheckpointSessionParams{
		SessionID: "sess-audit-closed-1",
		IssueID:   issueID,
		Trigger:   "manual",
		Actor:     "agent-audit-2",
		CommandID: "cmd-audit-closed-checkpoint-1",
	}); err != nil {
		t.Fatalf("checkpoint session: %v", err)
	}
	if _, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "session",
		ScopeID:   "sess-audit-closed-1",
		Actor:     "agent-audit-2",
		CommandID: "cmd-audit-closed-packet-1",
	}); err != nil {
		t.Fatalf("build active session packet: %v", err)
	}
	if _, err := s.SummarizeSession(ctx, SummarizeSessionParams{
		SessionID: "sess-audit-closed-1",
		Note:      "ready to close",
		Actor:     "agent-audit-2",
		CommandID: "cmd-audit-closed-summary-1",
	}); err != nil {
		t.Fatalf("summarize session: %v", err)
	}
	if _, err := s.CloseSession(ctx, CloseSessionParams{
		SessionID: "sess-audit-closed-1",
		Reason:    "done for now",
		Actor:     "agent-audit-2",
		CommandID: "cmd-audit-closed-close-1",
	}); err != nil {
		t.Fatalf("close session: %v", err)
	}

	snapshot, err := s.ContinuityAuditSnapshot(ctx, ContinuityAuditSnapshotParams{IssueID: issueID})
	if err != nil {
		t.Fatalf("continuity audit snapshot: %v", err)
	}
	if !hasContinuityAlertCode(snapshot.Alerts, "closed-session-packet-mismatch") {
		t.Fatalf("expected closed-session-packet-mismatch alert, got %+v", snapshot.Alerts)
	}
}

func hasContinuityAlertCode(alerts []ContinuityAlert, code string) bool {
	for _, alert := range alerts {
		if alert.Code == code {
			return true
		}
	}
	return false
}
