package store

import (
	"context"
	"testing"
)

func TestContinuitySnapshotIncludesIssueAgentAndSessionState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5151515"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Continuity status issue",
		Actor:     "agent-1",
		CommandID: "cmd-snapshot-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	packet, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "issue",
		ScopeID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-snapshot-issue-packet-1",
	})
	if err != nil {
		t.Fatalf("build issue packet: %v", err)
	}

	if _, _, err := s.CheckpointSession(ctx, CheckpointSessionParams{
		SessionID: "sess-snapshot-1",
		Trigger:   "manual",
		Actor:     "agent-1",
		CommandID: "cmd-snapshot-checkpoint-1",
	}); err != nil {
		t.Fatalf("checkpoint session: %v", err)
	}
	if _, err := s.SummarizeSession(ctx, SummarizeSessionParams{
		SessionID: "sess-snapshot-1",
		Note:      "checkpointed before resume",
		Actor:     "agent-1",
		CommandID: "cmd-snapshot-summarize-1",
	}); err != nil {
		t.Fatalf("summarize session: %v", err)
	}
	sessionPacket, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "session",
		ScopeID:   "sess-snapshot-1",
		Actor:     "agent-1",
		CommandID: "cmd-snapshot-session-packet-1",
	})
	if err != nil {
		t.Fatalf("build session packet: %v", err)
	}

	definition := `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"echo snapshot"}}]}`
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "snapshot-template",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: definition,
		Actor:          "human:alice",
		CommandID:      "cmd-snapshot-template-1",
	}); err != nil {
		t.Fatalf("create gate template: %v", err)
	}
	if _, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"snapshot-template@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-snapshot-gset-1",
	}); err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-snapshot-lock-1",
	}); err != nil {
		t.Fatalf("lock gate set: %v", err)
	}
	if _, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      issueID,
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://snapshot/2"},
		Actor:        "agent-1",
		CommandID:    "cmd-snapshot-gate-fail-2",
	}); err != nil {
		t.Fatalf("evaluate gate: %v", err)
	}

	if _, _, _, err := s.UseRehydratePacket(ctx, UsePacketParams{
		AgentID:   "agent-snapshot-1",
		PacketID:  packet.PacketID,
		Actor:     "agent-1",
		CommandID: "cmd-snapshot-use-1",
	}); err != nil {
		t.Fatalf("use packet: %v", err)
	}

	snapshot, err := s.ContinuitySnapshot(ctx, ContinuitySnapshotParams{
		IssueID: issueID,
		AgentID: "agent-snapshot-1",
	})
	if err != nil {
		t.Fatalf("continuity snapshot: %v", err)
	}

	if !snapshot.Issue.HasPacket || snapshot.Issue.LatestPacket.PacketID != packet.PacketID {
		t.Fatalf("expected issue packet %q, got %+v", packet.PacketID, snapshot.Issue)
	}
	if !snapshot.Issue.PacketStale || snapshot.Issue.PacketFresh {
		t.Fatalf("expected stale issue packet after gate change, got %+v", snapshot.Issue)
	}
	if snapshot.Issue.OpenLoopCount != 1 {
		t.Fatalf("expected one open loop, got %+v", snapshot.Issue)
	}
	if !snapshot.Agent.HasFocus || snapshot.Agent.Focus.ActiveIssueID != issueID {
		t.Fatalf("expected focus on %q, got %+v", issueID, snapshot.Agent)
	}
	if !snapshot.Agent.HasLastPacket || snapshot.Agent.LastPacket.PacketID != packet.PacketID {
		t.Fatalf("expected focus packet %q, got %+v", packet.PacketID, snapshot.Agent)
	}
	if !snapshot.Session.HasSession || snapshot.Session.Source != "latest-open" {
		t.Fatalf("expected latest open session, got %+v", snapshot.Session)
	}
	if snapshot.Session.Session.SessionID != "sess-snapshot-1" || snapshot.Session.Session.SummaryEventID == "" {
		t.Fatalf("expected summarized session sess-snapshot-1, got %+v", snapshot.Session.Session)
	}
	if !snapshot.Session.HasPacket || snapshot.Session.Packet.PacketID != sessionPacket.PacketID {
		t.Fatalf("expected session packet %q, got %+v", sessionPacket.PacketID, snapshot.Session)
	}
}
