package store

import (
	"context"
	"strings"
	"testing"
)

func TestNextIssuePrefersContinuitySignalsForAgentResume(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	baselineIssueID := "mem-4343434"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   baselineIssueID,
		Type:      "task",
		Title:     "Baseline in-progress task",
		Actor:     "agent-1",
		CommandID: "cmd-next-create-baseline-1",
	}); err != nil {
		t.Fatalf("create baseline issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   baselineIssueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-next-progress-baseline-1",
	}); err != nil {
		t.Fatalf("move baseline issue to inprogress: %v", err)
	}
	priority := "p0"
	if _, _, _, err := s.UpdateIssue(ctx, UpdateIssueParams{
		IssueID:   baselineIssueID,
		Priority:  &priority,
		Actor:     "agent-1",
		CommandID: "cmd-next-priority-baseline-1",
	}); err != nil {
		t.Fatalf("set baseline issue priority: %v", err)
	}

	continuityIssueID := "mem-4545454"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   continuityIssueID,
		Type:      "task",
		Title:     "Continuity-heavy resume task",
		Actor:     "agent-1",
		CommandID: "cmd-next-create-continuity-1",
	}); err != nil {
		t.Fatalf("create continuity issue: %v", err)
	}
	definition := `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"echo continuity"}}]}`
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "next-continuity",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: definition,
		Actor:          "human:alice",
		CommandID:      "cmd-next-template-1",
	}); err != nil {
		t.Fatalf("create continuity gate template: %v", err)
	}
	if _, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      continuityIssueID,
		TemplateRefs: []string{"next-continuity@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-next-instantiate-1",
	}); err != nil {
		t.Fatalf("instantiate continuity gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   continuityIssueID,
		Actor:     "agent-1",
		CommandID: "cmd-next-lock-1",
	}); err != nil {
		t.Fatalf("lock continuity gate set: %v", err)
	}
	packet, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "issue",
		ScopeID:   continuityIssueID,
		Actor:     "agent-1",
		CommandID: "cmd-next-packet-build-1",
	})
	if err != nil {
		t.Fatalf("build continuity packet: %v", err)
	}
	if _, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      continuityIssueID,
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/next-continuity-1"},
		Actor:        "agent-1",
		CommandID:    "cmd-next-gate-fail-1",
	}); err != nil {
		t.Fatalf("evaluate continuity gate fail: %v", err)
	}
	if _, _, _, err := s.UseRehydratePacket(ctx, UsePacketParams{
		AgentID:   "agent-next-1",
		PacketID:  packet.PacketID,
		Actor:     "agent-1",
		CommandID: "cmd-next-packet-use-1",
	}); err != nil {
		t.Fatalf("use continuity packet: %v", err)
	}

	baselineNext, err := s.NextIssue(ctx, "")
	if err != nil {
		t.Fatalf("next issue without continuity agent: %v", err)
	}
	if baselineNext.Candidate.Issue.ID != baselineIssueID {
		t.Fatalf("expected baseline issue %q without continuity agent, got %q", baselineIssueID, baselineNext.Candidate.Issue.ID)
	}

	resumeNext, err := s.NextIssue(ctx, "agent-next-1")
	if err != nil {
		t.Fatalf("next issue for continuity agent: %v", err)
	}
	if resumeNext.Candidate.Issue.ID != continuityIssueID {
		t.Fatalf("expected continuity issue %q, got %q", continuityIssueID, resumeNext.Candidate.Issue.ID)
	}
	if resumeNext.Considered != 2 {
		t.Fatalf("expected 2 considered issues, got %d", resumeNext.Considered)
	}

	reasonText := strings.Join(resumeNext.Candidate.Reasons, "\n")
	for _, expected := range []string{
		"matches the agent's active focus for resume",
		"agent already holds the latest recovery packet",
		"has 1 open loop(s) that need continuity",
		"1 required gate(s) are failing",
		"available issue packet is stale",
	} {
		if !strings.Contains(reasonText, expected) {
			t.Fatalf("expected next issue reasons to contain %q, got %q", expected, reasonText)
		}
	}
}
