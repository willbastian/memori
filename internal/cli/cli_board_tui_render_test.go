package cli

import (
	"strings"
	"testing"

	"github.com/willbastian/memori/internal/store"
)

func TestRenderBoardTUIWideShowsDetailPane(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Agent:   "agent-wide-1",
		Summary: boardSummary{Total: 3, Todo: 1, InProgress: 1, Blocked: 1},
		LikelyNext: []boardIssueRow{
			{
				Issue:   boardTestIssue("mem-a111111", "Task", "Todo", "Next one"),
				Score:   220,
				Reasons: []string{"matches the agent's active focus for resume", "has 1 open loop(s) that need continuity"},
			},
		},
		Active:  []boardIssueRow{{Issue: boardTestIssue("mem-b222222", "Task", "InProgress", "Active one")}},
		Blocked: []boardIssueRow{{Issue: boardTestIssue("mem-c333333", "Bug", "Blocked", "Blocked one")}},
	}, 120, 28)
	model.detailOpen = true

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"MEMORI BOARD",
		"NEXT 1",
		"ISSUE DETAIL",
		"a111111 · Next one",
		"[ REASONS ]",
		"active focus",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected wide render to contain %q, got:\n%s", want, rendered)
		}
	}
	for _, dontWant := range []string{"ACTIONABLE", "T3 IP1 BLK1 RDY1"} {
		if strings.Contains(rendered, dontWant) {
			t.Fatalf("expected wide render to omit %q, got:\n%s", dontWant, rendered)
		}
	}
}

func TestRenderBoardTUIWideDefaultsToListOnlyUntilPaneOpened(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		LikelyNext: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Next one")},
		},
	}, 120, 28)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"MEMORI BOARD",
		"NEXT 1",
		"> Next one",
		"Next one  · mem-a111111 · task",
		"enter detail",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected default wide render to contain %q, got:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "ISSUE DETAIL") || strings.Contains(rendered, "CONTINUITY") {
		t.Fatalf("expected default wide render to stay list-first until pane open, got:\n%s", rendered)
	}
}

func TestRenderBoardTUIAlwaysShowsExplicitSelectedRowMarker(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "First row")},
			{Issue: boardTestIssue("mem-b222222", "Task", "Todo", "Second row")},
		},
	}, 96, 18)
	model.lane = boardLaneReady
	model.index = 1
	model = boardNormalizeModel(model)

	rendered := renderBoardTUI(model, false)
	if !strings.Contains(rendered, "> Second row") {
		t.Fatalf("expected rendered board to show an explicit selected-row marker, got:\n%s", rendered)
	}
}

func TestRenderBoardTUIWideShowsContinuityPane(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		LikelyNext: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Next one")},
		},
	}, 120, 28)
	model.detailOpen = true
	model.panelMode = boardPanelModeContinuity
	model.audit = store.ContinuityAuditSnapshot{
		Resolution: store.ContinuityResolution{
			Source:       "agent-focus-session",
			Status:       "fresh",
			SessionID:    "sess-audit-1",
			PacketID:     "pkt-audit-1",
			PacketScope:  "session",
			PacketSource: "packet",
		},
		Issue: store.IssueContinuitySnapshot{
			IssueID:     "mem-a111111",
			HasPacket:   true,
			PacketFresh: true,
			LatestPacket: store.RehydratePacket{
				PacketID: "pkt-issue-1",
			},
		},
		Sessions: []store.ContinuitySessionCandidate{
			{
				Session:      store.Session{SessionID: "sess-audit-1"},
				Lifecycle:    "active",
				HasPacket:    true,
				HasSummary:   true,
				IsSelected:   true,
				ResolverNote: "agent focus selected this session directly",
			},
		},
		SessionPackets: []store.ContinuityPacketCandidate{
			{
				Packet: store.RehydratePacket{PacketID: "pkt-audit-1", BuiltFromEventID: "evt-1"},
				Status: "active",
			},
		},
		RecentWrites: []store.ContinuityWrite{
			{EventType: "session.checkpointed", EntityID: "sess-audit-1", CreatedAt: "2026-03-20T10:00:00Z"},
		},
	}

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"CONTINUITY",
		"[ DECISION ]",
		"Resume looks healthy for this issue.",
		"memori will resume from the session",
		"[ CURRENT SESSION ]",
		"sess-audit-1",
		"[ NEXT STEP ]",
		"memori context resume --session sess-audit-1",
		"[ EVIDENCE ]",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected continuity render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUIContinuityPaneExplainsWeakHandoff(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		LikelyNext: []boardIssueRow{
			{Issue: boardTestIssue("mem-dccbb32", "Story", "InProgress", "Implement the continuity inspection facet in the board TUI")},
		},
	}, 140, 28)
	model.detailOpen = true
	model.panelMode = boardPanelModeContinuity
	model.audit = store.ContinuityAuditSnapshot{
		Resolution: store.ContinuityResolution{
			Source:       "latest-open-issue",
			Status:       "fresh",
			SessionID:    "sess_85471748e4fa",
			PacketID:     "pkt_42f7cb9fac4f8072",
			PacketScope:  "issue",
			PacketSource: "issue-packet",
		},
		Issue: store.IssueContinuitySnapshot{
			IssueID:     "mem-dccbb32",
			HasPacket:   true,
			PacketFresh: true,
			LatestPacket: store.RehydratePacket{
				PacketID: "pkt_42f7cb9fac4f8072",
			},
		},
		Session: store.SessionContinuitySnapshot{
			HasSession: true,
			Session: store.Session{
				SessionID: "sess_85471748e4fa",
				StartedAt: "2026-03-21T01:52:00Z",
			},
		},
		Sessions: []store.ContinuitySessionCandidate{
			{
				Session: store.Session{
					SessionID: "sess_85471748e4fa",
					StartedAt: "2026-03-21T01:52:00Z",
				},
				Lifecycle:    "active",
				HasPacket:    false,
				HasSummary:   false,
				IsSelected:   true,
				ResolverNote: "latest open session for this issue",
			},
		},
		Alerts: []store.ContinuityAlert{
			{
				Level:   "warn",
				Code:    "session-unsaved",
				Message: "session sess_85471748e4fa has context chunks but no summary and no session packet",
			},
		},
		RecentWrites: []store.ContinuityWrite{
			{EventType: "session.checkpointed", EntityID: "sess_85471748e4fa", CreatedAt: "2026-03-21T01:52:00Z"},
		},
	}

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"Resume is available, but handoff is weak.",
		"if you stop now:",
		"Best next step: save this session",
		"memori context summarize --session sess_8547174",
		"handoff is weak: work exists",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected continuity handoff guidance to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardSnapshotAddsIssueSignalsToHumanRows(t *testing.T) {
	t.Parallel()

	snapshot := boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
		},
	}

	rendered, err := renderBoardSnapshot(snapshot, boardRenderOptions{Width: 80})
	if err != nil {
		t.Fatalf("render board snapshot: %v", err)
	}
	if !strings.Contains(rendered, "mem-a111111 Ready one [task/todo]") {
		t.Fatalf("expected human board row to show issue signal, got:\n%s", rendered)
	}
}

func TestRenderBoardTUINarrowShowsSinglePaneAndHelp(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")}},
	}, 72, 22)
	model = boardReduce(model, boardActionToggleHelp)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"KEYBOARD",
		"move selection",
		"jump parent / child",
		"quit",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected narrow help render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestBoardListPanelPadsColoredRowsToPanelWidth(t *testing.T) {
	t.Parallel()

	theme := boardTheme{
		colors:     true,
		accentFG:   "1;2;3",
		panelBG:    "4;5;6",
		mutedFG:    "7;8;9",
		panelAltBG: "10;11;12",
		selectedFG: "13;14;15",
		selectedBG: "16;17;18",
		taskFG:     "19;20;21",
		detailFG:   "22;23;24",
	}
	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "First row")},
			{Issue: boardTestIssue("mem-b222222", "Task", "Todo", "Second row")},
		},
	}, 32, 12)
	model.lane = boardLaneReady
	model = boardNormalizeModel(model)

	lines := boardListPanel(model, theme, 32, 4)
	for _, idx := range []int{1, 2} {
		if got := visualWidth(lines[idx]); got != 32 {
			t.Fatalf("expected rendered row %d to be padded to width 32, got %d (%q)", idx, got, stripANSI(lines[idx]))
		}
	}
}

func TestBoardSearchPanelPadsColoredRowsToPanelWidth(t *testing.T) {
	t.Parallel()

	theme := boardTheme{
		colors:     true,
		accentFG:   "1;2;3",
		panelBG:    "4;5;6",
		mutedFG:    "7;8;9",
		panelAltBG: "10;11;12",
		selectedFG: "13;14;15",
		selectedBG: "16;17;18",
		detailFG:   "19;20;21",
	}
	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
		},
		Blocked: []boardIssueRow{
			{Issue: boardTestIssue("mem-b222222", "Bug", "Blocked", "Blocked match")},
		},
	}, 32, 12)
	model.lane = boardLaneReady
	model = boardNormalizeModel(model)

	var quit bool
	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionSearchOpen})
	if quit {
		t.Fatalf("did not expect search open to quit")
	}
	model, quit = boardHandleInput(model, boardKeyInput{text: "b22"})
	if quit {
		t.Fatalf("did not expect text entry to quit")
	}

	lines := boardSearchPanel(model, theme, 32, 4)
	if got := visualWidth(lines[2]); got != 32 {
		t.Fatalf("expected rendered search row to be padded to width 32, got %d (%q)", got, stripANSI(lines[2]))
	}
}

func TestBoardDetailPanelPadsColoredSectionLinesToPanelWidth(t *testing.T) {
	t.Parallel()

	theme := boardTheme{
		colors:      true,
		accentFG:    "1;2;3",
		panelBG:     "4;5;6",
		mutedFG:     "7;8;9",
		panelAltBG:  "10;11;12",
		detailFG:    "13;14;15",
		metaFG:      "16;17;18",
		titleMetaBG: "19;20;21",
		readyFG:     "22;23;24",
		readyBG:     "25;26;27",
		keyFG:       "28;29;30",
		nextBG:      "31;32;33",
	}
	issue := boardTestIssue("mem-a111111", "Task", "Todo", "Detail title")
	issue.Description = "Description text for a narrow detail pane."
	issue.Acceptance = "Acceptance text should not leave stale content behind."
	issue.References = []string{"internal/cli/board_tui_detail_panel.go"}

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{
				Issue:   issue,
				Reasons: []string{"implementation-ready", "todo work is actionable"},
			},
		},
	}, 40, 16)
	model.lane = boardLaneReady
	model.detailOpen = true
	model = boardNormalizeModel(model)

	lines := boardDetailPanel(model, theme, 40, 12)
	for _, idx := range []int{5, 7, 9} {
		if got := visualWidth(lines[idx]); got != 40 {
			t.Fatalf("expected detail line %d to be padded to width 40, got %d (%q)", idx, got, stripANSI(lines[idx]))
		}
	}
}

func TestBoardFramePanelPreservesStyledRowContent(t *testing.T) {
	t.Parallel()

	theme := boardTheme{
		colors:     true,
		borderFG:   "1;2;3",
		panelBG:    "4;5;6",
		selectedFG: "7;8;9",
		selectedBG: "10;11;12",
	}

	line := theme.paintLine(theme.selectedFG, theme.selectedBG, true, padRight(" selected row ", 16))
	panel := boardFramePanel(theme, []string{line}, 18, 3)

	if !strings.Contains(panel[1], "\x1b[1;38;2;7;8;9;48;2;10;11;12m") {
		t.Fatalf("expected framed panel to preserve row styling, got %q", panel[1])
	}
	if !strings.Contains(stripANSI(panel[1]), "selected row") {
		t.Fatalf("expected framed panel to preserve row content, got %q", stripANSI(panel[1]))
	}
}

func TestRenderBoardTUIShowsHierarchyCuesInListAndDetail(t *testing.T) {
	t.Parallel()

	parent := boardTestIssue("mem-a111111", "Story", "Todo", "Parent story")
	parent.Description = "Parent work item."
	child := boardTestIssue("mem-b222222", "Task", "Todo", "Child task")
	child.Description = "Child work item."

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{
				Issue: parent,
				Hierarchy: boardIssueHierarchy{
					ChildIDs:        []string{"mem-b222222"},
					ChildCount:      1,
					DescendantCount: 1,
					HasChildren:     true,
				},
			},
			{
				Issue: child,
				Hierarchy: boardIssueHierarchy{
					Depth:           1,
					Path:            []string{"mem-a111111", "mem-b222222"},
					AncestorIDs:     []string{"mem-a111111"},
					ParentID:        "mem-a111111",
					ParentTitle:     "Parent story",
					ParentType:      "Story",
					ParentStatus:    "Todo",
					DescendantCount: 0,
				},
			},
		},
	}, 108, 24)
	model.lane = boardLaneReady
	model.index = 1
	model.detailOpen = true
	model = boardNormalizeModel(model)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"[-] Parent story  · mem-a111111 · story",
		"`- Child task  · mem-b222222 · task",
		"[ HIERARCHY ]",
		"path: ... > mem-b222222",
		"parent: mem-a111111",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected hierarchy render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUIShowsConsistentNestedHierarchyPrefixes(t *testing.T) {
	t.Parallel()

	root := boardIssueRow{
		Issue: boardTestIssue("mem-a111111", "Epic", "Todo", "Root epic"),
		Hierarchy: boardIssueHierarchy{
			ChildIDs:        []string{"mem-b222222", "mem-c333333"},
			ChildCount:      2,
			DescendantCount: 3,
			HasChildren:     true,
		},
	}
	child := boardIssueRow{
		Issue: boardTestIssue("mem-b222222", "Story", "Todo", "Nested story"),
		Hierarchy: boardIssueHierarchy{
			Depth:           1,
			Path:            []string{"mem-a111111", "mem-b222222"},
			AncestorIDs:     []string{"mem-a111111"},
			ParentID:        "mem-a111111",
			SiblingIndex:    0,
			SiblingCount:    2,
			ChildIDs:        []string{"mem-d444444"},
			ChildCount:      1,
			DescendantCount: 1,
			HasChildren:     true,
		},
	}
	childSibling := boardIssueRow{
		Issue: boardTestIssue("mem-c333333", "Story", "Todo", "Sibling story"),
		Hierarchy: boardIssueHierarchy{
			Depth:        1,
			Path:         []string{"mem-a111111", "mem-c333333"},
			AncestorIDs:  []string{"mem-a111111"},
			ParentID:     "mem-a111111",
			SiblingIndex: 1,
			SiblingCount: 2,
		},
	}
	grandchild := boardIssueRow{
		Issue: boardTestIssue("mem-d444444", "Task", "Todo", "Grandchild task"),
		Hierarchy: boardIssueHierarchy{
			Depth:        2,
			Path:         []string{"mem-a111111", "mem-b222222", "mem-d444444"},
			AncestorIDs:  []string{"mem-a111111", "mem-b222222"},
			ParentID:     "mem-b222222",
			SiblingCount: 1,
		},
	}

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{root, child, grandchild, childSibling},
	}, 120, 28)
	model.lane = boardLaneReady
	model = boardNormalizeModel(model)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"[-] Root epic  · mem-a111111 · epic",
		"   [-] Nested story  · mem-b222222 · story",
		"      `- Grandchild task  · mem-d444444 · task",
		"   `- Sibling story  · mem-c333333 · story",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected nested hierarchy render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUIUsesLaneSiblingOrderForLeafBranches(t *testing.T) {
	t.Parallel()

	root := boardIssueRow{
		Issue: boardTestIssue("mem-a111111", "Epic", "Todo", "Root epic"),
		Hierarchy: boardIssueHierarchy{
			ChildIDs:        []string{"mem-b222222", "mem-c333333", "mem-d444444"},
			ChildCount:      3,
			DescendantCount: 3,
			HasChildren:     true,
		},
	}
	first := boardIssueRow{
		Issue: boardTestIssue("mem-b222222", "Story", "Todo", "First child"),
		Hierarchy: boardIssueHierarchy{
			Depth:        1,
			Path:         []string{"mem-a111111", "mem-b222222"},
			AncestorIDs:  []string{"mem-a111111"},
			ParentID:     "mem-a111111",
			SiblingIndex: 2,
			SiblingCount: 3,
		},
	}
	second := boardIssueRow{
		Issue: boardTestIssue("mem-c333333", "Story", "Todo", "Second child"),
		Hierarchy: boardIssueHierarchy{
			Depth:        1,
			Path:         []string{"mem-a111111", "mem-c333333"},
			AncestorIDs:  []string{"mem-a111111"},
			ParentID:     "mem-a111111",
			SiblingIndex: 0,
			SiblingCount: 3,
		},
	}
	last := boardIssueRow{
		Issue: boardTestIssue("mem-d444444", "Story", "Todo", "Third child"),
		Hierarchy: boardIssueHierarchy{
			Depth:        1,
			Path:         []string{"mem-a111111", "mem-d444444"},
			AncestorIDs:  []string{"mem-a111111"},
			ParentID:     "mem-a111111",
			SiblingIndex: 1,
			SiblingCount: 3,
		},
	}

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{root, first, second, last},
	}, 120, 28)
	model.lane = boardLaneReady
	model = boardNormalizeModel(model)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"|- First child  · mem-b222222 · story",
		"|- Second child  · mem-c333333 · story",
		"`- Third child  · mem-d444444 · story",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected lane-order branches to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUIShowsSearchPanel(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
		},
		Blocked: []boardIssueRow{
			{Issue: boardTestIssue("mem-b222222", "Bug", "Blocked", "Blocked match")},
		},
	}, 108, 24)
	model.lane = boardLaneReady
	model = boardNormalizeModel(model)

	var quit bool
	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionSearchOpen})
	if quit {
		t.Fatalf("did not expect search open to quit")
	}
	model, quit = boardHandleInput(model, boardKeyInput{text: "b22"})
	if quit {
		t.Fatalf("did not expect text entry to quit")
	}

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"SEARCH",
		"/b22",
		"BLOCKED",
		"b222222",
		"[enter jump]",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected search render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUINarrowShowsSearchPanel(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
		},
		Blocked: []boardIssueRow{
			{Issue: boardTestIssue("mem-b222222", "Bug", "Blocked", "Blocked match")},
		},
	}, 72, 22)
	model.lane = boardLaneReady
	model = boardNormalizeModel(model)

	var quit bool
	model, quit = boardHandleInput(model, boardKeyInput{action: boardActionSearchOpen})
	if quit {
		t.Fatalf("did not expect search open to quit")
	}
	model, quit = boardHandleInput(model, boardKeyInput{text: "b22"})
	if quit {
		t.Fatalf("did not expect text entry to quit")
	}

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"SEARCH",
		"/b22",
		"BLOCKED",
		"[enter jump]",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected narrow search render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUIShowsInspectorStatusAndToast(t *testing.T) {
	t.Parallel()

	issue := boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")
	issue.Description = strings.Repeat("Long detail body for scrolling and status visibility. ", 16)

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{{Issue: issue}},
	}, 120, 20)
	model.lane = boardLaneReady
	model.detailOpen = true
	model.panelMode = boardPanelModeContinuity
	model.snapshotLoad = boardAsyncLoadState{stale: true, err: "snapshot failed"}
	model.auditLoad = boardAsyncLoadState{stale: true, err: "audit failed"}
	model.toast = boardToast{message: "Board refresh failed: snapshot unavailable", tone: boardToastToneWarn}
	model = boardNormalizeModel(model)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"snapshot stale",
		"the latest continuity refresh failed",
		"Board refresh failed: snapshot unavailable",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected inspector status render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestBoardContinuityPanelDirectRenderShowsHeader(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")}},
	}, 60, 12)
	model.lane = boardLaneReady
	model.detailOpen = true
	model.panelMode = boardPanelModeContinuity
	model = boardNormalizeModel(model)

	lines := boardContinuityPanel(model, defaultBoardTheme(false), 32, 6)
	if len(lines) != 6 || !strings.Contains(lines[0], "CONTINUITY") {
		t.Fatalf("expected direct continuity panel render, got %#v", lines)
	}
}

func TestRenderBoardTUIVeryNarrowStillShowsTickets(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "A narrow pane should still show tickets")},
		},
	}, 28, 14)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"BOARD",
		"READY 1",
		"a11111",
		"A narrow pa",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected very narrow render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUIVeryNarrowFooterKeepsStatusAndIssueID(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "A narrow pane should still show footer priority")},
		},
	}, 28, 14)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"a11111 TODO",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected very narrow footer to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUINarrowDetailPrefersFullIssueContent(t *testing.T) {
	t.Parallel()

	issue := boardTestIssue("mem-a111111", "Task", "Todo", "Narrow detail")
	issue.Description = "Ship the full issue detail in narrow mode before continuity hints."
	issue.Acceptance = "Description and acceptance criteria remain visible in compact panes."
	issue.References = []string{"docs/board.md", "internal/cli/board_tui.go"}

	model := newBoardTUIModel(boardSnapshot{
		LikelyNext: []boardIssueRow{
			{
				Issue:   issue,
				Reasons: []string{"active focus resume", "open loops waiting"},
			},
		},
	}, 72, 18)
	model.detailOpen = true

	rendered := renderBoardTUI(model, false)
	descriptionIndex := strings.Index(rendered, "[ DESCRIPTION ]")
	acceptanceIndex := strings.Index(rendered, "[ ACCEPTANCE ]")
	reasonsIndex := strings.Index(rendered, "[ REASONS ]")
	for _, want := range []string{
		"[ DESCRIPTION ]",
		"Ship the full issue detail",
		"[ ACCEPTANCE ]",
		"compact panes.",
		"[ REFERENCES ]",
		"docs/board.md",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected narrow detail render to contain %q, got:\n%s", want, rendered)
		}
	}
	if reasonsIndex != -1 && descriptionIndex != -1 && reasonsIndex < descriptionIndex {
		t.Fatalf("expected reasons to come after full issue detail in narrow mode, got:\n%s", rendered)
	}
	if reasonsIndex != -1 && acceptanceIndex != -1 && reasonsIndex < acceptanceIndex {
		t.Fatalf("expected reasons to come after acceptance details in narrow mode, got:\n%s", rendered)
	}
}

func TestRenderBoardTUINarrowDetailPrioritizesCompactMetaAndHierarchy(t *testing.T) {
	t.Parallel()

	issue := boardTestIssue("mem-2f488f2", "Task", "Todo", "Add Darwin password prompt seams for deterministic auth coverage")
	issue.Priority = "P2"
	issue.Description = "Detail should keep compact metadata and hierarchy readable."

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{
				Issue: issue,
				Hierarchy: boardIssueHierarchy{
					Depth:           2,
					Path:            []string{"mem-127b139", "mem-c5cc217", "mem-2f488f2"},
					AncestorIDs:     []string{"mem-127b139", "mem-c5cc217"},
					ParentID:        "mem-c5cc217",
					ParentTitle:     "Strengthen regression coverage",
					ChildIDs:        []string{"mem-a111111", "mem-b222222", "mem-c333333"},
					ChildCount:      3,
					DescendantCount: 3,
				},
			},
		},
	}, 40, 18)
	model.lane = boardLaneReady
	model.detailOpen = true
	model = boardNormalizeModel(model)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"2f488f2",
		"Task",
		"Todo",
		"P2",
		"p:c5cc217",
		"path: ... > mem-2f488f2",
		"parent: mem-c5cc217",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected narrow detail render to contain %q, got:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "mem-a111111, mem-b222222") {
		t.Fatalf("expected narrow hierarchy to avoid dumping full child id list, got:\n%s", rendered)
	}
}

func TestRenderBoardTUIHistoryModeShowsDoneAndWontDoTabs(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
		},
		Done: []boardIssueRow{
			{Issue: boardTestIssue("mem-b222222", "Task", "Done", "Done one")},
		},
		WontDo: []boardIssueRow{
			{Issue: boardTestIssue("mem-c333333", "Bug", "WontDo", "Declined one")},
		},
	}, 132, 24)
	model = boardReduce(model, boardActionToggleHistory)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"DONE 1",
		"WONTDO 1",
		"f actionable",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected history render to contain %q, got:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "ALL WORK") {
		t.Fatalf("expected history render to omit old header mode label, got:\n%s", rendered)
	}
}

func TestRenderBoardTUIReadyLaneMarksReadyRowsWithinContextTree(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Active: []boardIssueRow{
			{
				Issue: boardTestIssue("mem-a111111", "Story", "InProgress", "Parent story"),
				Hierarchy: boardIssueHierarchy{
					ChildIDs:        []string{"mem-b222222", "mem-c333333", "mem-d444444"},
					ChildCount:      3,
					DescendantCount: 3,
					HasChildren:     true,
				},
			},
			{
				Issue: boardTestIssue("mem-c333333", "Bug", "InProgress", "Active sibling"),
				Hierarchy: boardIssueHierarchy{
					Depth:        1,
					Path:         []string{"mem-a111111", "mem-c333333"},
					AncestorIDs:  []string{"mem-a111111"},
					ParentID:     "mem-a111111",
					SiblingIndex: 1,
					SiblingCount: 3,
				},
			},
		},
		Ready: []boardIssueRow{
			{
				Issue: boardTestIssue("mem-b222222", "Task", "Todo", "Ready child"),
				Hierarchy: boardIssueHierarchy{
					Depth:        1,
					Path:         []string{"mem-a111111", "mem-b222222"},
					AncestorIDs:  []string{"mem-a111111"},
					ParentID:     "mem-a111111",
					SiblingIndex: 0,
					SiblingCount: 3,
				},
			},
		},
		Done: []boardIssueRow{
			{
				Issue: boardTestIssue("mem-d444444", "Task", "Done", "Done child"),
				Hierarchy: boardIssueHierarchy{
					Depth:        1,
					Path:         []string{"mem-a111111", "mem-d444444"},
					AncestorIDs:  []string{"mem-a111111"},
					ParentID:     "mem-a111111",
					SiblingIndex: 2,
					SiblingCount: 3,
				},
			},
		},
	}, 120, 24)
	model.lane = boardLaneReady
	model = boardNormalizeModel(model)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"Parent story  · [in progress] · mem-a111111 · story",
		"Ready child  · mem-b222222 · task",
		"Active sibling  · [in progress] · mem-c333333 · bug",
		"Done child  · [done] · mem-d444444 · task",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected ready context render to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRenderBoardTUIActiveLaneMarksActiveRowsWithinContextTree(t *testing.T) {
	t.Parallel()

	model := newBoardTUIModel(boardSnapshot{
		Active: []boardIssueRow{
			{
				Issue: boardTestIssue("mem-b222222", "Task", "InProgress", "Active child"),
				Hierarchy: boardIssueHierarchy{
					Depth:        1,
					Path:         []string{"mem-a111111", "mem-b222222"},
					AncestorIDs:  []string{"mem-a111111"},
					ParentID:     "mem-a111111",
					SiblingIndex: 0,
					SiblingCount: 2,
				},
			},
		},
		Ready: []boardIssueRow{
			{
				Issue: boardTestIssue("mem-a111111", "Story", "Todo", "Parent story"),
				Hierarchy: boardIssueHierarchy{
					ChildIDs:        []string{"mem-b222222", "mem-c333333", "mem-d444444"},
					ChildCount:      3,
					DescendantCount: 3,
					HasChildren:     true,
				},
			},
			{
				Issue: boardTestIssue("mem-c333333", "Task", "Todo", "Ready sibling"),
				Hierarchy: boardIssueHierarchy{
					Depth:        1,
					Path:         []string{"mem-a111111", "mem-c333333"},
					AncestorIDs:  []string{"mem-a111111"},
					ParentID:     "mem-a111111",
					SiblingIndex: 1,
					SiblingCount: 3,
				},
			},
		},
		Done: []boardIssueRow{
			{
				Issue: boardTestIssue("mem-d444444", "Task", "Done", "Done sibling"),
				Hierarchy: boardIssueHierarchy{
					Depth:        1,
					Path:         []string{"mem-a111111", "mem-d444444"},
					AncestorIDs:  []string{"mem-a111111"},
					ParentID:     "mem-a111111",
					SiblingIndex: 2,
					SiblingCount: 3,
				},
			},
		},
	}, 120, 24)
	model.lane = boardLaneActive
	model = boardNormalizeModel(model)

	rendered := renderBoardTUI(model, false)
	for _, want := range []string{
		"Parent story  · [todo] · mem-a111111 · story",
		"Active child  · mem-b222222 · task",
		"Ready sibling  · [todo] · mem-c333333 · task",
		"Done sibling  · [done] · mem-d444444 · task",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected active context render to contain %q, got:\n%s", want, rendered)
		}
	}
}
