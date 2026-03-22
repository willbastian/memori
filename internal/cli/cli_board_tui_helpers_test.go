package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/x/ansi"
	"github.com/willbastian/memori/internal/store"
)

func TestBoardSearchScoreRanksExactPrefixAndContainsMatches(t *testing.T) {
	t.Parallel()

	if got := boardSearchScore("mem-abcd123", ""); got != 3 {
		t.Fatalf("expected empty query score 3, got %d", got)
	}
	if got := boardSearchScore("mem-abcd123", "mem-abcd123"); got != 0 {
		t.Fatalf("expected exact full id score 0, got %d", got)
	}
	if got := boardSearchScore("mem-abcd123", "abcd123"); got != 0 {
		t.Fatalf("expected exact short id score 0, got %d", got)
	}
	if got := boardSearchScore("mem-abcd123", "abc"); got != 1 {
		t.Fatalf("expected prefix score 1, got %d", got)
	}
	if got := boardSearchScore("mem-abcd123", "d12"); got != 2 {
		t.Fatalf("expected contains score 2, got %d", got)
	}
}

func TestBoardStatusHelpersMapStatusesToCodesAndPalette(t *testing.T) {
	t.Parallel()

	theme := boardTheme{
		activeBG:  "1;2;3",
		blockedBG: "4;5;6",
		readyBG:   "7;8;9",
		doneBG:    "10;11;12",
		wontDoBG:  "13;14;15",
		nextBG:    "16;17;18",
	}

	cases := []struct {
		status string
		code   string
		bg     string
	}{
		{status: "InProgress", code: ">>", bg: theme.activeBG},
		{status: "Blocked", code: "!!", bg: theme.blockedBG},
		{status: "Done", code: "OK", bg: theme.doneBG},
		{status: "WontDo", code: "NO", bg: theme.wontDoBG},
		{status: "Todo", code: "..", bg: theme.nextBG},
	}

	for _, tc := range cases {
		if got := boardStatusCode(tc.status); got != tc.code {
			t.Fatalf("status %q: expected code %q, got %q", tc.status, tc.code, got)
		}
		if got := boardStatusPalette(theme, tc.status); got != tc.bg {
			t.Fatalf("status %q: expected palette %q, got %q", tc.status, tc.bg, got)
		}
	}
}

func TestBoardListRowShowsIssueIDAndScoreOnWideRows(t *testing.T) {
	t.Parallel()

	row := boardIssueRow{
		Issue: store.Issue{
			ID:     "mem-abcd123",
			Status: "InProgress",
			Title:  "Refactor the giant file",
		},
		Score: 7,
	}

	got := boardListRow(row, true, 80)
	for _, want := range []string{"mem-abcd123", "Refactor the giant file", "s7"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected wide board row to contain %q, got %q", want, got)
		}
	}
}

func TestBoardListRowKeepsIssueIDVisibleOnNarrowRows(t *testing.T) {
	t.Parallel()

	row := boardIssueRow{
		Issue: store.Issue{
			ID:     "mem-abcd123",
			Status: "Todo",
			Title:  "A narrow pane should still keep the issue id visible",
		},
	}

	for _, width := range []int{24, 32} {
		got := boardListRow(row, false, width)
		if !strings.Contains(got, "abcd12") {
			t.Fatalf("width %d: expected narrow row to keep short issue id visible, got %q", width, got)
		}
	}
}

func TestBoardLaneMembershipTokenMarksReadyAndActiveRows(t *testing.T) {
	t.Parallel()

	ready := boardIssueRow{Issue: store.Issue{Status: "Todo"}}
	active := boardIssueRow{Issue: store.Issue{Status: "InProgress"}}
	blocked := boardIssueRow{Issue: store.Issue{Status: "Blocked"}}

	if got := boardLaneMembershipToken(boardLaneReady, ready); got != "R" {
		t.Fatalf("expected ready token R, got %q", got)
	}
	if got := boardLaneMembershipToken(boardLaneReady, active); got != "." {
		t.Fatalf("expected non-ready context token ., got %q", got)
	}
	if got := boardLaneMembershipToken(boardLaneActive, active); got != "A" {
		t.Fatalf("expected active token A, got %q", got)
	}
	if got := boardLaneMembershipToken(boardLaneActive, blocked); got != "." {
		t.Fatalf("expected non-active context token ., got %q", got)
	}
}

func TestBoardRowForegroundUsesIssueTypePalette(t *testing.T) {
	t.Parallel()

	theme := boardTheme{
		detailFG: "0",
		epicFG:   "1",
		storyFG:  "2",
		taskFG:   "3",
		bugFG:    "4",
	}

	cases := []struct {
		issueType string
		want      string
	}{
		{issueType: "Epic", want: theme.epicFG},
		{issueType: "Story", want: theme.storyFG},
		{issueType: "Task", want: theme.taskFG},
		{issueType: "Bug", want: theme.bugFG},
		{issueType: "Unknown", want: theme.detailFG},
	}

	for _, tc := range cases {
		if got := boardRowForeground(theme, boardIssueRow{Issue: store.Issue{Type: tc.issueType}}); got != tc.want {
			t.Fatalf("type %q: expected %q, got %q", tc.issueType, tc.want, got)
		}
	}
}

func TestBoardLaneRowStyleUsesDistinctHistoricalPalette(t *testing.T) {
	t.Parallel()

	theme := boardTheme{
		detailFG: "0;0;0",
		taskFG:   "13;14;15",
		wontDoFG: "7;8;9",
		wontDoBG: "10;11;12",
	}

	doneFG, doneBG, doneBold, doneDim := boardLaneRowStyle(theme, boardLaneReady, boardIssueRow{
		Issue: store.Issue{Type: "Task", Status: "Done"},
	})
	if doneFG != theme.taskFG || doneBG != "" || doneBold || !doneDim {
		t.Fatalf("expected done context row to keep its type color and dim the row, got fg=%q bg=%q bold=%v dim=%v", doneFG, doneBG, doneBold, doneDim)
	}

	todoFG, todoBG, todoBold, todoDim := boardLaneRowStyle(theme, boardLaneReady, boardIssueRow{
		Issue: store.Issue{Type: "Task", Status: "Todo"},
	})
	if todoFG != theme.taskFG || todoBG != "" || !todoBold || todoDim {
		t.Fatalf("expected matching lane row to keep primary palette, got fg=%q bg=%q bold=%v dim=%v", todoFG, todoBG, todoBold, todoDim)
	}
}

func TestBoardThemePaintLineHonorsColorMode(t *testing.T) {
	t.Parallel()

	plainTheme := boardTheme{colors: false}
	if got := plainTheme.paintLine("1;2;3", "4;5;6", true, "hello"); got != "hello" {
		t.Fatalf("expected plain paintLine to return raw value, got %q", got)
	}

	colorTheme := boardTheme{colors: true}
	got := colorTheme.paintLine("1;2;3", "4;5;6", true, "hello")
	if !strings.Contains(got, "hello") {
		t.Fatalf("expected lipgloss-rendered content to preserve text, got %q", got)
	}
	dimmed := colorTheme.paintLineStyled("1;2;3", "", false, true, "hello")
	if !strings.Contains(dimmed, "hello") {
		t.Fatalf("expected dimmed lipgloss render to preserve text, got %q", dimmed)
	}
}

func TestBoardVisualHelpersUseRenderedWidthInsteadOfBytes(t *testing.T) {
	t.Parallel()

	if got := visualWidth("éé"); got != 2 {
		t.Fatalf("expected unicode width 2, got %d", got)
	}
	if got := visualWidth("\x1b[31méé\x1b[0m"); got != 2 {
		t.Fatalf("expected ANSI-stripped unicode width 2, got %d", got)
	}
	if got := padRight("é", 3); visualWidth(got) != 3 {
		t.Fatalf("expected padded unicode width 3, got %d (%q)", visualWidth(got), got)
	}
	if got := truncateBoardLine("éééééé", 5); got != "éé..." {
		t.Fatalf("expected unicode truncation to respect rendered width, got %q", got)
	}
	if got := wrapText("éééé é", 10); len(got) != 1 || got[0] != "éééé é" {
		t.Fatalf("expected unicode text to stay on one rendered-width line, got %#v", got)
	}
}

func TestTrimVisualPreservesANSIWhenValueAlreadyFits(t *testing.T) {
	t.Parallel()

	theme := boardTheme{colors: true}
	styled := theme.paintLine("1;2;3", "4;5;6", true, "hello")

	got := trimVisual(styled, 5)
	if got != styled {
		t.Fatalf("expected trimVisual to preserve styled value when it already fits, got %q", got)
	}
}

func TestBoardSearchHighlightedIDUsesLipGlossHighlighting(t *testing.T) {
	t.Parallel()

	theme := boardTheme{colors: true}
	got := boardSearchHighlightedID("mem-abcd123", "abcd", theme)
	if stripped := ansi.Strip(got); stripped != "mem-abcd123" {
		t.Fatalf("expected highlighted id to preserve id text, got %q", stripped)
	}
}

func TestBoardReasonHelpersOrderAndCompactTags(t *testing.T) {
	t.Parallel()

	reasons := []string{
		"todo work remains",
		"open loop requires follow-up",
		"priority P1 keeps this near the top",
		"matches the agent's active focus",
		"open loop requires follow-up",
	}

	ordered := orderBoardReasons(reasons)
	if ordered[0] != "matches the agent's active focus" {
		t.Fatalf("expected focus reason first, got %q", ordered[0])
	}
	if ordered[1] != "open loop requires follow-up" {
		t.Fatalf("expected open loop reason second, got %q", ordered[1])
	}

	tags := boardReasonTags(reasons)
	want := []string{"focus", "loop", "p1", "+1 more"}
	if len(tags) != len(want) {
		t.Fatalf("expected %d tags, got %d (%v)", len(want), len(tags), tags)
	}
	for i, tag := range want {
		if tags[i] != tag {
			t.Fatalf("expected tag[%d]=%q, got %q", i, tag, tags[i])
		}
	}
}

func TestBoardReasonTagCoversKnownMappings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		reason string
		want   string
	}{
		{reason: "required gate(s) are failing", want: "fail"},
		{reason: "required gate(s) are blocked", want: "blocked"},
		{reason: "required gate(s) still need evaluation", want: "gates"},
		{reason: "issue packet is ready", want: "fresh"},
		{reason: "fresh issue packet arrived", want: "fresh"},
		{reason: "packet is stale", want: "stale"},
		{reason: "priority P0 item", want: "p0"},
		{reason: "priority P2 item", want: "p2"},
		{reason: "in-progress work is underway", want: "active"},
		{reason: "todo work remains", want: "todo"},
		{reason: "implementation-ready handoff", want: "task"},
		{reason: "operational value bugfix", want: "bug"},
		{reason: "can start immediately with no blockers", want: "standalone"},
		{reason: "unrecognized reason", want: ""},
	}

	for _, tc := range cases {
		if got := boardReasonTag(tc.reason); got != tc.want {
			t.Fatalf("reason %q: expected %q, got %q", tc.reason, tc.want, got)
		}
	}
}

func TestBoardStatusAndViewportHelpersCoverAsyncBranches(t *testing.T) {
	t.Parallel()

	started := boardBeginAsyncLoad(boardAsyncLoadState{})
	if !started.loading || started.stale || started.err != "" {
		t.Fatalf("expected fresh async load start, got %+v", started)
	}

	now := time.Date(2026, time.March, 22, 19, 15, 0, 0, time.UTC)
	startedWithHistory := boardBeginAsyncLoad(boardAsyncLoadState{lastSuccessAt: now, err: "old"})
	if !startedWithHistory.loading || !startedWithHistory.stale || startedWithHistory.err != "" {
		t.Fatalf("expected historical async load start to mark stale and clear err, got %+v", startedWithHistory)
	}

	succeeded := boardSucceedAsyncLoad(startedWithHistory, now.Add(time.Minute))
	if succeeded.loading || succeeded.stale || succeeded.err != "" || succeeded.lastSuccessAt.IsZero() {
		t.Fatalf("expected async success state, got %+v", succeeded)
	}

	failed := boardFailAsyncLoad(boardAsyncLoadState{lastSuccessAt: now}, errors.New("boom"))
	if failed.loading || !failed.stale || failed.err != "boom" {
		t.Fatalf("expected failed async state to preserve stale context, got %+v", failed)
	}

	model := boardSetToast(boardTUIModel{}, boardToastToneWarn, "  refresh failed  ")
	if model.toast.message != "refresh failed" || model.toast.tone != boardToastToneWarn || model.toast.id == 0 {
		t.Fatalf("expected toast to be trimmed and assigned, got %+v", model.toast)
	}
	unchanged := boardSetToast(model, boardToastToneInfo, "   ")
	if !reflect.DeepEqual(unchanged, model) {
		t.Fatalf("expected blank toast message to leave model unchanged, got %+v", unchanged)
	}
	cleared := boardClearToast(model, model.toast.id)
	if cleared.toast.message != "" || cleared.toast.id != 0 {
		t.Fatalf("expected toast clear to remove active toast, got %+v", cleared.toast)
	}
	if boardClearToast(model, model.toast.id+1).toast.message == "" {
		t.Fatalf("expected mismatched toast clear to preserve toast")
	}

	if got := boardCurrentPanelScroll(boardTUIModel{}); got != 0 {
		t.Fatalf("expected nil panel scroll map to read as zero, got %d", got)
	}

	scrollModel := boardTUIModel{selectedIssue: "mem-a111111", panelMode: boardPanelModeContinuity}
	scrollModel = boardSetCurrentPanelScroll(scrollModel, -3)
	if got := boardCurrentPanelScroll(scrollModel); got != 0 {
		t.Fatalf("expected negative scroll offset to clamp to zero, got %d", got)
	}
	scrollModel = boardSetCurrentPanelScroll(scrollModel, 7)
	if got := boardCurrentPanelScroll(scrollModel); got != 7 {
		t.Fatalf("expected stored panel scroll offset 7, got %d", got)
	}
	noIssueModel := boardSetCurrentPanelScroll(boardTUIModel{}, 9)
	if len(noIssueModel.panelScroll) != 0 {
		t.Fatalf("expected empty selected issue to skip scroll persistence, got %+v", noIssueModel.panelScroll)
	}

	if got := boardPanelScrollKey(" mem-a111111 ", boardPanelModeContinuity); got != "continuity:mem-a111111" {
		t.Fatalf("unexpected panel scroll key %q", got)
	}

	window, offset, label := boardViewportWindow(nil, 0, 2)
	if window != nil || offset != 0 || label != "" {
		t.Fatalf("expected empty viewport for non-positive height, got window=%v offset=%d label=%q", window, offset, label)
	}
	window, offset, label = boardViewportWindow(nil, 2, 0)
	if len(window) != 2 || offset != 0 || label != "" {
		t.Fatalf("expected padded empty viewport, got window=%v offset=%d label=%q", window, offset, label)
	}
	window, offset, label = boardViewportWindow([]string{"a", "b", "c", "d"}, 2, 9)
	if offset != 2 || label != "3-4/4" || !reflect.DeepEqual(window, []string{"c", "d"}) {
		t.Fatalf("expected clamped viewport window, got window=%v offset=%d label=%q", window, offset, label)
	}

	if got := padLines([]string{"a"}, 3); !reflect.DeepEqual(got, []string{"a", "", ""}) {
		t.Fatalf("unexpected padded lines %#v", got)
	}

	if got := boardPanelSubtitle("Audit", []string{"snapshot stale", "audit failed"}, "3-4/8"); got != "Audit · snapshot stale · audit failed · scroll 3-4/8" {
		t.Fatalf("unexpected panel subtitle %q", got)
	}

	loadModel := boardTUIModel{
		snapshotLoad: boardAsyncLoadState{loading: true, stale: true},
		auditLoad:    boardAsyncLoadState{err: "boom"},
		spinner:      newBoardSpinner(),
	}
	theme := defaultBoardTheme(false)
	if got := boardInspectorStatusTokens(loadModel, theme); !reflect.DeepEqual(got, []string{"snapshot stale |", "audit failed"}) {
		t.Fatalf("unexpected inspector status tokens %#v", got)
	}

	if got := boardLoadToken("snapshot", boardAsyncLoadState{loading: true}, spinner.New(spinner.WithSpinner(spinner.Line))); got != "snapshot |" {
		t.Fatalf("unexpected active load token %q", got)
	}
	if got := boardLoadToken("snapshot", boardAsyncLoadState{stale: true}, spinner.New(spinner.WithSpinner(spinner.Line))); got != "snapshot stale" {
		t.Fatalf("unexpected stale load token %q", got)
	}
	if got := boardLoadToken("snapshot", boardAsyncLoadState{}, spinner.New(spinner.WithSpinner(spinner.Line))); got != "" {
		t.Fatalf("expected empty load token, got %q", got)
	}

	if got := boardSnapshotStatusLine(boardAsyncLoadState{loading: true, stale: true}); !strings.Contains(got, "last successful snapshot") {
		t.Fatalf("unexpected stale snapshot status %q", got)
	}
	if got := boardSnapshotStatusLine(boardAsyncLoadState{loading: true}); got != "board refresh is in progress" {
		t.Fatalf("unexpected active snapshot status %q", got)
	}
	if got := boardSnapshotStatusLine(boardAsyncLoadState{err: "boom"}); !strings.Contains(got, "new retry will happen automatically") {
		t.Fatalf("unexpected failed snapshot status %q", got)
	}

	if got := boardAuditStatusLine(boardAsyncLoadState{loading: true, stale: true}); !strings.Contains(got, "last successful audit") {
		t.Fatalf("unexpected stale audit status %q", got)
	}
	if got := boardAuditStatusLine(boardAsyncLoadState{loading: true}); !strings.Contains(got, "loading continuity evidence") {
		t.Fatalf("unexpected active audit status %q", got)
	}
	if got := boardAuditStatusLine(boardAsyncLoadState{err: "boom"}); !strings.Contains(got, "unavailable right now") {
		t.Fatalf("unexpected failed audit status %q", got)
	}
}

func TestBoardPanelGeometryAndLoadingHelpers(t *testing.T) {
	t.Parallel()

	issue := boardTestIssue("mem-a111111", "Task", "Todo", "Geometry")
	issue.Description = strings.Repeat("geometry coverage text ", 20)
	model := newBoardTUIModel(boardSnapshot{
		Ready: []boardIssueRow{{Issue: issue}},
	}, 120, 20)
	model.lane = boardLaneReady
	model.detailOpen = true
	model.snapshotLoad.loading = true
	model = boardNormalizeModel(model)

	if !boardAnyLoading(model) {
		t.Fatal("expected loading helper to report active snapshot load")
	}
	if got := boardCurrentPanelInnerWidth(model); got != 44 {
		t.Fatalf("expected wide inspector width 44, got %d", got)
	}
	model.toast = boardToast{message: "toast"}
	if got := boardCurrentPanelBodyHeight(model); got != 12 {
		t.Fatalf("expected toast to reduce panel body height to 12, got %d", got)
	}

	model = boardSetCurrentPanelScroll(model, 999)
	model = boardClampPanelScroll(model)
	if got := boardCurrentPanelScroll(model); got != boardMaxPanelScroll(model) {
		t.Fatalf("expected clamped panel scroll, got %d want %d", got, boardMaxPanelScroll(model))
	}

	adjusted := boardAdjustPanelScroll(model, -999)
	if got := boardCurrentPanelScroll(adjusted); got != 0 {
		t.Fatalf("expected adjusted scroll to clamp at zero, got %d", got)
	}

	model.detailOpen = false
	if got := boardMaxPanelScroll(model); got != 0 {
		t.Fatalf("expected closed panel max scroll 0, got %d", got)
	}
}

func TestBoardContinuityHelpersCoverRemainingStates(t *testing.T) {
	t.Parallel()

	if got := boardSessionLifecycle(store.Session{}); got != "active" {
		t.Fatalf("expected empty session lifecycle active, got %q", got)
	}
	if got := boardSessionLifecycle(store.Session{EndedAt: "2026-03-22T19:00:00Z"}); got != "closed" {
		t.Fatalf("expected ended session lifecycle closed, got %q", got)
	}

	if got := boardContinuityStatusLabel("fallback"); got != "fallback" {
		t.Fatalf("unexpected fallback label %q", got)
	}
	if got := boardContinuityStatusLabel("stale"); got != "stale" {
		t.Fatalf("unexpected stale label %q", got)
	}
	if got := boardContinuityStatusLabel("ambiguous"); got != "amb" {
		t.Fatalf("unexpected ambiguous label %q", got)
	}
	if got := boardContinuityStatusLabel("missing"); got != "miss" {
		t.Fatalf("unexpected missing label %q", got)
	}
	if got := boardContinuityStatusLabel("fresh"); got != "fresh" {
		t.Fatalf("unexpected fresh label %q", got)
	}

	if got := boardYesNo(true); got != "Y" {
		t.Fatalf("expected boardYesNo(true)=yes, got %q", got)
	}
	if got := boardYesNo(false); got != "N" {
		t.Fatalf("expected boardYesNo(false)=no, got %q", got)
	}
}

func TestBoardContinuityNarrativeHelpersCoverAdditionalBranches(t *testing.T) {
	t.Parallel()

	ambiguous := store.ContinuityAuditSnapshot{
		Alerts: []store.ContinuityAlert{{Code: "multiple-open-sessions"}},
		Resolution: store.ContinuityResolution{
			Source: "latest-open-issue",
		},
	}
	if got := boardContinuityVerdict(ambiguous); !strings.Contains(got, "ambiguous") {
		t.Fatalf("unexpected ambiguous verdict %q", got)
	}
	if got := boardContinuityResumeSource(ambiguous); !strings.Contains(got, "latest open session") {
		t.Fatalf("unexpected ambiguous resume source %q", got)
	}
	if got := boardContinuityActionSummary(ambiguous); !strings.Contains(got, "keep resume explicit") {
		t.Fatalf("unexpected ambiguous action summary %q", got)
	}

	stale := store.ContinuityAuditSnapshot{
		Alerts: []store.ContinuityAlert{{Code: "issue-packet-stale"}},
		Issue:  store.IssueContinuitySnapshot{IssueID: "mem-a111111"},
	}
	if got := boardContinuityVerdict(stale); !strings.Contains(got, "stale") {
		t.Fatalf("unexpected stale verdict %q", got)
	}
	if got := boardContinuityActionSummary(stale); !strings.Contains(got, "refresh the issue packet") {
		t.Fatalf("unexpected stale action summary %q", got)
	}
	commands := boardContinuityRecommendedCommands(stale)
	if len(commands) != 1 || !strings.Contains(commands[0], "context packet build --scope issue") {
		t.Fatalf("unexpected stale commands %#v", commands)
	}

	packetSession := store.ContinuitySessionCandidate{HasPacket: true}
	if got := boardContinuitySessionValueLine(packetSession); !strings.Contains(got, "saved session packet") {
		t.Fatalf("unexpected packet session value line %q", got)
	}
	summarySession := store.ContinuitySessionCandidate{HasSummary: true}
	if got := boardContinuitySessionValueLine(summarySession); !strings.Contains(got, "summary exists") {
		t.Fatalf("unexpected summary session value line %q", got)
	}

	missing := store.ContinuityAuditSnapshot{Resolution: store.ContinuityResolution{Status: "missing"}}
	if got := boardContinuityVerdict(missing); !strings.Contains(got, "little saved state") {
		t.Fatalf("unexpected missing verdict %q", got)
	}

	fallback := store.ContinuityAuditSnapshot{Resolution: store.ContinuityResolution{Status: "fallback", Source: "agent-focus-issue-open"}}
	if got := boardContinuityVerdict(fallback); !strings.Contains(got, "fallback context") {
		t.Fatalf("unexpected fallback verdict %q", got)
	}
	if got := boardContinuityResumeSource(fallback); !strings.Contains(got, "latest open session") {
		t.Fatalf("unexpected fallback resume source %q", got)
	}

	packetOnly := store.ContinuityAuditSnapshot{
		Issue: store.IssueContinuitySnapshot{
			HasPacket: true,
			LatestPacket: store.RehydratePacket{
				PacketID: "pkt-issue-1",
			},
		},
	}
	if got := boardContinuityResumeSource(packetOnly); !strings.Contains(got, "saved issue packet") {
		t.Fatalf("unexpected packet-only resume source %q", got)
	}
	if got := boardContinuityStopNowLine(packetOnly); !strings.Contains(got, "saved issue packet") {
		t.Fatalf("unexpected packet-only stop-now line %q", got)
	}

	activeSession := store.ContinuitySessionCandidate{Lifecycle: "active"}
	if got := boardContinuitySessionValueLine(activeSession); !strings.Contains(got, "only raw session context") {
		t.Fatalf("unexpected active session value line %q", got)
	}

	if got := boardContinuityResumeSource(store.ContinuityAuditSnapshot{Resolution: store.ContinuityResolution{Source: "agent-focus-session"}}); !strings.Contains(got, "already tied to this agent's focus") {
		t.Fatalf("unexpected focus-session resume source %q", got)
	}
	if got := boardContinuityResumeSource(store.ContinuityAuditSnapshot{Resolution: store.ContinuityResolution{Source: "agent-focus-issue-latest"}}); !strings.Contains(got, "latest historical session") {
		t.Fatalf("unexpected focus-latest resume source %q", got)
	}
	if got := boardContinuityResumeSource(store.ContinuityAuditSnapshot{Resolution: store.ContinuityResolution{Source: "latest-session-issue"}}); !strings.Contains(got, "latest historical session for this issue") {
		t.Fatalf("unexpected latest-session resume source %q", got)
	}
}

func TestBoardContinuityEvidenceLinesCoverAlertsAlternatesAndPackets(t *testing.T) {
	t.Parallel()

	audit := store.ContinuityAuditSnapshot{
		Alerts: []store.ContinuityAlert{
			{Code: "a1", Message: "first alert"},
			{Code: "a2", Message: "second alert"},
			{Code: "a3", Message: "third alert"},
		},
		Sessions: []store.ContinuitySessionCandidate{
			{
				Session:    store.Session{SessionID: "sess-selected"},
				Lifecycle:  "active",
				IsSelected: true,
			},
			{
				Session:   store.Session{SessionID: "sess-alt"},
				Lifecycle: "closed",
			},
		},
		IssuePackets: []store.ContinuityPacketCandidate{
			{Packet: store.RehydratePacket{PacketID: "pkt-issue-1"}, Status: "fresh"},
		},
		SessionPackets: []store.ContinuityPacketCandidate{
			{Packet: store.RehydratePacket{PacketID: "pkt-session-1"}, Status: "active"},
		},
		RecentWrites: []store.ContinuityWrite{
			{EventType: "session.checkpointed", CreatedAt: "2026-03-22T18:00:00Z"},
			{EventType: "session.summarized", CreatedAt: "2026-03-22T18:05:00Z"},
			{EventType: "packet.built", CreatedAt: "2026-03-22T18:10:00Z"},
		},
	}

	lines := boardContinuityEvidenceLines(audit, 80)
	for _, want := range []string{
		"alert: first alert",
		"alerts: +1 more",
		"candidate sessions: 2",
		"alternate: sess-alt (closed)",
		"issue packet: pkt-issue-1 (fresh)",
		"session packet: pkt-session-1 (active)",
		"recent: session.checkpointed",
		"recent: session.summarized",
	} {
		found := false
		for _, line := range lines {
			if strings.Contains(line, want) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected evidence lines to contain %q, got %#v", want, lines)
		}
	}
}

func TestBoardCompactFormattingHelpersCoverFallbackBranches(t *testing.T) {
	t.Parallel()

	if got := formatBoardSummaryCompact(boardSummary{Total: 5, InProgress: 2, Blocked: 1, Todo: 1, WontDo: 1}); got != "T5 I2 B1 R1 W1" {
		t.Fatalf("unexpected compact summary %q", got)
	}
	if got := boardCompactStatusLabel("Blocked"); got != "BLK" {
		t.Fatalf("unexpected blocked compact label %q", got)
	}
	if got := boardCompactStatusLabel("Done"); got != "DONE" {
		t.Fatalf("unexpected done compact label %q", got)
	}
	if got := boardCompactStatusLabel("WontDo"); got != "NO" {
		t.Fatalf("unexpected wontdo compact label %q", got)
	}
	if got := boardCompactStatusLabel("Todo"); got != "TODO" {
		t.Fatalf("unexpected todo compact label %q", got)
	}
	if got := boardHierarchyToggleToken(true); got != "[-] " {
		t.Fatalf("unexpected expanded hierarchy toggle %q", got)
	}
	if got := boardHierarchyToggleToken(false); got != "[+] " {
		t.Fatalf("unexpected collapsed hierarchy toggle %q", got)
	}
}

func TestBoardDetailActionLineAndSnapshotStatusHelpersCoverBranches(t *testing.T) {
	t.Parallel()

	withReasons := boardIssueRow{
		Issue:   boardTestIssue("mem-a111111", "Task", "Todo", "Ready"),
		Reasons: []string{"matches the agent's active focus", "todo work remains"},
	}
	if got := boardDetailActionLine(withReasons, 80); !strings.Contains(got, "active focus") {
		t.Fatalf("unexpected detail action line with reasons %q", got)
	}

	cases := []struct {
		status string
		want   string
	}{
		{status: "InProgress", want: "keep continuity current"},
		{status: "Blocked", want: "inspect the blocker"},
		{status: "Done", want: "done and kept here"},
		{status: "WontDo", want: "won't do and kept here"},
		{status: "Todo", want: "review the scope and acceptance"},
	}
	for _, tc := range cases {
		got := boardDetailActionLine(boardIssueRow{Issue: boardTestIssue("mem-b222222", "Task", tc.status, "Any")}, 80)
		if !strings.Contains(got, tc.want) {
			t.Fatalf("status %q: expected %q in %q", tc.status, tc.want, got)
		}
	}

	snapshot := boardSnapshot{
		Active:  []boardIssueRow{{Issue: boardTestIssue("mem-a111111", "Task", "InProgress", "Active")}},
		Blocked: []boardIssueRow{{Issue: boardTestIssue("mem-b222222", "Task", "Blocked", "Blocked")}},
		Ready:   []boardIssueRow{{Issue: boardTestIssue("mem-c333333", "Task", "Todo", "Ready")}},
		Done:    []boardIssueRow{{Issue: boardTestIssue("mem-d444444", "Task", "Done", "Done")}},
		WontDo:  []boardIssueRow{{Issue: boardTestIssue("mem-e555555", "Task", "WontDo", "No")}},
	}
	statusCases := map[string]string{
		"InProgress": "mem-a111111",
		"Blocked":    "mem-b222222",
		"Todo":       "mem-c333333",
		"Done":       "mem-d444444",
		"WontDo":     "mem-e555555",
	}
	for status, wantID := range statusCases {
		rows := rawSnapshotRowsForStatus(snapshot, status)
		if len(rows) != 1 || rows[0].Issue.ID != wantID {
			t.Fatalf("status %q: unexpected rows %+v", status, rows)
		}
	}
	if rows := rawSnapshotRowsForStatus(snapshot, "Unknown"); rows != nil {
		t.Fatalf("expected unknown status rows nil, got %+v", rows)
	}
}

func TestBoardHierarchyAndMetaFormattingHelpersCoverBranches(t *testing.T) {
	t.Parallel()

	if got := boardCompactHierarchyPath([]string{"mem-a111111"}, 40, true); got != "mem-a111111" {
		t.Fatalf("unexpected single-item compact hierarchy path %q", got)
	}
	if got := boardCompactHierarchyPath([]string{"mem-a111111", "mem-b222222"}, 24, true); !strings.Contains(got, "... > mem-b222222") {
		t.Fatalf("unexpected elided compact hierarchy path %q", got)
	}
	if got := boardCompactHierarchyPath([]string{"mem-a111111", "mem-b222222", "mem-c333333"}, 120, false); got != "mem-a111111 > mem-b222222 > mem-c333333" {
		t.Fatalf("unexpected full hierarchy path %q", got)
	}

	theme := defaultBoardTheme(false)
	if got := boardMetaOverflowToken(theme, 1); !strings.Contains(got, "+1 more") {
		t.Fatalf("unexpected singular overflow token %q", got)
	}
	if got := boardMetaOverflowToken(theme, 3); !strings.Contains(got, "+3 more") {
		t.Fatalf("unexpected plural overflow token %q", got)
	}

	statusCases := map[string]string{
		"InProgress": "in progress",
		"Blocked":    "blocked",
		"Done":       "done",
		"WontDo":     "won't do",
		"Todo":       "todo",
	}
	for status, want := range statusCases {
		if got := boardExpandedStatusLabel(status); got != want {
			t.Fatalf("status %q: expected %q, got %q", status, want, got)
		}
	}
}

func TestBoardLaneAndTabsHelpersCoverAdditionalBranches(t *testing.T) {
	t.Parallel()

	theme := boardTheme{
		detailFG: "0;0;0",
		taskFG:   "1;2;3",
		wontDoFG: "4;5;6",
	}
	contextWontDo := boardIssueRow{Issue: store.Issue{Type: "Task", Status: "WontDo"}}
	if got := boardLaneRowForeground(theme, boardLaneReady, contextWontDo); got != theme.wontDoFG {
		t.Fatalf("expected wontdo context row to use wontdo fg, got %q", got)
	}
	plainTask := boardIssueRow{Issue: store.Issue{Type: "Task", Status: "Todo"}}
	if got := boardLaneRowForeground(theme, boardLaneReady, plainTask); got != theme.taskFG {
		t.Fatalf("expected regular task row to keep task fg, got %q", got)
	}

	model := newBoardTUIModel(boardSnapshot{
		LikelyNext: []boardIssueRow{{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Next")}},
		Active:     []boardIssueRow{{Issue: boardTestIssue("mem-b222222", "Task", "InProgress", "Active")}},
		Blocked:    []boardIssueRow{{Issue: boardTestIssue("mem-c333333", "Bug", "Blocked", "Blocked")}},
		Ready:      []boardIssueRow{{Issue: boardTestIssue("mem-d444444", "Task", "Todo", "Ready")}},
		Done:       []boardIssueRow{{Issue: boardTestIssue("mem-e555555", "Task", "Done", "Done")}},
		WontDo:     []boardIssueRow{{Issue: boardTestIssue("mem-f666666", "Bug", "WontDo", "No")}},
	}, 120, 24)
	model.lane = boardLaneReady
	model.showHistory = true
	model = boardNormalizeModel(model)

	if got := formatBoardTabsCompact(model, 28); !strings.Contains(got, "READY 1") {
		t.Fatalf("expected very narrow tab summary to lead with lane title, got %q", got)
	}
	if got := formatBoardTabsCompact(model, 72); !strings.Contains(got, "all") || !strings.Contains(got, "D1") || !strings.Contains(got, "W1") {
		t.Fatalf("expected wide tab summary to include history counts, got %q", got)
	}
}

func TestBoardThemeAndColorHelpersCoverRemainingBranches(t *testing.T) {
	t.Parallel()

	colorTheme := defaultBoardTheme(true)
	if got := boardLipGlossColor("255;16;0"); got != "#ff1000" {
		t.Fatalf("unexpected RGB conversion %q", got)
	}
	if got := boardLipGlossColor("not-a-color"); got != "not-a-color" {
		t.Fatalf("expected invalid color to pass through, got %q", got)
	}
	if got := boardLipGlossColor("300;-1;oops"); got != "#ff0000" {
		t.Fatalf("expected clamped color conversion, got %q", got)
	}

	style := colorTheme.lineStyle(colorTheme.accentFG, colorTheme.panelAltBG, true, true)
	rendered := style.Render("chip")
	if !strings.Contains(rendered, "chip") {
		t.Fatalf("expected line style render to contain content, got %q", rendered)
	}

	if got := ansi.Strip(colorTheme.rule(5)); got != "·····" {
		t.Fatalf("unexpected rule rendering %q", ansi.Strip(colorTheme.rule(5)))
	}
}

func TestBoardToastLineAndColorModeBranches(t *testing.T) {
	theme := defaultBoardTheme(false)
	model := boardTUIModel{}
	if got := boardToastLine(model, theme, 30); visualWidth(got) != 30 {
		t.Fatalf("expected empty toast line width 30, got %d (%q)", visualWidth(got), got)
	}

	model.toast = boardToast{message: "saved", tone: boardToastToneSuccess}
	if got := ansi.Strip(boardToastLine(model, theme, 30)); !strings.Contains(got, "saved") {
		t.Fatalf("expected success toast line to contain message, got %q", got)
	}
	model.toast = boardToast{message: "warn", tone: boardToastToneWarn}
	if got := ansi.Strip(boardToastLine(model, theme, 30)); !strings.Contains(got, "warn") {
		t.Fatalf("expected warn toast line to contain message, got %q", got)
	}

	original := boardTUISupportsInteractive
	t.Cleanup(func() {
		boardTUISupportsInteractive = original
	})

	t.Setenv("MEMORI_COLOR", "always")
	if !boardTUIShouldUseColor(&bytes.Buffer{}) {
		t.Fatal("expected MEMORI_COLOR=always to force color")
	}
	t.Setenv("MEMORI_COLOR", "never")
	if boardTUIShouldUseColor(&bytes.Buffer{}) {
		t.Fatal("expected MEMORI_COLOR=never to disable color")
	}
	t.Setenv("MEMORI_COLOR", "")
	t.Setenv("NO_COLOR", "")
	t.Setenv("CLICOLOR", "0")
	if boardTUIShouldUseColor(&bytes.Buffer{}) {
		t.Fatal("expected CLICOLOR=0 to disable color")
	}
	t.Setenv("CLICOLOR", "")
	t.Setenv("CLICOLOR_FORCE", "1")
	if !boardTUIShouldUseColor(&bytes.Buffer{}) {
		t.Fatal("expected CLICOLOR_FORCE to enable color")
	}
	t.Setenv("CLICOLOR_FORCE", "")
	t.Setenv("FORCE_COLOR", "1")
	if !boardTUIShouldUseColor(&bytes.Buffer{}) {
		t.Fatal("expected FORCE_COLOR to enable color")
	}
	t.Setenv("FORCE_COLOR", "")
	t.Setenv("TERM", "dumb")
	boardTUISupportsInteractive = func(io.Writer) bool { return false }
	if boardTUIShouldUseColor(&bytes.Buffer{}) {
		t.Fatal("expected TERM=dumb to disable color")
	}
}

func TestBoardLayoutHelpersHandleEnvAndWidths(t *testing.T) {
	t.Setenv("COLUMNS", "120")
	if got := boardRenderWidth(); got != 120 {
		t.Fatalf("expected COLUMNS width 120, got %d", got)
	}

	t.Setenv("COLUMNS", "20")
	if got := boardRenderWidth(); got != 80 {
		t.Fatalf("expected fallback width 80 for narrow columns, got %d", got)
	}

	t.Setenv("COLUMNS", "not-a-number")
	if got := boardRenderWidth(); got != 80 {
		t.Fatalf("expected fallback width 80 for invalid columns, got %d", got)
	}

	if got := boardSectionLimit(40); got != 2 {
		t.Fatalf("expected compact section limit 2, got %d", got)
	}
	if got := boardSectionLimit(70); got != 3 {
		t.Fatalf("expected mid section limit 3, got %d", got)
	}
	if got := boardSectionLimit(120); got != 5 {
		t.Fatalf("expected wide section limit 5, got %d", got)
	}

	if got := boardLikelyNextLimit(40); got != 1 {
		t.Fatalf("expected compact next limit 1, got %d", got)
	}
	if got := boardLikelyNextLimit(70); got != 2 {
		t.Fatalf("expected mid next limit 2, got %d", got)
	}
	if got := boardLikelyNextLimit(120); got != 3 {
		t.Fatalf("expected wide next limit 3, got %d", got)
	}
}

func TestBoardFieldAndLineHelpersRenderFallbacks(t *testing.T) {
	t.Parallel()

	if got := truncateBoardLine("abcdef", 3); got != "abc" {
		t.Fatalf("expected hard truncate for width 3, got %q", got)
	}
	if got := truncateBoardLine("abcdef", 5); got != "ab..." {
		t.Fatalf("expected ellipsis truncate for width 5, got %q", got)
	}
	if got := truncateBoardLine(" abc ", 10); got != "abc" {
		t.Fatalf("expected trim without truncation, got %q", got)
	}

	var out bytes.Buffer
	ui := textUI{out: &out, colors: false}
	boardField(ui, "Summary", "1234567890", 5)
	if got := out.String(); got != "Summary: 12345...\n" {
		t.Fatalf("expected truncated field output, got %q", got)
	}
}

func TestSortBoardRowsUsesRankUpdatedAtAndIDFallbacks(t *testing.T) {
	t.Parallel()

	rows := []boardIssueRow{
		{Issue: store.Issue{ID: "mem-c1c1c1c", UpdatedAt: "2026-03-08T12:00:00Z"}},
		{Issue: store.Issue{ID: "mem-a1a1a1a", UpdatedAt: "2026-03-08T10:00:00Z"}},
		{Issue: store.Issue{ID: "mem-b1b1b1b", UpdatedAt: "2026-03-08T09:00:00Z"}},
		{Issue: store.Issue{ID: "mem-d1d1d1d", UpdatedAt: "2026-03-08T09:00:00Z"}},
	}

	sortBoardRows(rows, map[string]int{
		"mem-c1c1c1c": 2,
		"mem-a1a1a1a": 1,
	})

	got := []string{rows[0].Issue.ID, rows[1].Issue.ID, rows[2].Issue.ID, rows[3].Issue.ID}
	want := []string{"mem-a1a1a1a", "mem-c1c1c1c", "mem-b1b1b1b", "mem-d1d1d1d"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected order %v, got %v", want, got)
		}
	}
}

func TestBoardSnapshotSignatureIgnoresGeneratedAtButTracksContent(t *testing.T) {
	t.Parallel()

	base := boardSnapshot{
		GeneratedAt: "2026-03-08T12:00:00Z",
		Agent:       "agent-1",
		Summary:     boardSummary{Total: 1, Todo: 1},
		Ready: []boardIssueRow{
			{Issue: store.Issue{ID: "mem-a1b2c3d", Title: "Keep signatures stable"}},
		},
	}

	sameContent := base
	sameContent.GeneratedAt = "2026-03-08T12:01:00Z"
	if got, want := boardSnapshotSignature(sameContent), boardSnapshotSignature(base); got != want {
		t.Fatalf("expected GeneratedAt to be ignored in signature, got %q want %q", got, want)
	}

	changedContent := base
	changedContent.Ready = append([]boardIssueRow(nil), base.Ready...)
	changedContent.Ready[0].Issue.Title = "Changed"
	if got, want := boardSnapshotSignature(changedContent), boardSnapshotSignature(base); got == want {
		t.Fatalf("expected content change to alter signature, got %q", got)
	}
}

func TestBoardSnapshotSignatureIgnoresGeneratedAt(t *testing.T) {
	t.Parallel()

	base := boardSnapshot{
		Agent: "agent-1",
		Summary: boardSummary{
			Total:      1,
			InProgress: 1,
		},
		Active: []boardIssueRow{
			{Issue: store.Issue{ID: "mem-a1b2c3d", Title: "Active work"}},
		},
	}

	first := base
	first.GeneratedAt = "2026-03-08T00:00:00Z"
	second := base
	second.GeneratedAt = "2026-03-08T01:00:00Z"

	if sig := boardSnapshotSignature(first); sig == "" {
		t.Fatal("expected non-empty board snapshot signature")
	}
	if got := boardSnapshotSignature(first); got != boardSnapshotSignature(second) {
		t.Fatalf("expected generated_at to be ignored, got %q and %q", got, boardSnapshotSignature(second))
	}
}

func TestBoardReasonOrderAndTagsCoverAdditionalMappings(t *testing.T) {
	t.Parallel()

	reasons := []string{
		"priority P2 work remains",
		"agent already holds the latest recovery packet",
		"required gate(s) are blocked",
		"priority P0 incident response",
	}

	if got := orderBoardReasons(reasons); !reflect.DeepEqual(got[:3], []string{
		"agent already holds the latest recovery packet",
		"required gate(s) are blocked",
		"priority P2 work remains",
	}) {
		t.Fatalf("unexpected additional reason ordering: %#v", got)
	}

	if got := boardReasonTags(reasons); !reflect.DeepEqual(got, []string{"packet", "blocked", "p2", "+1 more"}) {
		t.Fatalf("unexpected additional tags: %#v", got)
	}

	if got := boardReasonTag("priority P1 follow-up"); got != "p1" {
		t.Fatalf("expected P1 tag, got %q", got)
	}
}

func TestRunBoardLoopSkipsUnchangedFramesAndStopsOnCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var out bytes.Buffer
	callCount := 0
	done := make(chan error, 1)
	go func() {
		done <- runBoardLoop(ctx, &out, 5*time.Millisecond, func() (string, string, error) {
			callCount++
			if callCount >= 3 {
				cancel()
			}
			if callCount == 1 {
				return "frame-1", "sig-1", nil
			}
			return "frame-1-again", "sig-1", nil
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runBoardLoop returned error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for runBoardLoop to stop")
	}

	if got := out.String(); got != "frame-1" {
		t.Fatalf("expected only first frame to render, got %q", got)
	}
	if callCount < 3 {
		t.Fatalf("expected repeated render attempts before cancel, got %d", callCount)
	}
}

func TestRunBoardLoopPropagatesRenderErrors(t *testing.T) {
	t.Parallel()

	wantErr := "render failed"
	err := runBoardLoop(context.Background(), &bytes.Buffer{}, time.Millisecond, func() (string, string, error) {
		return "", "", errors.New(wantErr)
	})
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("expected render error %q, got %v", wantErr, err)
	}
}
