package cli

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

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
		activeBG:   "1;2;3",
		blockedBG:  "4;5;6",
		readyBG:    "7;8;9",
		doneBG:     "10;11;12",
		wontDoBG:   "13;14;15",
		nextBG:     "16;17;18",
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
	for _, want := range []string{">>", "mem-abcd123", "Refactor the giant file", "s7"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected wide board row to contain %q, got %q", want, got)
		}
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
	if !strings.HasPrefix(got, "\x1b[1;38;2;1;2;3;48;2;4;5;6m") {
		t.Fatalf("expected ANSI prefix, got %q", got)
	}
	if !strings.HasSuffix(got, "hello\x1b[0m") {
		t.Fatalf("expected ANSI suffix, got %q", got)
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
