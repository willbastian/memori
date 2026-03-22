package cli

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/spinner"
)

type boardPanelContent struct {
	title    string
	subtitle string
	body     []string
}

func boardSidePanel(model boardTUIModel, theme boardTheme, width, height int) []string {
	content := boardSidePanelContent(model, theme, width)
	return boardRenderViewportPanel(model, content, theme, width, height)
}

func boardSidePanelContent(model boardTUIModel, theme boardTheme, width int) boardPanelContent {
	title := "Issue Detail"
	subtitle := "Context"
	body := []string{
		theme.paintLine(theme.mutedFG, theme.panelAltBG, false, padRight(" press <enter> to open the selected issue panel ", width)),
	}
	if !model.detailOpen {
		return boardPanelContent{title: title, subtitle: subtitle, body: body}
	}
	if model.panelMode == boardPanelModeContinuity {
		return boardContinuityPanelContent(model, theme, width)
	}
	return boardDetailPanelContent(model, theme, width)
}

func boardRenderViewportPanel(model boardTUIModel, content boardPanelContent, theme boardTheme, width, height int) []string {
	lines := make([]string, 0, height)
	visibleBody := maxInt(height-1, 0)
	view := boardViewportModelForContent(model, model.panelMode, content.body, width, visibleBody)
	rangeLabel := boardViewportRangeLabel(view)
	body := strings.Split(view.View(), "\n")
	subtitle := boardPanelSubtitle(content.subtitle, boardInspectorStatusTokens(model, theme), rangeLabel)
	lines = append(lines, boardPanelHeader(theme, content.title, subtitle, width))
	lines = append(lines, body...)
	for len(lines) < height {
		lines = append(lines, padRight("", width))
	}
	return lines[:height]
}

func boardViewportWindow(lines []string, visible, offset int) ([]string, int, string) {
	if visible <= 0 {
		return nil, 0, ""
	}
	if len(lines) == 0 {
		return padLines(nil, visible), 0, ""
	}
	maxOffset := maxInt(len(lines)-visible, 0)
	if offset < 0 {
		offset = 0
	}
	if offset > maxOffset {
		offset = maxOffset
	}
	end := minInt(offset+visible, len(lines))
	window := append([]string(nil), lines[offset:end]...)
	window = padLines(window, visible)
	if len(lines) <= visible {
		return window, offset, ""
	}
	return window, offset, fmt.Sprintf("%d-%d/%d", offset+1, end, len(lines))
}

func padLines(lines []string, height int) []string {
	for len(lines) < height {
		lines = append(lines, padRight("", 0))
	}
	return lines
}

func boardPanelSubtitle(base string, tokens []string, rangeLabel string) string {
	parts := make([]string, 0, len(tokens)+2)
	if base != "" {
		parts = append(parts, base)
	}
	parts = append(parts, tokens...)
	if rangeLabel != "" {
		parts = append(parts, "scroll "+rangeLabel)
	}
	return strings.Join(parts, " · ")
}

func boardInspectorStatusTokens(model boardTUIModel, theme boardTheme) []string {
	tokens := make([]string, 0, 2)
	if token := boardLoadToken("snapshot", model.snapshotLoad, boardStyledSpinner(model.spinner, theme)); token != "" {
		tokens = append(tokens, token)
	}
	if token := boardLoadToken("audit", model.auditLoad, boardStyledSpinner(model.spinner, theme)); token != "" {
		tokens = append(tokens, token)
	}
	return tokens
}

func boardLoadToken(label string, state boardAsyncLoadState, spin spinner.Model) string {
	switch {
	case state.loading && state.stale:
		return label + " stale " + spin.View()
	case state.loading:
		return label + " " + spin.View()
	case strings.TrimSpace(state.err) != "" && state.stale:
		return label + " stale"
	case strings.TrimSpace(state.err) != "":
		return label + " failed"
	case state.stale:
		return label + " stale"
	default:
		return ""
	}
}
