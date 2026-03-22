package cli

import (
	"strconv"
	"strings"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
)

func newBoardSearchInput() textinput.Model {
	input := textinput.New()
	input.Prompt = "/"
	input.Placeholder = "issue id"
	input.CharLimit = 64
	return input
}

func newBoardHelp() help.Model {
	return help.New()
}

func newBoardSpinner() spinner.Model {
	return spinner.New(spinner.WithSpinner(spinner.Line))
}

func newBoardViewport() viewport.Model {
	return viewport.New(1, 1)
}

func boardStyledSearchInput(input textinput.Model, theme boardTheme, width int) textinput.Model {
	input.Width = maxInt(width-2, 1)
	input.PromptStyle = theme.lineStyle(theme.detailFG, "", true, false)
	input.TextStyle = theme.lineStyle(theme.detailFG, "", false, false)
	input.PlaceholderStyle = theme.lineStyle(theme.mutedFG, "", false, false)
	input.Cursor.Style = theme.lineStyle(theme.detailFG, theme.titleMetaBG, false, false)
	return input
}

func boardStyledHelp(model help.Model, theme boardTheme, width int, showAll bool) help.Model {
	model.Width = width
	model.ShowAll = showAll
	model.Styles.ShortKey = theme.lineStyle(theme.keyFG, "", true, false)
	model.Styles.ShortDesc = theme.lineStyle(theme.helpFG, "", false, false)
	model.Styles.ShortSeparator = theme.lineStyle(theme.mutedFG, "", false, false)
	model.Styles.FullKey = theme.lineStyle(theme.keyFG, "", true, false)
	model.Styles.FullDesc = theme.lineStyle(theme.helpFG, "", false, false)
	model.Styles.FullSeparator = theme.lineStyle(theme.mutedFG, "", false, false)
	model.Styles.Ellipsis = theme.lineStyle(theme.mutedFG, "", false, false)
	return model
}

func boardStyledSpinner(model spinner.Model, theme boardTheme) spinner.Model {
	model.Style = theme.lineStyle(theme.keyFG, "", true, false)
	return model
}

func boardSyncViewportState(model boardTUIModel) boardTUIModel {
	width := boardCurrentPanelInnerWidth(model)
	height := boardCurrentPanelBodyHeight(model)

	model.detailViewport.Width = width
	model.detailViewport.Height = height
	model.continuityViewport.Width = width
	model.continuityViewport.Height = height

	model.detailViewport = boardSyncViewportOffset(model, model.detailViewport, boardPanelModeDetail)
	model.continuityViewport = boardSyncViewportOffset(model, model.continuityViewport, boardPanelModeContinuity)
	return model
}

func boardSyncViewportOffset(model boardTUIModel, view viewport.Model, mode boardPanelMode) viewport.Model {
	view.SetYOffset(boardPanelScrollForMode(model, mode))
	return view
}

func boardPanelScrollForMode(model boardTUIModel, mode boardPanelMode) int {
	if model.panelScroll == nil {
		return 0
	}
	return model.panelScroll[boardPanelScrollKey(model.selectedIssue, mode)]
}

func boardViewportModelForContent(model boardTUIModel, mode boardPanelMode, content []string, width, height int) viewport.Model {
	var view viewport.Model
	switch mode {
	case boardPanelModeContinuity:
		view = model.continuityViewport
	default:
		view = model.detailViewport
	}
	view.Width = width
	view.Height = height
	view.SetContent(strings.Join(content, "\n"))
	view.SetYOffset(boardPanelScrollForMode(model, mode))
	return view
}

func boardViewportRangeLabel(view viewport.Model) string {
	total := view.TotalLineCount()
	visible := view.VisibleLineCount()
	if total == 0 || total <= visible {
		return ""
	}
	start := view.YOffset + 1
	end := minInt(view.YOffset+visible, total)
	return strconv.Itoa(start) + "-" + strconv.Itoa(end) + "/" + strconv.Itoa(total)
}

func boardHelpView(model boardTUIModel, theme boardTheme, width int, full bool) string {
	helpModel := boardStyledHelp(model.help, theme, width, full)
	return helpModel.View(boardKeys)
}

func boardSearchInputView(model boardTUIModel, theme boardTheme, width int) string {
	input := boardStyledSearchInput(model.searchInput, theme, width)
	return input.View()
}
