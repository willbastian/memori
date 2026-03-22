package cli

import (
	"strings"
	"time"
)

type boardAsyncLoadState struct {
	loading       bool
	stale         bool
	lastSuccessAt time.Time
	err           string
}

type boardToastTone string

const (
	boardToastToneInfo    boardToastTone = "info"
	boardToastToneWarn    boardToastTone = "warn"
	boardToastToneSuccess boardToastTone = "success"
)

type boardToast struct {
	id      int
	tone    boardToastTone
	message string
}

func boardBeginAsyncLoad(state boardAsyncLoadState) boardAsyncLoadState {
	state.loading = true
	state.err = ""
	if !state.lastSuccessAt.IsZero() {
		state.stale = true
	}
	return state
}

func boardSucceedAsyncLoad(state boardAsyncLoadState, now time.Time) boardAsyncLoadState {
	state.loading = false
	state.stale = false
	state.err = ""
	state.lastSuccessAt = now
	return state
}

func boardFailAsyncLoad(state boardAsyncLoadState, err error) boardAsyncLoadState {
	state.loading = false
	if strings.TrimSpace(state.err) == "" && err != nil {
		state.err = err.Error()
	} else if err != nil {
		state.err = err.Error()
	}
	if !state.lastSuccessAt.IsZero() {
		state.stale = true
	}
	return state
}

func boardSetToast(model boardTUIModel, tone boardToastTone, message string) boardTUIModel {
	message = strings.TrimSpace(message)
	if message == "" {
		return model
	}
	model.nextToastID++
	model.toast = boardToast{
		id:      model.nextToastID,
		tone:    tone,
		message: message,
	}
	return model
}

func boardClearToast(model boardTUIModel, id int) boardTUIModel {
	if model.toast.id == id {
		model.toast = boardToast{}
	}
	return model
}

func boardAnyLoading(model boardTUIModel) bool {
	return model.snapshotLoad.loading || model.auditLoad.loading
}

func boardStartSnapshotLoad(model boardTUIModel) (boardTUIModel, int) {
	model.nextSnapshotRequestID++
	model.activeSnapshotRequestID = model.nextSnapshotRequestID
	model.snapshotLoad = boardBeginAsyncLoad(model.snapshotLoad)
	return model, model.activeSnapshotRequestID
}

func boardStartAuditLoad(model boardTUIModel) (boardTUIModel, int) {
	model.nextAuditRequestID++
	model.activeAuditRequestID = model.nextAuditRequestID
	model.auditLoad = boardBeginAsyncLoad(model.auditLoad)
	return model, model.activeAuditRequestID
}

func boardPanelScrollKey(issueID string, mode boardPanelMode) string {
	return boardPanelModeTitle(mode) + ":" + strings.TrimSpace(issueID)
}

func boardCurrentPanelScroll(model boardTUIModel) int {
	return boardPanelScrollForMode(model, model.panelMode)
}

func boardSetCurrentPanelScroll(model boardTUIModel, offset int) boardTUIModel {
	if model.panelScroll == nil {
		model.panelScroll = make(map[string]int)
	}
	if offset < 0 {
		offset = 0
	}
	key := boardPanelScrollKey(model.selectedIssue, model.panelMode)
	if strings.TrimSpace(model.selectedIssue) == "" {
		return model
	}
	model.panelScroll[key] = offset
	switch model.panelMode {
	case boardPanelModeContinuity:
		model.continuityViewport.SetYOffset(offset)
	default:
		model.detailViewport.SetYOffset(offset)
	}
	return model
}

func boardClampPanelScroll(model boardTUIModel) boardTUIModel {
	if !model.detailOpen {
		return model
	}
	maxOffset := boardMaxPanelScroll(model)
	current := boardCurrentPanelScroll(model)
	if current <= maxOffset {
		return model
	}
	return boardSetCurrentPanelScroll(model, maxOffset)
}

func boardMaxPanelScroll(model boardTUIModel) int {
	if !model.detailOpen {
		return 0
	}
	content := boardSidePanelContent(model, defaultBoardTheme(false), boardCurrentPanelInnerWidth(model))
	view := boardViewportModelForContent(model, model.panelMode, content.body, boardCurrentPanelInnerWidth(model), boardCurrentPanelBodyHeight(model))
	return maxInt(view.TotalLineCount()-view.VisibleLineCount(), 0)
}

func boardAdjustPanelScroll(model boardTUIModel, delta int) boardTUIModel {
	content := boardSidePanelContent(model, defaultBoardTheme(false), boardCurrentPanelInnerWidth(model))
	view := boardViewportModelForContent(model, model.panelMode, content.body, boardCurrentPanelInnerWidth(model), boardCurrentPanelBodyHeight(model))
	switch {
	case delta < 0:
		view.ScrollUp(-delta)
	case delta > 0:
		view.ScrollDown(delta)
	}
	model = boardSetCurrentPanelScroll(model, view.YOffset)
	return boardSyncViewportState(model)
}

func boardCurrentPanelInnerWidth(model boardTUIModel) int {
	width := maxInt(model.width, 24)
	if width >= 100 && model.detailOpen {
		rightWidth := minInt(maxInt(width/3+6, 38), 52)
		return maxInt(rightWidth-2, 1)
	}
	return maxInt(width-2, 1)
}

func boardCurrentPanelBodyHeight(model boardTUIModel) int {
	height := maxInt(model.height, 10)
	bodyHeight := maxInt(height-4, 5)
	if model.toast.message != "" {
		bodyHeight--
	}
	return maxInt(bodyHeight-3, 1)
}
