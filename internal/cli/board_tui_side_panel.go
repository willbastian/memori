package cli

func boardSidePanel(model boardTUIModel, theme boardTheme, width, height int) []string {
	title := "Issue Detail"
	subtitle := "Context"
	if model.panelMode == boardPanelModeContinuity {
		title = "Continuity"
		subtitle = "Audit"
	}

	if !model.detailOpen {
		lines := []string{
			boardPanelHeader(theme, title, subtitle, width),
			theme.paintLine(theme.mutedFG, theme.panelAltBG, false, padRight(" press <enter> to open the selected issue panel ", width)),
		}
		for len(lines) < height {
			lines = append(lines, padRight("", width))
		}
		return lines[:height]
	}

	if model.panelMode == boardPanelModeContinuity {
		return boardContinuityPanel(model, theme, width, height)
	}
	return boardDetailPanel(model, theme, width, height)
}
