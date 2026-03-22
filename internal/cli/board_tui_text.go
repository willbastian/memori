package cli

import (
	"strings"

	"github.com/charmbracelet/x/ansi"
)

func wrapText(value string, width int) []string {
	width = maxInt(width, 10)
	words := strings.Fields(value)
	if len(words) == 0 {
		return nil
	}
	lines := []string{words[0]}
	for _, word := range words[1:] {
		current := lines[len(lines)-1]
		if visualWidth(current)+1+visualWidth(word) <= width {
			lines[len(lines)-1] = current + " " + word
			continue
		}
		lines = append(lines, word)
	}
	return lines
}

func padRight(value string, width int) string {
	if width <= 0 {
		return ""
	}
	if visualWidth(value) >= width {
		return trimVisual(value, width)
	}
	return value + strings.Repeat(" ", width-visualWidth(value))
}

func padVisual(value string, width int) string {
	if visualWidth(value) >= width {
		return value
	}
	return value + strings.Repeat(" ", width-visualWidth(value))
}

func visualWidth(value string) int {
	return ansi.StringWidth(value)
}

func trimVisual(value string, width int) string {
	if width <= 0 {
		return ""
	}
	if visualWidth(value) <= width {
		return value
	}
	return ansi.Truncate(value, width, "")
}
