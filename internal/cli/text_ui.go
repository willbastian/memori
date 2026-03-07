package cli

import (
	"fmt"
	"io"
	"strings"
)

type textUI struct {
	out    io.Writer
	colors bool
}

func newTextUI(out io.Writer) textUI {
	return textUI{
		out:    out,
		colors: shouldUseColor(out),
	}
}

func (ui textUI) heading(value string) {
	_, _ = fmt.Fprintln(ui.out, colorize(ui.colors, "1;36", value))
}

func (ui textUI) success(value string) {
	_, _ = fmt.Fprintf(ui.out, "%s %s\n", colorize(ui.colors, "1;32", "OK"), value)
}

func (ui textUI) note(value string) {
	_, _ = fmt.Fprintf(ui.out, "%s %s\n", colorize(ui.colors, "1;33", "Note"), value)
}

func (ui textUI) section(label string) {
	_, _ = fmt.Fprintf(ui.out, "%s:\n", colorize(ui.colors, "1;34", label))
}

func (ui textUI) field(label, value string) {
	if strings.TrimSpace(value) == "" {
		return
	}
	_, _ = fmt.Fprintf(ui.out, "%s: %s\n", colorize(ui.colors, "1", label), value)
}

func (ui textUI) bullet(value string) {
	_, _ = fmt.Fprintf(ui.out, "- %s\n", value)
}

func (ui textUI) blank() {
	_, _ = fmt.Fprintln(ui.out)
}

func (ui textUI) nextSteps(steps ...string) {
	if len(steps) == 0 {
		return
	}
	ui.blank()
	ui.section("Next")
	for _, step := range steps {
		ui.bullet(step)
	}
}
