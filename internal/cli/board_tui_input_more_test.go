package cli

import (
	"bufio"
	"io"
	"strings"
	"testing"
)

func TestReadBoardInputParsesAdditionalNavigationKeys(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		input     string
		want      boardAction
		backspace bool
	}{
		{name: "quit", input: "q", want: boardActionQuit},
		{name: "down", input: "j", want: boardActionDown},
		{name: "up", input: "k", want: boardActionUp},
		{name: "previous lane", input: "h", want: boardActionPrevLane},
		{name: "next lane", input: "l", want: boardActionNextLane},
		{name: "top", input: "g", want: boardActionTop},
		{name: "bottom", input: "G", want: boardActionBottom},
		{name: "toggle help", input: "?", want: boardActionToggleHelp},
		{name: "parent", input: "[", want: boardActionParent},
		{name: "child", input: "]", want: boardActionChild},
		{name: "collapse", input: "{", want: boardActionCollapse},
		{name: "expand", input: "}", want: boardActionExpand},
		{name: "enter toggles detail", input: "\r", want: boardActionToggleDetail},
		{name: "newline toggles detail", input: "\n", want: boardActionToggleDetail},
		{name: "space toggles detail", input: " ", want: boardActionToggleDetail},
		{name: "ctrl h backspace", input: "\b", backspace: true},
		{name: "delete backspace", input: "\x7f", backspace: true},
		{name: "arrow up", input: "\x1b[A", want: boardActionUp},
		{name: "arrow down", input: "\x1b[B", want: boardActionDown},
		{name: "arrow right", input: "\x1b[C", want: boardActionNextLane},
		{name: "arrow left", input: "\x1b[D", want: boardActionPrevLane},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := readBoardInput(bufio.NewReader(strings.NewReader(tc.input)))
			if err != nil {
				t.Fatalf("read board input: %v", err)
			}
			if got.action != tc.want || got.backspace != tc.backspace {
				t.Fatalf("expected action=%v backspace=%v, got %+v", tc.want, tc.backspace, got)
			}
		})
	}
}

func TestReadBoardInputHandlesFallbacksAndErrors(t *testing.T) {
	t.Parallel()

	if _, err := readBoardInput(bufio.NewReader(strings.NewReader(""))); err != io.EOF {
		t.Fatalf("expected EOF for empty reader, got %v", err)
	}

	unknownEscape, err := readBoardInput(bufio.NewReader(strings.NewReader("\x1bx")))
	if err != nil {
		t.Fatalf("read unknown escape: %v", err)
	}
	if unknownEscape.action != boardActionQuit {
		t.Fatalf("expected non-CSI escape to quit, got %+v", unknownEscape)
	}

	if _, err := readBoardInput(bufio.NewReader(strings.NewReader("\x1b["))); err != io.EOF {
		t.Fatalf("expected EOF for truncated escape sequence, got %v", err)
	}

	unknownArrow, err := readBoardInput(bufio.NewReader(strings.NewReader("\x1b[Z")))
	if err != nil {
		t.Fatalf("read unknown arrow: %v", err)
	}
	if unknownArrow != (boardKeyInput{}) {
		t.Fatalf("expected unknown arrow to produce zero-value input, got %+v", unknownArrow)
	}

	nonPrintable, err := readBoardInput(bufio.NewReader(strings.NewReader("\x01")))
	if err != nil {
		t.Fatalf("read non-printable input: %v", err)
	}
	if nonPrintable != (boardKeyInput{}) {
		t.Fatalf("expected non-printable input to be ignored, got %+v", nonPrintable)
	}
}

func TestReadBoardInputsSkipsEmptyEventsAndStopsOnEOF(t *testing.T) {
	t.Parallel()

	actions := make(chan boardKeyInput, 2)
	errCh := make(chan error, 1)

	go readBoardInputs(bufio.NewReader(strings.NewReader("\x00j")), actions, errCh)

	action := <-actions
	if action.action != boardActionDown {
		t.Fatalf("expected only the non-empty input to be emitted, got %+v", action)
	}

	if err := <-errCh; err != io.EOF {
		t.Fatalf("expected EOF after inputs drained, got %v", err)
	}

	select {
	case extra := <-actions:
		t.Fatalf("expected no extra actions, got %+v", extra)
	default:
	}
}
