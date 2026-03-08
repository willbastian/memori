//go:build darwin

package cli

import (
	"bytes"
	"os"
	"testing"
)

func TestBoardInputReturnsStandardInputOnDarwin(t *testing.T) {
	t.Parallel()

	if got := boardInput(); got != os.Stdin {
		t.Fatalf("expected board input to be os.Stdin, got %v", got)
	}
}

func TestBoardSupportsInteractiveRejectsNonTTYCasesOnDarwin(t *testing.T) {
	if boardSupportsInteractive(&bytes.Buffer{}) {
		t.Fatal("expected non-file writer to disable interactive board support")
	}

	t.Setenv("TERM", "dumb")
	if boardSupportsInteractive(os.Stdout) {
		t.Fatal("expected TERM=dumb to disable interactive board support")
	}
}

func TestBoardTerminalSizeFallsBackWhenWriterIsNotInteractiveOnDarwin(t *testing.T) {
	t.Parallel()

	width, height := boardTerminalSize(&bytes.Buffer{})
	if width != boardRenderWidth() || height != 24 {
		t.Fatalf("expected fallback terminal size for non-file writer, got %dx%d", width, height)
	}

	file, err := os.CreateTemp(t.TempDir(), "memori-board-size-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer file.Close()

	width, height = boardTerminalSize(file)
	if width != boardRenderWidth() || height != 24 {
		t.Fatalf("expected fallback terminal size for regular file, got %dx%d", width, height)
	}
}
