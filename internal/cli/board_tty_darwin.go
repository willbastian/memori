//go:build darwin

package cli

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mattn/go-isatty"
	"golang.org/x/sys/unix"
)

func boardInput() *os.File {
	return os.Stdin
}

func boardSupportsInteractive(out io.Writer) bool {
	file, ok := out.(*os.File)
	if !ok {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("TERM")), "dumb") {
		return false
	}
	return isatty.IsTerminal(file.Fd()) && isatty.IsTerminal(os.Stdin.Fd())
}

// TODO(mem-5ece68e): extract injectable terminal and termios adapters so raw-mode
// coverage does not depend on PTY-style Darwin integration tests.
func boardEnterRawMode() (func(), error) {
	fd := int(os.Stdin.Fd())
	state, err := unix.IoctlGetTermios(fd, unix.TIOCGETA)
	if err != nil {
		return nil, fmt.Errorf("read terminal state: %w", err)
	}
	original := *state
	raw := *state
	raw.Iflag &^= unix.ICRNL | unix.IXON
	raw.Lflag &^= unix.ECHO | unix.ICANON | unix.ISIG | unix.IEXTEN
	raw.Cc[unix.VMIN] = 1
	raw.Cc[unix.VTIME] = 0
	if err := unix.IoctlSetTermios(fd, unix.TIOCSETA, &raw); err != nil {
		return nil, fmt.Errorf("enable raw mode: %w", err)
	}
	return func() {
		_ = unix.IoctlSetTermios(fd, unix.TIOCSETA, &original)
	}, nil
}

func boardTerminalSize(out io.Writer) (int, int) {
	file, ok := out.(*os.File)
	if !ok {
		return boardRenderWidth(), 24
	}
	ws, err := unix.IoctlGetWinsize(int(file.Fd()), unix.TIOCGWINSZ)
	if err != nil || ws.Col == 0 || ws.Row == 0 {
		return boardRenderWidth(), 24
	}
	return int(ws.Col), int(ws.Row)
}
