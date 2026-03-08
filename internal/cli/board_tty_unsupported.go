//go:build !darwin && !linux

package cli

import (
	"errors"
	"io"
	"os"
)

func boardInput() *os.File {
	return os.Stdin
}

func boardSupportsInteractive(io.Writer) bool {
	return false
}

func boardEnterRawMode() (func(), error) {
	return nil, errors.New("interactive board is unsupported on this platform")
}

func boardTerminalSize(io.Writer) (int, int) {
	return boardRenderWidth(), 24
}
