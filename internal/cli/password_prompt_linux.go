//go:build linux

package cli

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/mattn/go-isatty"
	"golang.org/x/sys/unix"
)

func readPasswordNoEcho(prompt string) (string, error) {
	fd := int(os.Stdin.Fd())
	if !isatty.IsTerminal(uintptr(fd)) {
		return "", errors.New("human mutation auth requires an interactive terminal")
	}

	state, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return "", fmt.Errorf("read terminal state: %w", err)
	}
	original := *state
	disabled := *state
	disabled.Lflag &^= unix.ECHO

	if _, err := fmt.Fprint(os.Stderr, prompt); err != nil {
		return "", err
	}
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, &disabled); err != nil {
		return "", fmt.Errorf("disable terminal echo: %w", err)
	}
	defer func() {
		_ = unix.IoctlSetTermios(fd, unix.TCSETS, &original)
		_, _ = fmt.Fprintln(os.Stderr)
	}()

	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read password: %w", err)
	}
	return strings.TrimRight(line, "\r\n"), nil
}
