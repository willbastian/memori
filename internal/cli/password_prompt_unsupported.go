//go:build !darwin && !linux

package cli

import "errors"

func readPasswordNoEcho(string) (string, error) {
	return "", errors.New("human mutation auth is unsupported on this platform")
}
