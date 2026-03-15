package cli

import (
	"errors"
	"fmt"
	"io"
)

func runContext(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("context subcommand required: start|save|checkpoint|summarize|close|rehydrate|packet|loops")
	}
	switch args[0] {
	case "start":
		return runContextStart(args[1:], out)
	case "save":
		return runContextSave(args[1:], out)
	case "checkpoint":
		return runContextCheckpoint(args[1:], out)
	case "summarize":
		return runContextSummarize(args[1:], out)
	case "close":
		return runContextClose(args[1:], out)
	case "rehydrate":
		return runContextRehydrate(args[1:], out)
	case "packet":
		return runContextPacket(args[1:], out)
	case "loops":
		return runContextLoops(args[1:], out)
	default:
		return fmt.Errorf("unknown context subcommand %q", args[0])
	}
}
