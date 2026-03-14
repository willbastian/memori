package cli

import (
	"errors"
	"fmt"
	"io"
)

func runIssue(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("issue subcommand required: create|link|update|show|next")
	}

	switch args[0] {
	case "create":
		return runIssueCreate(args[1:], out)
	case "link":
		return runIssueLink(args[1:], out)
	case "update":
		return runIssueUpdate(args[1:], out)
	case "show":
		return runIssueShow(args[1:], out)
	case "next":
		return runIssueNext(args[1:], out)
	default:
		return fmt.Errorf("unknown issue subcommand %q", args[0])
	}
}
