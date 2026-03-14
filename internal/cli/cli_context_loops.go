package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/willbastian/memori/internal/store"
)

func runContextLoops(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context loops", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issue := fs.String("issue", "", "optional issue key filter")
	cycle := fs.Int("cycle", 0, "optional cycle filter (> 0)")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var cyclePtr *int
	if hasFlag(args, "cycle") {
		if *cycle <= 0 {
			return errors.New("--cycle must be > 0")
		}
		cyclePtr = cycle
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	loops, err := s.ListOpenLoops(ctx, store.ListOpenLoopsParams{
		IssueID: *issue,
		CycleNo: cyclePtr,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "context loops",
			Data: contextLoopsData{
				Count: len(loops),
				Loops: loops,
			},
		})
	}

	if len(loops) == 0 {
		_, _ = fmt.Fprintln(out, "No context loops matched the filters.")
		return nil
	}
	for _, loop := range loops {
		_, _ = fmt.Fprintf(out, "- %s [%s/%s] issue=%s cycle=%d", loop.LoopID, loop.LoopType, loop.Status, loop.IssueID, loop.CycleNo)
		if strings.TrimSpace(loop.Priority) != "" {
			_, _ = fmt.Fprintf(out, " priority=%s", loop.Priority)
		}
		if strings.TrimSpace(loop.SourceEventID) != "" {
			_, _ = fmt.Fprintf(out, " source=%s", loop.SourceEventID)
		}
		_, _ = fmt.Fprintln(out)
	}
	return nil
}

type contextLoopsData struct {
	Count int              `json:"count"`
	Loops []store.OpenLoop `json:"loops"`
}
