package cli

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

func boardRenderWidth() int {
	if raw := strings.TrimSpace(os.Getenv("COLUMNS")); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value > 20 {
			return value
		}
	}
	return 80
}

func boardSectionLimit(width int) int {
	switch {
	case width < 50:
		return 2
	case width < 80:
		return 3
	default:
		return 5
	}
}

func boardLikelyNextLimit(width int) int {
	switch {
	case width < 50:
		return 1
	case width < 80:
		return 2
	default:
		return 3
	}
}

func truncateBoardLine(value string, width int) string {
	value = strings.TrimSpace(value)
	if width <= 0 || visualWidth(value) <= width {
		return value
	}
	if width <= 3 {
		return trimVisual(value, width)
	}
	return trimVisual(value, width-3) + "..."
}

func boardSnapshotSignature(snapshot boardSnapshot) string {
	normalized := snapshot
	normalized.GeneratedAt = ""
	payload, err := json.Marshal(normalized)
	if err != nil {
		return ""
	}
	return string(payload)
}

func runBoardLoop(ctx context.Context, out io.Writer, interval time.Duration, render func() (string, string, error)) error {
	var lastSignature string
	redraw := boardSupportsInteractive(out)
	renderFrame := func(first bool) error {
		rendered, signature, err := render()
		if err != nil {
			return err
		}
		if !first && signature == lastSignature {
			return nil
		}
		if redraw {
			_, _ = io.WriteString(out, "\x1b[H\x1b[2J")
		} else if !first {
			_, _ = io.WriteString(out, "\n")
		}
		lastSignature = signature
		_, _ = io.WriteString(out, rendered)
		return nil
	}

	if err := renderFrame(true); err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := renderFrame(false); err != nil {
				return err
			}
		}
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
