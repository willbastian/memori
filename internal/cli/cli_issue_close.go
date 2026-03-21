package cli

import (
	"context"

	"github.com/willbastian/memori/internal/store"
)

func issueCloseRecordForDisplay(ctx context.Context, s *store.Store, issue store.Issue) (*store.IssueCloseRecord, error) {
	if issue.Status != "Done" {
		return nil, nil
	}
	return s.LatestIssueCloseRecord(ctx, issue.ID)
}

func renderIssueCloseRecord(ui textUI, closeRecord *store.IssueCloseRecord) {
	if closeRecord == nil {
		return
	}
	ui.blank()
	ui.section("Close")
	ui.field("Mode", formatIssueCloseMode(closeRecord))
	ui.field("Closed", closeRecord.ClosedAt)
	if closeRecord.Proof != nil {
		ui.field("Gate Set", closeRecord.Proof.GateSetID)
	}
}

func formatIssueCloseMode(closeRecord *store.IssueCloseRecord) string {
	if closeRecord == nil {
		return ""
	}

	label := "Ungated"
	if closeRecord.Mode == store.IssueCloseModeGated {
		label = "Gated"
	}
	if closeRecord.LegacyInferred {
		return label + " (legacy inferred)"
	}
	return label
}
