package cli

import (
	"sort"
	"strings"
)

type boardSearchMatch struct {
	lane boardLane
	row  boardIssueRow
}

func boardSearchResults(model boardTUIModel) []boardSearchMatch {
	query := strings.ToLower(strings.TrimSpace(model.searchQuery))
	preference := boardLanePreference(model.lane, model.navigationLanes())
	laneRank := make(map[boardLane]int, len(preference))
	for idx, lane := range preference {
		laneRank[lane] = idx
	}
	seen := make(map[string]struct{})
	results := make([]boardSearchMatch, 0)
	for _, lane := range preference {
		for _, row := range model.rawRowsForLane(lane) {
			if _, ok := seen[row.Issue.ID]; ok {
				continue
			}
			if !boardSearchMatches(row.Issue.ID, query) {
				continue
			}
			seen[row.Issue.ID] = struct{}{}
			results = append(results, boardSearchMatch{lane: lane, row: row})
		}
	}
	sort.SliceStable(results, func(i, j int) bool {
		leftScore := boardSearchScore(results[i].row.Issue.ID, query)
		rightScore := boardSearchScore(results[j].row.Issue.ID, query)
		if leftScore != rightScore {
			return leftScore < rightScore
		}
		if results[i].lane != results[j].lane {
			return laneRank[results[i].lane] < laneRank[results[j].lane]
		}
		return results[i].row.Issue.ID < results[j].row.Issue.ID
	})
	return results
}

func boardSearchMatches(issueID, query string) bool {
	if query == "" {
		return true
	}
	id := strings.ToLower(strings.TrimSpace(issueID))
	shortID := strings.TrimPrefix(id, "mem-")
	return strings.HasPrefix(id, query) || strings.HasPrefix(shortID, query) || strings.Contains(id, query) || strings.Contains(shortID, query)
}

func boardSearchScore(issueID, query string) int {
	if query == "" {
		return 3
	}
	id := strings.ToLower(strings.TrimSpace(issueID))
	shortID := strings.TrimPrefix(id, "mem-")
	switch {
	case id == query || shortID == query:
		return 0
	case strings.HasPrefix(id, query) || strings.HasPrefix(shortID, query):
		return 1
	default:
		return 2
	}
}

func boardLanePreference(preferred boardLane, lanes []boardLane) []boardLane {
	allowed := make(map[boardLane]struct{}, len(lanes))
	for _, lane := range lanes {
		allowed[lane] = struct{}{}
	}
	appendLane := func(out []boardLane, seen map[boardLane]struct{}, lane boardLane) []boardLane {
		if _, ok := allowed[lane]; !ok {
			return out
		}
		if _, ok := seen[lane]; ok {
			return out
		}
		seen[lane] = struct{}{}
		return append(out, lane)
	}
	if preferred == boardLaneNext {
		out := make([]boardLane, 0, len(lanes))
		seen := make(map[boardLane]struct{}, len(lanes))
		for _, lane := range []boardLane{boardLaneReady, boardLaneActive, boardLaneBlocked, boardLaneDone, boardLaneWontDo, boardLaneNext} {
			out = appendLane(out, seen, lane)
		}
		return out
	}
	order := []boardLane{preferred, boardLaneActive, boardLaneBlocked, boardLaneReady, boardLaneDone, boardLaneWontDo, boardLaneNext}
	seen := make(map[boardLane]struct{}, len(order))
	out := make([]boardLane, 0, len(order))
	for _, lane := range order {
		out = appendLane(out, seen, lane)
	}
	return out
}
