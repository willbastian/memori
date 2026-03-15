package cli

import (
	"strings"

	"github.com/willbastian/memori/internal/store"
)

func buildBoardHierarchy(issues []store.Issue) map[string]boardIssueHierarchy {
	issueByID := make(map[string]store.Issue, len(issues))
	childrenByParent := make(map[string][]string)
	roots := make([]string, 0, len(issues))
	for _, issue := range issues {
		issueByID[issue.ID] = issue
		parentID := strings.TrimSpace(issue.ParentID)
		if parentID == "" {
			roots = append(roots, issue.ID)
			continue
		}
		childrenByParent[parentID] = append(childrenByParent[parentID], issue.ID)
	}

	descendantMemo := make(map[string]int, len(issues))
	hierarchyByID := make(map[string]boardIssueHierarchy, len(issues))
	for _, issue := range issues {
		hierarchyByID[issue.ID] = boardHierarchyForIssue(issue, issueByID, childrenByParent, roots, descendantMemo)
	}
	return hierarchyByID
}

func boardHierarchyForIssue(
	issue store.Issue,
	issueByID map[string]store.Issue,
	childrenByParent map[string][]string,
	roots []string,
	descendantMemo map[string]int,
) boardIssueHierarchy {
	parentID := strings.TrimSpace(issue.ParentID)
	ancestors := boardAncestorPath(issue.ID, parentID, issueByID)
	childIDs := append([]string(nil), childrenByParent[issue.ID]...)
	siblings := roots
	if parentID != "" {
		siblings = childrenByParent[parentID]
	}

	hierarchy := boardIssueHierarchy{
		Depth:           len(ancestors),
		Path:            append(append([]string(nil), ancestors...), issue.ID),
		AncestorIDs:     ancestors,
		ParentID:        parentID,
		ChildIDs:        childIDs,
		ChildCount:      len(childIDs),
		DescendantCount: boardDescendantCount(issue.ID, childrenByParent, descendantMemo),
		HasChildren:     len(childIDs) > 0,
		SiblingCount:    len(siblings),
	}
	for idx, siblingID := range siblings {
		if siblingID == issue.ID {
			hierarchy.SiblingIndex = idx
			break
		}
	}
	if parentID != "" {
		if parent, ok := issueByID[parentID]; ok {
			hierarchy.ParentTitle = parent.Title
			hierarchy.ParentType = parent.Type
			hierarchy.ParentStatus = parent.Status
		}
	}
	return hierarchy
}

func boardAncestorPath(issueID, parentID string, issueByID map[string]store.Issue) []string {
	if strings.TrimSpace(parentID) == "" {
		return nil
	}
	ancestors := make([]string, 0, 4)
	visited := map[string]struct{}{
		issueID: {},
	}
	current := strings.TrimSpace(parentID)
	for current != "" {
		if _, seen := visited[current]; seen {
			break
		}
		visited[current] = struct{}{}
		ancestors = append(ancestors, current)
		parent, ok := issueByID[current]
		if !ok {
			break
		}
		current = strings.TrimSpace(parent.ParentID)
	}
	for left, right := 0, len(ancestors)-1; left < right; left, right = left+1, right-1 {
		ancestors[left], ancestors[right] = ancestors[right], ancestors[left]
	}
	return ancestors
}

func boardDescendantCount(issueID string, childrenByParent map[string][]string, memo map[string]int) int {
	if count, ok := memo[issueID]; ok {
		return count
	}
	total := 0
	for _, childID := range childrenByParent[issueID] {
		total++
		total += boardDescendantCount(childID, childrenByParent, memo)
	}
	memo[issueID] = total
	return total
}
