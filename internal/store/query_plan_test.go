package store

import (
	"fmt"
	"strings"
	"testing"
)

// planOf returns SQLite's query plan for query (one buffer's filter in place of %s), one
// detail line per step.
func planOf(t *testing.T, s *Store, query string) string {
	t.Helper()
	q := "EXPLAIN QUERY PLAN " + fmt.Sprintf(query, "network_id=? AND target=?")
	args := make([]any, strings.Count(q, "?"))
	for i := range args {
		args[i] = 1
	}
	rows, err := s.db.Query(q, args...)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(lines, "\n")
}

// TestIDQueriesUseTargetIDIndex: every id-ordered read of one buffer walks
// idx_messages_target_id instead of sorting the buffer's whole history (a temp B-tree), which
// cost a 300k-line buffer about 200 ms per query on a small bouncer.
func TestIDQueriesUseTargetIDIndex(t *testing.T) {
	s := openTemp(t)
	queries := map[string]string{
		"newest line":        sqlNewestLine,
		"unread, never read": unreadLinesSQL(false),
		"messages before":    sqlMessagesBefore,
		"messages after":     sqlMessagesAfter,
		"messages between":   sqlMessagesBetween,
		"messages latest":    sqlMessagesLatest,
		"has message before": sqlMessageBefore,
		"has message after":  sqlMessageAfter,
	}
	for name, q := range queries {
		plan := planOf(t, s, q)
		if !strings.Contains(plan, "idx_messages_target_id") || strings.Contains(plan, "TEMP B-TREE") {
			t.Errorf("%s: want a walk of idx_messages_target_id with no sort, plan:\n%s", name, plan)
		}
	}
}

// TestUnreadSinceMarkerRangesOverTime: with a read marker, the unread query ranges over the
// time index (the few lines after the marker), never a backwards walk of the whole buffer by id.
func TestUnreadSinceMarkerRangesOverTime(t *testing.T) {
	s := openTemp(t)
	plan := planOf(t, s, unreadLinesSQL(true))
	if !strings.Contains(plan, "idx_messages_target_time (network_id=? AND target=? AND time>?)") {
		t.Errorf("want a time range on idx_messages_target_time, plan:\n%s", plan)
	}
}
