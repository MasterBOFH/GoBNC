package store

import (
	"context"
	"testing"
	"time"
)

// seedIDs stores n PRIVMSGs to #c, a minute apart from t0, and returns their ids.
func seedIDs(t *testing.T, s *Store, nid int64, t0 time.Time, n int) []int64 {
	t.Helper()
	ids := make([]int64, n)
	for i := 0; i < n; i++ {
		id, err := s.InsertMessage(context.Background(), Message{NetworkID: nid, Target: "#c",
			Time: t0.Add(time.Duration(i) * time.Minute), Command: "PRIVMSG", Source: "a!b@c", Raw: "x", Text: "m"})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}
	return ids
}

func pageIDs(ms []Message) []int64 {
	out := make([]int64, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}
	return out
}

func TestIDPaging(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	nid, err := s.UpsertNetwork(ctx, Network{Name: "n", Host: "h", Port: 1, Nick: "x", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ids := seedIDs(t, s, nid, t0, 120)

	latest, _ := s.MessagesLatest(ctx, nid, "#c", 50)
	if got := pageIDs(latest); len(got) != 50 || got[0] != ids[70] || got[49] != ids[119] {
		t.Fatalf("latest = %v", got)
	}
	before, _ := s.MessagesBefore(ctx, nid, "#c", ids[70], 50)
	if got := pageIDs(before); len(got) != 50 || got[0] != ids[20] || got[49] != ids[69] {
		t.Fatalf("before = %v", got)
	}
	first, _ := s.MessagesBefore(ctx, nid, "#c", ids[20], 50)
	if got := pageIDs(first); len(got) != 20 || got[0] != ids[0] {
		t.Fatalf("first page = %v", got)
	}
	after, _ := s.MessagesAfter(ctx, nid, "#c", ids[100], 50)
	if got := pageIDs(after); len(got) != 19 || got[0] != ids[101] || got[18] != ids[119] {
		t.Fatalf("after = %v", got)
	}
	around, _ := s.MessagesAround(ctx, nid, "#c", ids[60], 10)
	if got := pageIDs(around); len(got) != 10 || got[0] != ids[55] || got[5] != ids[60] || got[9] != ids[64] {
		t.Fatalf("around = %v", got)
	}

	for _, tc := range []struct {
		id            int64
		before, after bool
	}{{ids[0], false, true}, {ids[119], true, false}, {ids[60], true, true}} {
		b, _ := s.HasMessageBefore(ctx, nid, "#c", tc.id)
		a, _ := s.HasMessageAfter(ctx, nid, "#c", tc.id)
		if b != tc.before || a != tc.after {
			t.Fatalf("id %d: has_before=%v has_after=%v, want %v/%v", tc.id, b, a, tc.before, tc.after)
		}
	}

	// Read marker at message 99's time: the first unread is 100; a device floor above it wins.
	marker := t0.Add(99 * time.Minute)
	if id, _ := s.FirstUnreadID(ctx, nid, "#c", marker, 0); id != ids[100] {
		t.Fatalf("first unread = %d, want %d", id, ids[100])
	}
	if id, _ := s.FirstUnreadID(ctx, nid, "#c", marker, ids[110]); id != ids[111] {
		t.Fatalf("first unread with floor = %d, want %d", id, ids[111])
	}
	if id, _ := s.FirstUnreadID(ctx, nid, "#c", time.Time{}, 0); id != ids[0] {
		t.Fatalf("first unread with no marker = %d, want %d", id, ids[0])
	}
	if id, _ := s.FirstUnreadID(ctx, nid, "#c", t0.Add(200*time.Minute), 0); id != 0 {
		t.Fatalf("first unread when all read = %d", id)
	}
	if max, _ := s.MaxMessageID(ctx, nid); max != ids[119] {
		t.Fatalf("max id = %d", max)
	}
}

// MessagesBetween is the gap fill's page: ascending from after, never reaching before, scoped
// to one network and target like the other id pages.
func TestMessagesBetween(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	nid, err := s.UpsertNetwork(ctx, Network{Name: "n", Host: "h", Port: 1, Nick: "x", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.UpsertNetwork(ctx, Network{Name: "o", Host: "h", Port: 1, Nick: "x", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ids := seedIDs(t, s, nid, t0, 20)
	// Rows of another network and another target take ids in between (ids are global).
	seedIDs(t, s, other, t0, 3)
	if _, err := s.InsertMessage(ctx, Message{NetworkID: nid, Target: "#d", Time: t0, Command: "PRIVMSG", Source: "a!b@c", Raw: "x", Text: "m"}); err != nil {
		t.Fatal(err)
	}
	more := seedIDs(t, s, nid, t0.Add(time.Hour), 5)

	// Adjacent bounds: nothing between.
	if got, err := s.MessagesBetween(ctx, nid, "#c", ids[4], ids[5], 50); err != nil || len(got) != 0 {
		t.Fatalf("adjacent = %v, %v", pageIDs(got), err)
	}
	// Partial page: every row strictly inside, ascending.
	got, err := s.MessagesBetween(ctx, nid, "#c", ids[2], ids[8], 50)
	if p := pageIDs(got); err != nil || len(p) != 5 || p[0] != ids[3] || p[4] != ids[7] {
		t.Fatalf("partial = %v, %v; want %d..%d", p, err, ids[3], ids[7])
	}
	// Full page: the oldest limit rows after the lower bound.
	got, _ = s.MessagesBetween(ctx, nid, "#c", ids[0], ids[19], 4)
	if p := pageIDs(got); len(p) != 4 || p[0] != ids[1] || p[3] != ids[4] {
		t.Fatalf("full = %v", p)
	}
	// Across the other network's and target's rows: only (nid, #c).
	got, _ = s.MessagesBetween(ctx, nid, "#c", ids[19], more[1], 50)
	if p := pageIDs(got); len(p) != 1 || p[0] != more[0] {
		t.Fatalf("scoped = %v, want [%d]", p, more[0])
	}
}
