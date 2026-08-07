package session

import (
	"context"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/irc"
)

func hasChathistory(d Downlink) bool {
	return d.HasCap("chathistory") || d.HasCap("draft/chathistory")
}

// playLegacyHistory sends unfetched backlog for target to a client without CHATHISTORY.
// Advances the shared playback cursor. Must not run for CHATHISTORY clients.
func (s *Session) playLegacyHistory(d Downlink, target string) {
	if s.hist == nil || hasChathistory(d) {
		return
	}
	s.mu.RLock()
	cm := s.isupport.CaseMapping
	netID := s.Network.ID
	s.mu.RUnlock()
	folded := cm.Canonical(target)

	after, err := s.legacyPlaybackAfter(folded)
	if err != nil {
		return
	}
	msgs, err := s.hist.QueryLegacyAfter(context.Background(), netID, folded, after)
	if err != nil || len(msgs) == 0 {
		return
	}
	var lastTS string
	for _, m := range msgs {
		if !history.IsLegacyReplayCommand(m.Command) {
			continue
		}
		parsed, ok := history.ParseStoredLine(m)
		if !ok {
			continue
		}
		out := s.rewriteFor(d, parsed)
		if out.Command == "" || !history.IsLegacyReplayCommand(out.Command) {
			continue
		}
		_ = d.Send(out)
		lastTS = storeFormatTime(m.Time)
	}
	if lastTS != "" {
		_, _, _ = s.setPlaybackCursorIfNewer(folded, lastTS)
	}
}

func storeFormatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z")
}

func (s *Session) legacyPlaybackAfter(foldedTarget string) (time.Time, error) {
	ts, err := s.getPlaybackCursor(foldedTarget)
	if err != nil {
		return time.Time{}, err
	}
	if ts != "" {
		t, err := parsePlaybackTime(ts)
		if err != nil {
			return time.Time{}, err
		}
		return t, nil
	}
	return time.Now().UTC().Add(-history.LegacyPlaybackHorizon), nil
}

func parsePlaybackTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

func (s *Session) getPlaybackCursor(foldedTarget string) (string, error) {
	if s.store != nil && s.Network.ID != 0 {
		ts, ok, err := s.store.GetPlaybackCursor(context.Background(), s.Network.ID, foldedTarget)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", nil
		}
		return ts, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.playbackCursors == nil {
		return "", nil
	}
	return s.playbackCursors[foldedTarget], nil
}

func (s *Session) setPlaybackCursorIfNewer(foldedTarget, ts string) (stored string, updated bool, err error) {
	if s.store != nil && s.Network.ID != 0 {
		return s.store.SetPlaybackCursorIfNewer(context.Background(), s.Network.ID, foldedTarget, ts)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.playbackCursors == nil {
		s.playbackCursors = map[string]string{}
	}
	cur := s.playbackCursors[foldedTarget]
	if cur != "" {
		cmp, err := comparePlaybackTimes(cur, ts)
		if err != nil {
			return "", false, err
		}
		if cmp >= 0 {
			return cur, false, nil
		}
	}
	s.playbackCursors[foldedTarget] = ts
	return ts, true, nil
}

func comparePlaybackTimes(a, b string) (int, error) {
	ta, err := parsePlaybackTime(a)
	if err != nil {
		return 0, err
	}
	tb, err := parsePlaybackTime(b)
	if err != nil {
		return 0, err
	}
	switch {
	case ta.Before(tb):
		return -1, nil
	case ta.After(tb):
		return 1, nil
	default:
		return 0, nil
	}
}

// advanceLegacyPlaybackIfDelivered moves the shared cursor when a legacy client
// received this line live. CHATHISTORY-only delivery must not advance it.
func (s *Session) advanceLegacyPlaybackIfDelivered(msg irc.Message, deliveredToLegacy bool) {
	if !deliveredToLegacy {
		return
	}
	ts, ok := msg.Tag("time")
	if !ok || ts == "" {
		return
	}
	targets := s.historyTargets(msg)
	for _, target := range targets {
		_, _, _ = s.setPlaybackCursorIfNewer(target, ts)
	}
}

func legacyPlaybackCommands(msg irc.Message) bool {
	return history.IsLegacyReplayCommand(msg.Command)
}
