package session

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

const readMarkerCap = "draft/read-marker"

// handleMARKREAD processes client MARKREAD get/set (bouncer-local; never uplink).
func (s *Session) handleMARKREAD(d Downlink, msg irc.Message) error {
	if len(msg.Params) < 1 || msg.Param(0) == "" {
		return d.Send(failMARKREAD("NEED_MORE_PARAMS", "", "Missing parameters"))
	}
	display := msg.Param(0)
	s.mu.RLock()
	cm := s.isupport.CaseMapping
	s.mu.RUnlock()
	folded := cm.Canonical(display)

	if len(msg.Params) == 1 {
		ts, err := s.getReadMarker(folded)
		if err != nil {
			return d.Send(failMARKREAD("INTERNAL_ERROR", display, "The read timestamp could not be returned"))
		}
		return d.Send(s.markReadMsg(display, ts))
	}

	rawTS := msg.Param(1)
	norm, err := normalizeMarkReadTimestamp(rawTS)
	if err != nil {
		return d.Send(failMARKREAD("INVALID_PARAMS", rawTS, "Invalid parameters"))
	}
	stored, updated, err := s.setReadMarkerIfNewer(folded, norm)
	if err != nil {
		return d.Send(failMARKREAD("INTERNAL_ERROR", display, "The read timestamp could not be set"))
	}
	out := s.markReadMsg(display, stored)
	_ = d.Send(out)
	if updated {
		s.broadcastMarkRead(d.ID(), out)
	}
	return nil
}

func (s *Session) getReadMarker(foldedTarget string) (ts string, err error) {
	if s.store != nil && s.Network.ID != 0 {
		return storeGet(s.store, s.Network.ID, foldedTarget)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.readMarkers == nil {
		return "", nil
	}
	return s.readMarkers[foldedTarget], nil
}

func storeGet(st *store.Store, networkID int64, target string) (string, error) {
	ts, ok, err := st.GetReadMarker(context.Background(), networkID, target)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", nil
	}
	return ts, nil
}

func (s *Session) setReadMarkerIfNewer(foldedTarget, ts string) (stored string, updated bool, err error) {
	if s.store != nil && s.Network.ID != 0 {
		return s.store.SetReadMarkerIfNewer(context.Background(), s.Network.ID, foldedTarget, ts)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readMarkers == nil {
		s.readMarkers = make(map[string]string)
	}
	cur := s.readMarkers[foldedTarget]
	if cur != "" {
		cmp, err := compareMarkTimes(cur, ts)
		if err != nil {
			return "", false, err
		}
		if cmp >= 0 {
			return cur, false, nil
		}
	}
	s.readMarkers[foldedTarget] = ts
	return ts, true, nil
}

func compareMarkTimes(a, b string) (int, error) {
	ta, err := parseMarkTime(a)
	if err != nil {
		return 0, err
	}
	tb, err := parseMarkTime(b)
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

func parseMarkTime(s string) (time.Time, error) {
	s = strings.TrimPrefix(s, "timestamp=")
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid time")
}

// normalizeMarkReadTimestamp accepts timestamp=… or bare ISO @time; rejects *.
func normalizeMarkReadTimestamp(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "*" {
		return "", fmt.Errorf("invalid")
	}
	s := strings.TrimPrefix(raw, "timestamp=")
	if _, err := parseMarkTime(s); err != nil {
		return "", err
	}
	return s, nil
}

func (s *Session) markReadMsg(displayTarget, ts string) irc.Message {
	param := "*"
	if ts != "" {
		param = "timestamp=" + ts
	}
	return irc.Message{
		Source:  ServerName,
		Command: "MARKREAD",
		Params:  []string{displayTarget, param},
	}
}

func failMARKREAD(code, context, text string) irc.Message {
	params := []string{"MARKREAD", code}
	if context != "" {
		params = append(params, context)
	}
	params = append(params, text)
	return irc.Message{Source: ServerName, Command: "FAIL", Params: params}
}

func (s *Session) broadcastMarkRead(except ClientID, msg irc.Message) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for id, d := range s.downlinks {
		if id == except || !d.HasCap(readMarkerCap) {
			continue
		}
		_ = d.Send(msg)
	}
}

// sendMarkReadAfterJoin sends MARKREAD for a channel after JOIN, before 366.
func (s *Session) sendMarkReadAfterJoin(d Downlink, channel string) {
	if d == nil || !d.HasCap(readMarkerCap) || channel == "" {
		return
	}
	s.mu.RLock()
	cm := s.isupport.CaseMapping
	s.mu.RUnlock()
	folded := cm.Canonical(channel)
	ts, err := s.getReadMarker(folded)
	if err != nil {
		_ = d.Send(failMARKREAD("INTERNAL_ERROR", channel, "The read timestamp could not be returned"))
		return
	}
	_ = d.Send(s.markReadMsg(channel, ts))
}

// maybeSendMarkReadOnSelfJOIN injects MARKREAD after a self JOIN fan-out.
func (s *Session) maybeSendMarkReadOnSelfJOIN(msg irc.Message) {
	if !strings.EqualFold(msg.Command, "JOIN") {
		return
	}
	s.mu.RLock()
	selfNick := ""
	if s.self != nil {
		selfNick = s.self.Nick
	}
	cm := s.isupport.CaseMapping
	s.mu.RUnlock()
	if selfNick == "" || !cm.Equal(msg.Nick(), selfNick) {
		return
	}
	ch := msg.Param(0)
	if ch == "" {
		ch = msg.Trailing()
	}
	if ch == "" {
		return
	}
	s.mu.RLock()
	clients := make([]Downlink, 0, len(s.downlinks))
	for _, d := range s.downlinks {
		if d.HasCap(readMarkerCap) {
			clients = append(clients, d)
		}
	}
	s.mu.RUnlock()
	for _, d := range clients {
		s.sendMarkReadAfterJoin(d, ch)
	}
}
