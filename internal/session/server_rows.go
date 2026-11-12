package session

import (
	"context"
	"encoding/json"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// The Server buffer's module-only rows (Rete A3131). A lite client that was away while the
// uplink dropped and came back resyncs the Server buffer from history, and found nothing there:
// the registration burst is consumed by registration, and a drop or a reconnect left no line.
// So the burst's numerics, the uplink's ERROR and a State row for each drop, connect and
// registration are stored under store.ServerRowsTarget ("*"): module pages of the Server buffer
// serve them, IRC clients never see them (they get the burst live, and their own ERROR), and
// the unread scan counts none of them (it counts PRIVMSG and NOTICE only).

// stateRow is a stored State row's Text: the base protocol's State payload.
type stateRow struct {
	Connected  bool   `json:"connected"`
	Registered bool   `json:"registered"`
	Message    string `json:"message,omitempty"`
	Timestamp  string `json:"timestamp"`
}

// storeServerRow stores msg (a burst numeric or the uplink's ERROR) in the Server buffer's
// module-only rows and returns its id (0 when not stored). seq is its keeper line, so a resumed
// brain's replay stores it once.
func (s *Session) storeServerRow(msg irc.Message, seq uint64) int64 {
	if s.hist == nil || s.Network.ID == 0 {
		return 0
	}
	msg = ensureMessageTime(msg)
	msg = ensureMessageID(msg)
	ts := time.Now().UTC()
	if t, ok := msg.Tag("time"); ok {
		if parsed, err := time.Parse(time.RFC3339Nano, t); err == nil {
			ts = parsed
		}
	}
	msgid, _ := msg.Tag("msgid")
	id, err := s.hist.Store(context.Background(), history.Record{
		NetworkID: s.Network.ID,
		Target:    store.ServerRowsTarget,
		Time:      ts,
		MsgID:     msgid,
		Command:   msg.Command,
		Source:    msg.Source,
		Raw:       msg.Wire(),
		Text:      msg.Trailing(),
		KeeperSeq: seq,
	})
	if err != nil {
		s.log.Warn("store server row", "command", msg.Command, "err", err)
		return 0
	}
	return id
}

// recordState stores the Server buffer's State row for a change to state (with message, the
// reason, for a drop) and then tells the state hook. A drop is stored only after a connect or
// registration was: a reconnect attempt that never got a line stores nothing, so every
// "disconnected" row closes a "connected" one.
func (s *Session) recordState(state, message string) {
	connected := state == wire.NetConnecting || state == wire.NetRegistered
	registered := state == wire.NetRegistered
	s.mu.Lock()
	keep := s.hist != nil && s.Network.ID != 0 && (connected || s.stateRowUp)
	if keep {
		s.stateRowUp = connected
	}
	s.mu.Unlock()
	if keep {
		at := time.Now().UTC()
		row := stateRow{Connected: connected, Registered: registered, Message: message,
			Timestamp: at.Format(time.RFC3339Nano)}
		text, _ := json.Marshal(row)
		if _, err := s.hist.Store(context.Background(), history.Record{
			NetworkID: s.Network.ID,
			Target:    store.ServerRowsTarget,
			Time:      at,
			Command:   wire.HistoryStateCommand,
			Text:      string(text),
		}); err != nil {
			s.log.Warn("store state row", "state", state, "err", err)
		} else {
			s.mu.Lock()
			s.stateRowState, s.stateRowTime = state, row.Timestamp
			s.mu.Unlock()
		}
	}
	s.notifyState(state)
}

// StateRowTime is the time of the State row stored for the network's latest change to state,
// or "" when that change stored none: the live state frame names the row's instant.
func (s *Session) StateRowTime(state string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.stateRowState != state {
		return ""
	}
	return s.stateRowTime
}
