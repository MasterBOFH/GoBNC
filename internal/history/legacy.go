package history

import (
	"context"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// LegacyPlaybackMax is the safety cap on lines played per target on attach.
// Overridable in tests.
var LegacyPlaybackMax = 50000

// LegacyPlaybackHorizon is used when no playback cursor exists yet.
// Overridable in tests.
var LegacyPlaybackHorizon = 7 * 24 * time.Hour

// LegacyCommands are the only IRC commands included in legacy attach replay.
var LegacyCommands = []string{"PRIVMSG", "NOTICE"}

// QueryLegacyAfter returns PRIVMSG/NOTICE after `after`, oldest-first.
// Events (JOIN/PART/…) and TAGMSG are never included. No CHATHISTORY batching.
func (h *Store) QueryLegacyAfter(ctx context.Context, networkID int64, target string, after time.Time) ([]store.Message, error) {
	if h == nil || h.db == nil {
		return nil, nil
	}
	limit := LegacyPlaybackMax
	if limit < 1 {
		limit = 1
	}
	q := store.HistoryQuery{
		NetworkID: networkID,
		Target:    target,
		After:     &after,
		Limit:     limit,
		Commands:  append([]string(nil), LegacyCommands...),
	}
	return h.db.QueryMessages(ctx, q)
}

// ParseStoredLine turns a stored Raw into an IRC message for downlink send.
func ParseStoredLine(m store.Message) (irc.Message, bool) {
	if m.Raw == "" {
		return irc.Message{}, false
	}
	parsed, err := irc.Parse(m.Raw)
	if err != nil {
		return irc.Message{}, false
	}
	return parsed, true
}

// EnsureLineMsgID ensures msgid on a stored line for legacy/CHATHISTORY playback.
func (h *Store) EnsureLineMsgID(msg *irc.Message, m *store.Message) {
	h.ensurePlaybackMsgID(msg, m)
}

// IsLegacyReplayCommand reports whether cmd is sent on legacy attach replay.
func IsLegacyReplayCommand(cmd string) bool {
	switch cmd {
	case "PRIVMSG", "NOTICE":
		return true
	default:
		return false
	}
}
