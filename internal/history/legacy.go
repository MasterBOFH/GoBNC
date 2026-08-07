package history

import (
	"context"
	"fmt"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// DefaultLegacyPlaybackMax matches config.DefaultLegacyPlaybackMax.
const DefaultLegacyPlaybackMax = 50000

// LegacyPlaybackHorizon is used when no playback cursor exists yet.
// Overridable in tests.
var LegacyPlaybackHorizon = 7 * 24 * time.Hour

// LegacyCommands are the only IRC commands included in legacy attach replay.
var LegacyCommands = []string{"PRIVMSG", "NOTICE"}

// QueryLegacyAfter returns PRIVMSG/NOTICE after `after`, oldest-first.
// Events (JOIN/PART/…) and TAGMSG are never included. No CHATHISTORY batching.
// If legacyPlaybackMax is 0, results are unlimited; otherwise capped at that many lines.
func (h *Store) QueryLegacyAfter(ctx context.Context, networkID int64, target string, after time.Time) ([]store.Message, error) {
	if h == nil || h.db == nil {
		return nil, nil
	}
	limit := h.legacyPlaybackMax
	if limit < 0 {
		limit = DefaultLegacyPlaybackMax
	}
	if limit == 0 {
		limit = -1 // unlimited for store.QueryMessages
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
// Returns an error when Raw is empty or does not parse.
func ParseStoredLine(m store.Message) (irc.Message, error) {
	if m.Raw == "" {
		return irc.Message{}, fmt.Errorf("empty raw")
	}
	return irc.Parse(m.Raw)
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
