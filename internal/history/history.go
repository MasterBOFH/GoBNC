// Package history stores and serves CHATHISTORY.
package history

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// Record is a chat line to persist.
type Record struct {
	NetworkID int64
	Target    string
	Time      time.Time
	MsgID     string
	Command   string
	Source    string
	Raw       string
	Text      string
}

// Sender can receive IRC messages (downlink).
type Sender interface {
	Send(msg irc.Message) error
	HasCap(name string) bool
}

// Store wraps store.Store for history operations.
type Store struct {
	db       *store.Store
	batchSeq uint64
	maxLimit int
}

// New creates a history store.
func New(db *store.Store) *Store {
	return &Store{db: db, maxLimit: 100}
}

// MaxLimit returns the configured max lines per query.
func (h *Store) MaxLimit() int { return h.maxLimit }

// Store persists a record.
func (h *Store) Store(ctx context.Context, r Record) error {
	return h.db.InsertMessage(ctx, store.Message{
		NetworkID: r.NetworkID,
		Target:    r.Target,
		Time:      r.Time,
		MsgID:     r.MsgID,
		Command:   r.Command,
		Source:    r.Source,
		Raw:       r.Raw,
		Text:      r.Text,
	})
}

// HandleCHATHISTORY serves LATEST, BEFORE, AFTER, AROUND, BETWEEN.
func (h *Store) HandleCHATHISTORY(s Sender, networkID int64, msg irc.Message) error {
	if !s.HasCap("chathistory") && !s.HasCap("draft/chathistory") {
		return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "capability not enabled"}})
	}
	if len(msg.Params) < 2 {
		return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "need subcommand"}})
	}
	sub := strings.ToUpper(msg.Params[0])
	target := msg.Params[1]
	limit := 50
	q := store.HistoryQuery{NetworkID: networkID, Target: target}

	switch sub {
	case "LATEST":
		// CHATHISTORY LATEST <target> <timestamp|*> <limit>
		q.Latest = true
		if len(msg.Params) >= 4 {
			if n, err := strconv.Atoi(msg.Params[3]); err == nil {
				limit = n
			}
		}
		if len(msg.Params) >= 3 && msg.Params[2] != "*" {
			if t, err := parseSelector(msg.Params[2]); err == nil {
				q.Before = &t
			}
		}
	case "BEFORE":
		// CHATHISTORY BEFORE <target> <timestamp> <limit>
		if len(msg.Params) < 3 {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "BEFORE needs timestamp"}})
		}
		t, err := parseSelector(msg.Params[2])
		if err != nil {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "bad timestamp"}})
		}
		q.Before = &t
		if len(msg.Params) >= 4 {
			if n, err := strconv.Atoi(msg.Params[3]); err == nil {
				limit = n
			}
		}
	case "AFTER":
		// CHATHISTORY AFTER <target> <timestamp> <limit>
		if len(msg.Params) < 3 {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "AFTER needs timestamp"}})
		}
		t, err := parseSelector(msg.Params[2])
		if err != nil {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "bad timestamp"}})
		}
		q.After = &t
		if len(msg.Params) >= 4 {
			if n, err := strconv.Atoi(msg.Params[3]); err == nil {
				limit = n
			}
		}
	case "AROUND":
		// CHATHISTORY AROUND <target> <timestamp> <limit>
		if len(msg.Params) < 3 {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "AROUND needs timestamp"}})
		}
		t, err := parseSelector(msg.Params[2])
		if err != nil {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "bad timestamp"}})
		}
		q.Around = &t
		if len(msg.Params) >= 4 {
			if n, err := strconv.Atoi(msg.Params[3]); err == nil {
				limit = n
			}
		}
	case "BETWEEN":
		// CHATHISTORY BETWEEN <target> <timestamp> <timestamp> <limit>
		if len(msg.Params) < 4 {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "BETWEEN needs two timestamps"}})
		}
		a, err1 := parseSelector(msg.Params[2])
		b, err2 := parseSelector(msg.Params[3])
		if err1 != nil || err2 != nil {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "bad timestamp"}})
		}
		// Spec: first is start, second is end; return messages strictly between, ascending toward end.
		if a.After(b) {
			a, b = b, a
		}
		q.After = &a
		q.Before = &b
		q.Between = true
		if len(msg.Params) >= 5 {
			if n, err := strconv.Atoi(msg.Params[4]); err == nil {
				limit = n
			}
		}
	default:
		return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "unsupported subcommand " + sub}})
	}

	if limit > h.maxLimit {
		limit = h.maxLimit
	}
	if limit < 1 {
		limit = 1
	}
	q.Limit = limit
	q.Commands = historyCommandsFor(s)

	msgs, err := h.db.QueryMessages(context.Background(), q)
	if err != nil {
		return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "TEMPORARILY_UNAVAILABLE", err.Error()}})
	}
	return h.sendBatch(s, target, msgs)
}

// historyCommandsFor returns the command filter for a CHATHISTORY reply.
// Without event-playback, only PRIVMSG/NOTICE (and TAGMSG if message-tags).
func historyCommandsFor(s Sender) []string {
	if s.HasCap("event-playback") || s.HasCap("draft/event-playback") {
		return nil // all stored event types
	}
	cmds := []string{"PRIVMSG", "NOTICE"}
	if s.HasCap("message-tags") {
		cmds = append(cmds, "TAGMSG")
	}
	return cmds
}

func (h *Store) sendBatch(s Sender, target string, msgs []store.Message) error {
	id := fmt.Sprintf("h%d", atomic.AddUint64(&h.batchSeq, 1))
	if err := s.Send(irc.Message{Command: "BATCH", Params: []string{"+" + id, "chathistory", target}}); err != nil {
		return err
	}
	for _, m := range msgs {
		raw := m.Raw
		if raw == "" {
			continue
		}
		parsed, err := irc.Parse(raw)
		if err != nil {
			continue
		}
		if parsed.Tags == nil {
			parsed.Tags = map[string]string{}
		}
		parsed.Tags["batch"] = id
		if _, ok := parsed.Tag("time"); !ok {
			parsed.Tags["time"] = m.Time.UTC().Format("2006-01-02T15:04:05.000Z")
		}
		if err := s.Send(parsed); err != nil {
			return err
		}
	}
	return s.Send(irc.Message{Command: "BATCH", Params: []string{"-" + id}})
}

func parseSelector(s string) (time.Time, error) {
	s = strings.TrimPrefix(s, "timestamp=")
	s = strings.TrimPrefix(s, "msgid=") // msgid-as-time not supported; fall through parse
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}
