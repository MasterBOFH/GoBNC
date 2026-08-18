// Package history stores and serves CHATHISTORY.
package history

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
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
	// KeeperSeq: see store.Message.KeeperSeq's doc comment. 0 for a record
	// with no keeper line behind it.
	KeeperSeq uint64
}

// Sender can receive IRC messages (downlink).
type Sender interface {
	Send(msg irc.Message) error
	HasCap(name string) bool
}

// Store wraps store.Store for history operations.
type Store struct {
	db                *store.Store
	batchSeq          uint64
	maxLimit          int
	legacyPlaybackMax int // 0 = unlimited; <0 treated as DefaultLegacyPlaybackMax
	log               *slog.Logger
}

// DefaultChatHistoryMax is the default max lines per CHATHISTORY query.
const DefaultChatHistoryMax = 100

// New creates a history store with default CHATHISTORY and legacy playback limits.
func New(db *store.Store) *Store {
	return NewWithLimits(db, DefaultChatHistoryMax, DefaultLegacyPlaybackMax)
}

// NewWithLimits creates a history store.
// legacyPlaybackMax 0 means unlimited attach playback; positive caps per target.
func NewWithLimits(db *store.Store, chathistoryMax, legacyPlaybackMax int) *Store {
	if chathistoryMax <= 0 {
		chathistoryMax = DefaultChatHistoryMax
	}
	return &Store{db: db, maxLimit: chathistoryMax, legacyPlaybackMax: legacyPlaybackMax}
}

// SetLogger sets an optional logger for history playback diagnostics.
func (h *Store) SetLogger(l *slog.Logger) {
	if h != nil {
		h.log = l
	}
}

// SetLegacyPlaybackMax sets the per-target legacy attach cap (0 = unlimited). For tests.
func (h *Store) SetLegacyPlaybackMax(n int) {
	if h != nil {
		h.legacyPlaybackMax = n
	}
}

// SetMaxLimit sets the max lines per CHATHISTORY query (and ISUPPORT CHATHISTORY=N).
// Non-positive values become DefaultChatHistoryMax.
func (h *Store) SetMaxLimit(n int) {
	if h == nil {
		return
	}
	if n <= 0 {
		n = DefaultChatHistoryMax
	}
	h.maxLimit = n
}

// MaxLimit returns the configured max lines per CHATHISTORY query.
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
		KeeperSeq: r.KeeperSeq,
	})
}

// DistinctTargets returns every distinct target with stored history on networkID.
func (h *Store) DistinctTargets(ctx context.Context, networkID int64) ([]string, error) {
	return h.db.DistinctTargets(ctx, networkID)
}

// HandleCHATHISTORY serves LATEST, BEFORE, AFTER, AROUND, BETWEEN, TARGETS.
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
	ctx := context.Background()

	applySel := func(raw string, setBefore, setAfter, setAround bool) error {
		sel, err := parseSelector(raw)
		if err != nil {
			return err
		}
		bound, ts, err := h.resolveSelector(ctx, networkID, target, sel)
		if err != nil {
			return err
		}
		if sel.kind == selMsgid && bound == nil {
			// Unknown msgid: empty result (valid pagination end).
			return errMsgIDMissing
		}
		if setBefore {
			if bound != nil {
				q.BeforeBound = bound
			} else {
				q.Before = ts
			}
		}
		if setAfter {
			if bound != nil {
				q.AfterBound = bound
			} else {
				q.After = ts
			}
		}
		if setAround {
			if bound != nil {
				q.AroundBound = bound
			} else {
				q.Around = ts
			}
		}
		return nil
	}

	switch sub {
	case "LATEST":
		// CHATHISTORY LATEST <target> <*|timestamp|msgid> <limit>
		q.Latest = true
		if len(msg.Params) >= 4 {
			if n, err := strconv.Atoi(msg.Params[3]); err == nil {
				limit = n
			}
		}
		if len(msg.Params) >= 3 && msg.Params[2] != "*" {
			// Spec: restrict to messages after and excluding the selector.
			if err := applySel(msg.Params[2], false, true, false); err != nil {
				if err == errMsgIDMissing {
					return h.sendBatch(s, target, nil)
				}
				return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", msg.Params[2], "bad selector"}})
			}
		}
	case "BEFORE":
		if len(msg.Params) < 3 {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "BEFORE needs selector"}})
		}
		if err := applySel(msg.Params[2], true, false, false); err != nil {
			if err == errMsgIDMissing {
				return h.sendBatch(s, target, nil)
			}
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", msg.Params[2], "bad selector"}})
		}
		if len(msg.Params) >= 4 {
			if n, err := strconv.Atoi(msg.Params[3]); err == nil {
				limit = n
			}
		}
	case "AFTER":
		if len(msg.Params) < 3 {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "AFTER needs selector"}})
		}
		if err := applySel(msg.Params[2], false, true, false); err != nil {
			if err == errMsgIDMissing {
				return h.sendBatch(s, target, nil)
			}
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", msg.Params[2], "bad selector"}})
		}
		if len(msg.Params) >= 4 {
			if n, err := strconv.Atoi(msg.Params[3]); err == nil {
				limit = n
			}
		}
	case "AROUND":
		if len(msg.Params) < 3 {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "AROUND needs selector"}})
		}
		if err := applySel(msg.Params[2], false, false, true); err != nil {
			if err == errMsgIDMissing {
				return h.sendBatch(s, target, nil)
			}
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", msg.Params[2], "bad selector"}})
		}
		if len(msg.Params) >= 4 {
			if n, err := strconv.Atoi(msg.Params[3]); err == nil {
				limit = n
			}
		}
	case "BETWEEN":
		if len(msg.Params) < 4 {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "BETWEEN needs two selectors"}})
		}
		selA, err1 := parseSelector(msg.Params[2])
		selB, err2 := parseSelector(msg.Params[3])
		if err1 != nil || err2 != nil {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "bad selector"}})
		}
		boundA, tsA, err := h.resolveSelector(ctx, networkID, target, selA)
		if err != nil {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", msg.Params[2], "bad selector"}})
		}
		boundB, tsB, err := h.resolveSelector(ctx, networkID, target, selB)
		if err != nil {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", msg.Params[3], "bad selector"}})
		}
		if (selA.kind == selMsgid && boundA == nil) || (selB.kind == selMsgid && boundB == nil) {
			return h.sendBatch(s, target, nil)
		}
		// Order bounds so After < Before in store order.
		if boundA != nil && boundB != nil {
			if boundA.Time.After(boundB.Time) || (boundA.Time.Equal(boundB.Time) && boundA.ID > boundB.ID) {
				boundA, boundB = boundB, boundA
			}
			q.AfterBound, q.BeforeBound = boundA, boundB
		} else if tsA != nil && tsB != nil {
			a, b := *tsA, *tsB
			if a.After(b) {
				a, b = b, a
			}
			q.After, q.Before = &a, &b
		} else {
			// Mixed msgid/timestamp: convert timestamp to a bound at id=0 / MaxInt.
			if boundA == nil {
				boundA = &store.HistoryBound{Time: *tsA, ID: 0}
			}
			if boundB == nil {
				boundB = &store.HistoryBound{Time: *tsB, ID: 1<<63 - 1}
			}
			if boundA.Time.After(boundB.Time) || (boundA.Time.Equal(boundB.Time) && boundA.ID > boundB.ID) {
				boundA, boundB = boundB, boundA
			}
			q.AfterBound, q.BeforeBound = boundA, boundB
		}
		q.Between = true
		if len(msg.Params) >= 5 {
			if n, err := strconv.Atoi(msg.Params[4]); err == nil {
				limit = n
			}
		}
	case "TARGETS":
		// CHATHISTORY TARGETS <timestamp=..> <timestamp=..> <limit> — lists
		// targets (channels and PM peers), not messages; see handleTargets.
		if len(msg.Params) < 4 {
			return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "TARGETS needs two timestamps and a limit"}})
		}
		return h.handleTargets(s, networkID, msg.Params[1], msg.Params[2], msg.Params[3])
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

	msgs, err := h.db.QueryMessages(ctx, q)
	if err != nil {
		return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "TEMPORARILY_UNAVAILABLE", "history unavailable"}})
	}
	return h.sendBatch(s, target, msgs)
}

var errMsgIDMissing = fmt.Errorf("msgid not found")

type selectorKind int

const (
	selTimestamp selectorKind = iota
	selMsgid
)

type historySelector struct {
	kind  selectorKind
	time  time.Time
	msgid string
}

func parseSelector(s string) (historySelector, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "msgid=") {
		id := strings.TrimPrefix(s, "msgid=")
		if id == "" {
			return historySelector{}, fmt.Errorf("empty msgid")
		}
		return historySelector{kind: selMsgid, msgid: id}, nil
	}
	if strings.HasPrefix(s, "timestamp=") {
		s = strings.TrimPrefix(s, "timestamp=")
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return historySelector{kind: selTimestamp, time: t}, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return historySelector{}, err
	}
	return historySelector{kind: selTimestamp, time: t}, nil
}

// resolveSelector returns a store bound for msgid, or a timestamp pointer for timestamp=.
func (h *Store) resolveSelector(ctx context.Context, networkID int64, target string, sel historySelector) (*store.HistoryBound, *time.Time, error) {
	switch sel.kind {
	case selMsgid:
		m, err := h.db.MessageByMsgID(ctx, networkID, target, sel.msgid)
		if err != nil {
			return nil, nil, err
		}
		if m == nil {
			return nil, nil, nil
		}
		return &store.HistoryBound{Time: m.Time, ID: m.ID}, nil, nil
	default:
		t := sel.time
		return nil, &t, nil
	}
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

// handleTargets serves CHATHISTORY TARGETS. Unlike the other subcommands it
// lists targets (channels and PM peers) with history, not messages — see
// https://ircv3.net/specs/extensions/chathistory#chathistory-targets. A
// target is excluded unless its *latest* message falls within [aRaw, bRaw],
// even if it has other messages inside that range.
func (h *Store) handleTargets(s Sender, networkID int64, aRaw, bRaw, limitRaw string) error {
	selA, errA := parseSelector(aRaw)
	selB, errB := parseSelector(bRaw)
	if errA != nil || errB != nil || selA.kind != selTimestamp || selB.kind != selTimestamp {
		return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "INVALID_PARAMS", "TARGETS needs two timestamps, not msgids"}})
	}
	limit := 50
	if n, err := strconv.Atoi(limitRaw); err == nil {
		limit = n
	}
	if limit > h.maxLimit {
		limit = h.maxLimit
	}
	if limit < 1 {
		limit = 1
	}
	// Spec: "This may be forwards or backwards in time" — which of the two
	// params is later sets result order, matching the real bootstrap use
	// case (client sends now, long-ago, limit and wants its N most
	// recently active conversations, newest first).
	desc := selA.time.After(selB.time)
	lo, hi := selA.time, selB.time
	if lo.After(hi) {
		lo, hi = hi, lo
	}
	targets, err := h.db.TargetsBetween(context.Background(), networkID, lo, hi, historyCommandsFor(s), limit, desc)
	if err != nil {
		return s.Send(irc.Message{Command: "FAIL", Params: []string{"CHATHISTORY", "TEMPORARILY_UNAVAILABLE", "history unavailable"}})
	}
	return h.sendTargetsBatch(s, targets)
}

// sendTargetsBatch sends the draft/chathistory-targets batch handleTargets
// resolved to. Spec: this batch type takes no target parameter (unlike the
// "chathistory" batch other subcommands use), and each line is
// "CHATHISTORY TARGETS <name> <timestamp>", not a replayed message.
func (h *Store) sendTargetsBatch(s Sender, targets []store.TargetActivity) error {
	id := fmt.Sprintf("h%d", atomic.AddUint64(&h.batchSeq, 1))
	if err := s.Send(irc.Message{Command: "BATCH", Params: []string{"+" + id, "draft/chathistory-targets"}}); err != nil {
		return err
	}
	for _, t := range targets {
		line := irc.Message{
			Command: "CHATHISTORY",
			Params:  []string{"TARGETS", t.Target, t.Time.UTC().Format("2006-01-02T15:04:05.000Z")},
			Tags:    map[string]string{"batch": id},
		}
		if err := s.Send(line); err != nil {
			return err
		}
	}
	return s.Send(irc.Message{Command: "BATCH", Params: []string{"-" + id}})
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
			if h.log != nil {
				h.log.Warn("history parse skip",
					"target", target,
					"msgid", m.MsgID,
					"time", m.Time.UTC().Format(time.RFC3339Nano),
					"line", gobnclog.RedactIRC(raw),
					"err", err)
			}
			continue
		}
		if parsed.Tags == nil {
			parsed.Tags = map[string]string{}
		}
		parsed.Tags["batch"] = id
		if _, ok := parsed.Tag("time"); !ok {
			parsed.Tags["time"] = m.Time.UTC().Format("2006-01-02T15:04:05.000Z")
		}
		h.ensurePlaybackMsgID(&parsed, &m)
		// Keep Raw body; Wire() replaces only the tag prefix (batch/msgid/time).
		if err := s.Send(parsed); err != nil {
			return err
		}
	}
	return s.Send(irc.Message{Command: "BATCH", Params: []string{"-" + id}})
}

// ensurePlaybackMsgID restores msgid from the messages.msgid column when the
// stored Raw lacked the tag. Never invents a new ID — playback must match the
// msgid that was (or would have been) on the wire when the line was stored.
func (h *Store) ensurePlaybackMsgID(msg *irc.Message, m *store.Message) {
	if _, ok := msg.Tag("msgid"); ok {
		return
	}
	if m.MsgID == "" {
		return
	}
	if msg.Tags == nil {
		msg.Tags = map[string]string{}
	}
	msg.Tags["msgid"] = m.MsgID
}
