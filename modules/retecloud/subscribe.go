package retecloud

import (
	"encoding/json"
	"math"
	"strings"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// requestSubscribe answers one subscribe (a subscribe_buffer, or an inline sync_session one):
// the buffer's state from a snapshot, its last lines from history, then live lines. snap is a
// snapshot already placed in the stream by this op (the bootstrap's); nil takes a fresh one.
// The subscription starts at that snapshot's seq, and history's newest id, so a line is neither
// lost between the two nor sent twice. History is read after the snapshot, so it is bounded to
// the snapshot's point (snap.LastID): a line stored later (a PART landing in between) comes live,
// after the state it changes, never as history the state does not reflect.
func (m *Module) requestSubscribe(c *wsConn, n *network, rid string, req SubscribeBufferRequest, snap *wire.Snapshot) {
	sid := sessionID(n.id)
	fail := func(text string) { c.send(subscribeError(rid, sid, req.Buffer, text)) }
	buffer := strings.TrimSpace(req.Buffer)
	if buffer == "" {
		fail("buffer is required")
		return
	}
	history := liteDefaultHistory
	if req.History != nil {
		history = max(0, min(*req.History, liteMaxHistory))
	}
	if req.Anchor != "" && req.Anchor != wire.AnchorFirstUnread {
		fail("Unknown anchor")
		return
	}
	ls := c.sessions[n.id]
	if ls == nil {
		fail("sync_session with sync_mode lite required")
		return
	}
	if _, already := ls.subs[n.key(buffer)]; !already && len(ls.subs) >= liteMaxSubscriptions {
		fail("Too many subscriptions")
		return
	}
	var upTo int64
	if snap == nil {
		fresh, err := m.snapshot(n.id)
		switch {
		case isNotFound(err):
			// A stopped network: no Session, so no state beyond the buffer's kind (a channel is
			// not joined) and no live lines until it restarts (resumeConn then carries the
			// subscription on from lastSent). History is the store's; nothing newer is stored
			// meanwhile, so there is no point to bound it to.
			fresh, upTo = wire.Snapshot{}, math.MaxInt64-1
		case err != nil:
			fail("Error retrieving messages")
			return
		default:
			upTo = fresh.LastID
		}
		snap = &fresh
		if ls = c.sessions[n.id]; ls == nil { // unsynced while catching up
			fail("sync_session with sync_mode lite required")
			return
		}
	} else {
		upTo = snap.LastID
	}
	if m.afterSubscribeSnapshot != nil {
		m.afterSubscribeSnapshot(n.id)
	}
	state := bufferState(n, *snap, buffer, req.Members.mode)
	name, _ := state["buffer"].(string)

	var msgs []wire.HistoryMessage
	hasMore := false
	page := map[string]interface{}{}
	if history > 0 {
		var err error
		msgs, hasMore, err = m.subscribeHistory(n, name, history, req, page, upTo)
		if err != nil {
			fail("Error retrieving messages")
			return
		}
	}
	var maxID int64
	out := make([]MessageResponse, 0, len(msgs))
	for _, msg := range msgs {
		out = append(out, messageResponse(msg))
		maxID = max(maxID, msg.ID)
	}
	if req.AfterID != nil && *req.AfterID > maxID {
		maxID = *req.AfterID
	}
	if _, ok := page["has_more_after"]; !ok {
		page["has_more_after"] = false
	}
	ls.subs[n.key(name)] = &liteSub{name: name, fromSeq: snap.Seq, historyMaxID: maxID, lastSent: maxID, members: req.Members.mode}
	data := map[string]interface{}{"buffer": name, "state": state, "messages": out, "has_more": hasMore}
	for k, v := range page {
		data[k] = v
	}
	c.send(ServerMessage{Type: frameSubscribeBuffer + "_success", RequestID: rid, SessionID: sid, Data: data})
}

// subscribeHistory fetches a subscribe's history page: the tail after after_id (gap when more
// was missed than fits), the page around the first unread line, or the latest lines — each
// holding only lines stored at or before upTo, the snapshot's newest message id.
func (m *Module) subscribeHistory(n *network, buffer string, limit int, req SubscribeBufferRequest, page map[string]interface{}, upTo int64) ([]wire.HistoryMessage, bool, error) {
	get := func(p wire.HistoryMessagesParams) (wire.HistoryMessagesResult, error) {
		p.Network, p.Buffer = n.id, buffer
		var res wire.HistoryMessagesResult
		err := m.call(wire.MethodHistoryMessages, p, &res)
		return res, err
	}
	before := upTo + 1
	newest := func() (wire.HistoryMessagesResult, error) {
		return get(wire.HistoryMessagesParams{BeforeID: &before, Limit: limit})
	}
	switch {
	case req.AfterID != nil:
		latest, err := newest()
		if err != nil {
			return nil, false, err
		}
		var tail []wire.HistoryMessage
		for _, msg := range latest.Messages {
			if msg.ID > *req.AfterID {
				tail = append(tail, msg)
			}
		}
		gap := false
		if len(tail) > 0 && len(tail) == len(latest.Messages) && latest.HasBefore {
			// Something between after_id and the page: the client missed more than fits.
			next, err := get(wire.HistoryMessagesParams{AfterID: req.AfterID, Limit: 1})
			if err != nil {
				return nil, false, err
			}
			gap = len(next.Messages) == 1 && next.Messages[0].ID < tail[0].ID
		}
		page["gap"] = gap
		hasMore := len(tail) > 0 && (latest.HasBefore || len(tail) < len(latest.Messages))
		return tail, hasMore, nil
	case req.Anchor == wire.AnchorFirstUnread:
		var mark wire.ReadMarkerGetResult
		if err := m.call(wire.MethodReadMarkersGet, wire.ReadMarkerGetParams{Network: n.id, Buffer: buffer}, &mark); err != nil {
			return nil, false, err
		}
		if mark.ID > 0 {
			anchor := mark.ID + 1
			res, err := get(wire.HistoryMessagesParams{AroundID: &anchor, Limit: limit})
			if err != nil {
				return nil, false, err
			}
			// Lines after the snapshot's point come live; they are not this page's to hold.
			kept := res.Messages[:0]
			for _, msg := range res.Messages {
				if msg.ID <= upTo {
					kept = append(kept, msg)
				}
			}
			page["anchor_id"] = anchor
			page["has_more_after"] = res.HasAfter && len(kept) == len(res.Messages)
			return kept, res.HasBefore, nil
		}
	}
	res, err := newest()
	return res.Messages, res.HasBefore, err
}

// messageResponse is a history line as retecloud-api replays a stored raw line.
func messageResponse(msg wire.HistoryMessage) MessageResponse {
	ts, _ := time.Parse(time.RFC3339Nano, msg.Time)
	data, _ := json.Marshal(storedLine{Raw: msg.Raw, BufferName: msg.Buffer})
	return MessageResponse{ID: msg.ID, Timestamp: ts, Event: "Raw", EventData: data}
}

// bufferState is the buffer_state a subscriber starts from (retecloud-api's bufferSnapshot).
func bufferState(n *network, snap wire.Snapshot, buffer string, members membersMode) map[string]interface{} {
	if buffer == "" || buffer == wire.ServerBuffer {
		return map[string]interface{}{"buffer": wire.ServerBuffer, "kind": "server"}
	}
	key := n.key(buffer)
	if n.isChannel(buffer) {
		for _, ch := range snap.Channels {
			if ch.Key == key {
				return channelState(ch, members, snap.ISupport["PREFIX"])
			}
		}
		return map[string]interface{}{"buffer": buffer, "kind": "channel", "joined": false}
	}
	out := map[string]interface{}{"buffer": buffer, "kind": "query"}
	for _, ch := range snap.Channels {
		for _, mem := range ch.Members {
			if n.key(mem.Nick) == key {
				out["user"] = map[string]interface{}{
					"nick": mem.Nick, "username": mem.User, "hostname": mem.Host, "realname": "",
					"account": mem.Account, "away": mem.Away, "bot": false, "secure": false, "oper": false,
				}
				return out
			}
		}
	}
	return out
}

func channelState(ch wire.Channel, members membersMode, prefix string) map[string]interface{} {
	var topicTime int64
	if t, err := time.Parse(time.RFC3339, ch.Topic.Time); err == nil {
		topicTime = t.Unix()
	}
	out := map[string]interface{}{
		"buffer":          ch.Name,
		"kind":            "channel",
		"joined":          true,
		"creation_id":     0,
		"topic":           ch.Topic.Text,
		"topic_setter":    ch.Topic.Setter,
		"topic_timestamp": topicTime,
		"modes":           ch.Modes,
		"member_count":    len(ch.Members),
	}
	modes, symbols := prefixRanks(prefix)
	switch members {
	case membersFull:
		users := make([]map[string]interface{}, 0, len(ch.Members))
		for _, mem := range ch.Members {
			users = append(users, map[string]interface{}{"nick": mem.Nick, "modes": modeLetters(mem.Prefixes, modes, symbols)})
		}
		out["users"] = users
	case membersCompact:
		users := make([]string, 0, len(ch.Members))
		for _, mem := range ch.Members {
			users = append(users, compactMember(mem.Nick, mem.Prefixes, modes, symbols))
		}
		out["users"] = users
	}
	return out
}

// prefixRanks parses ISUPPORT PREFIX "(qaohv)~&@%+" into mode letters and their symbols.
func prefixRanks(prefix string) (modes, symbols string) {
	if prefix == "" {
		prefix = "(ov)@+"
	}
	if strings.HasPrefix(prefix, "(") {
		if i := strings.IndexByte(prefix, ')'); i > 0 {
			return prefix[1:i], prefix[i+1:]
		}
	}
	return "", prefix
}

// modeLetters turns prefix symbols ("@+") into their mode letters ("ov").
func modeLetters(prefixes, modes, symbols string) string {
	var b strings.Builder
	for i := 0; i < len(prefixes); i++ {
		if j := strings.IndexByte(symbols, prefixes[i]); j >= 0 && j < len(modes) {
			b.WriteByte(modes[j])
		}
	}
	return b.String()
}

// compactMember renders "@+nick" (highest rank first) from symbols or mode letters.
func compactMember(nick, memberModes, modes, symbols string) string {
	have := make([]bool, len(symbols))
	for i := 0; i < len(memberModes); i++ {
		ch := memberModes[i]
		if j := strings.IndexByte(modes, ch); j >= 0 && j < len(symbols) {
			have[j] = true
		} else if j := strings.IndexByte(symbols, ch); j >= 0 {
			have[j] = true
		}
	}
	var b strings.Builder
	for j, ok := range have {
		if ok {
			b.WriteByte(symbols[j])
		}
	}
	b.WriteString(nick)
	return b.String()
}

// subscribeError is a subscribe_buffer_error naming the session and the buffer as the client
// sent them, so a client can tell which of several pipelined subscribes failed.
func subscribeError(rid, sid, buffer, text string) ServerMessage {
	f := errorFrame(frameSubscribeBuffer, rid, text)
	f.SessionID = sid
	f.Data = map[string]interface{}{"buffer": buffer}
	return f
}

func (m *Module) handleSubscribeBuffer(c *wsConn, msg *ClientMessage) {
	var req SubscribeBufferRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		var probe struct { // the buffer, when that much of the data reads
			Buffer string `json:"buffer"`
		}
		_ = json.Unmarshal(msg.Data, &probe)
		c.send(subscribeError(msg.RequestID, msg.SessionID, probe.Buffer, "Invalid request data"))
		return
	}
	if msg.SessionID == "" {
		c.send(subscribeError(msg.RequestID, "", req.Buffer, "session_id is required"))
		return
	}
	n := m.networkFor(msg.SessionID)
	if n == nil || c.sessions[n.id] == nil {
		c.send(subscribeError(msg.RequestID, msg.SessionID, req.Buffer, "sync_session with sync_mode lite required"))
		return
	}
	m.requestSubscribe(c, n, msg.RequestID, req, nil)
}

// handleUnsubscribeBuffer ends a subscription; the buffer's badge goes back to the activity
// stream.
func (m *Module) handleUnsubscribeBuffer(c *wsConn, msg *ClientMessage) {
	var req SubscribeBufferRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil || strings.TrimSpace(req.Buffer) == "" {
		c.send(errorFrame(frameUnsubscribeBuffer, msg.RequestID, "buffer is required"))
		return
	}
	n := m.networkFor(msg.SessionID)
	var ls *liteSession
	if n != nil {
		ls = c.sessions[n.id]
	}
	if ls == nil {
		c.send(errorFrame(frameUnsubscribeBuffer, msg.RequestID, "sync_session with sync_mode lite required"))
		return
	}
	key := n.key(req.Buffer)
	delete(ls.subs, key)
	if ls.activity[key] != nil {
		ls.dirty[key] = true
	}
	c.send(ServerMessage{Type: frameUnsubscribeBuffer + "_success", RequestID: msg.RequestID, SessionID: msg.SessionID,
		Data: map[string]interface{}{"buffer": req.Buffer}})
	m.flushActivity(c, ls)
}
