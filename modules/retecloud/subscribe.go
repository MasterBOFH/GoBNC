package retecloud

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// requestSubscribe answers one subscribe (a subscribe_buffer, or an inline sync_session one):
// the buffer's state from a snapshot, its last lines from history, then live lines. snap is a
// snapshot already placed in the stream by this op (the bootstrap's); nil takes a fresh one.
// The subscription starts at that snapshot's seq, and history's newest id, so a line is neither
// lost between the two nor sent twice.
func (m *Module) requestSubscribe(c *wsConn, n *network, rid string, req SubscribeBufferRequest, snap *wire.Snapshot) {
	sid := sessionID(n.id)
	buffer := strings.TrimSpace(req.Buffer)
	if buffer == "" {
		c.send(errorFrame(frameSubscribeBuffer, rid, "buffer is required"))
		return
	}
	history := liteDefaultHistory
	if req.History != nil {
		history = max(0, min(*req.History, liteMaxHistory))
	}
	if req.Anchor != "" && req.Anchor != wire.AnchorFirstUnread {
		c.send(errorFrame(frameSubscribeBuffer, rid, "Unknown anchor"))
		return
	}
	ls := c.sessions[n.id]
	if ls == nil {
		c.send(errorFrame(frameSubscribeBuffer, rid, "sync_session with sync_mode lite required"))
		return
	}
	if _, already := ls.subs[n.key(buffer)]; !already && len(ls.subs) >= liteMaxSubscriptions {
		c.send(errorFrame(frameSubscribeBuffer, rid, "Too many subscriptions"))
		return
	}
	if snap == nil {
		fresh, err := m.snapshot(n.id)
		if err != nil {
			c.send(ServerMessage{Type: frameSubscribeBuffer + "_error", RequestID: rid, SessionID: sid, Error: "Error retrieving messages"})
			return
		}
		snap = &fresh
		if ls = c.sessions[n.id]; ls == nil { // unsynced while catching up
			c.send(errorFrame(frameSubscribeBuffer, rid, "sync_session with sync_mode lite required"))
			return
		}
	}
	state := bufferState(n, *snap, buffer, req.Members.mode)
	name, _ := state["buffer"].(string)

	var msgs []wire.HistoryMessage
	hasMore := false
	page := map[string]interface{}{}
	if history > 0 {
		var err error
		msgs, hasMore, err = m.subscribeHistory(n, c.deviceID, name, history, req, page)
		if err != nil {
			c.send(ServerMessage{Type: frameSubscribeBuffer + "_error", RequestID: rid, SessionID: sid, Error: "Error retrieving messages"})
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
	ls.subs[n.key(name)] = &liteSub{name: name, fromSeq: snap.Seq, historyMaxID: maxID, members: req.Members.mode}
	data := map[string]interface{}{"buffer": name, "state": state, "messages": out, "has_more": hasMore}
	for k, v := range page {
		data[k] = v
	}
	c.send(ServerMessage{Type: frameSubscribeBuffer + "_success", RequestID: rid, SessionID: sid, Data: data})
}

// subscribeHistory fetches a subscribe's history page: the tail after after_id (gap when more
// was missed than fits), the page around the first unread line, or the latest lines.
func (m *Module) subscribeHistory(n *network, device, buffer string, limit int, req SubscribeBufferRequest, page map[string]interface{}) ([]wire.HistoryMessage, bool, error) {
	get := func(p wire.HistoryMessagesParams) (wire.HistoryMessagesResult, error) {
		p.Network, p.Buffer = n.id, buffer
		var res wire.HistoryMessagesResult
		err := m.call(wire.MethodHistoryMessages, p, &res)
		return res, err
	}
	switch {
	case req.AfterID != nil:
		latest, err := get(wire.HistoryMessagesParams{Limit: limit})
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
			page["anchor_id"] = anchor
			page["has_more_after"] = res.HasAfter
			return res.Messages, res.HasBefore, nil
		}
	}
	res, err := get(wire.HistoryMessagesParams{Limit: limit})
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
