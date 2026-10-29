package retecloud

import (
	"encoding/json"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// handleEvent applies one brain event, in stream order.
func (m *Module) handleEvent(ev wire.Event) {
	switch ev.Event {
	case wire.EventNetwork:
		var n wire.NetworkEvent
		if json.Unmarshal(ev.Data, &n) == nil {
			m.onNetworkEvent(n)
		}
	case wire.EventLine:
		var line wire.LineEvent
		if json.Unmarshal(ev.Data, &line) == nil {
			m.onLine(line)
		}
	}
}

// onLine routes one line to every connection following its network, then keeps the network's
// own state (our nick, joined channels) in step.
func (m *Module) onLine(line wire.LineEvent) {
	n := m.nets[line.Network]
	if n == nil || line.Seq <= n.lastSeq {
		return
	}
	n.lastSeq = line.Seq
	frame := ircLineFrame(line)
	for c := range m.conns {
		if ls := c.sessions[n.id]; ls != nil && m.liteAllows(c, n, ls, line) {
			c.send(frame)
		}
	}
	n.trackSelf(line)
}

// liteAllows decides whether a lite connection gets a line: when one of its buffers is
// subscribed (and the line is newer than that subscription's snapshot and history).
func (m *Module) liteAllows(c *wsConn, n *network, ls *liteSession, line wire.LineEvent) bool {
	for _, b := range line.Buffers {
		sub := ls.subs[b.Key]
		if sub == nil {
			continue
		}
		if line.Seq <= sub.fromSeq || (b.ID > 0 && b.ID <= sub.historyMaxID) {
			return false // already in the subscribe's snapshot or history
		}
		return true
	}
	return false
}

// ircLineFrame is a line as retecloud-api's irc_line: raw, timestamp, and the message id (and
// buffer) when it was stored.
func ircLineFrame(line wire.LineEvent) ServerMessage {
	ts, err := time.Parse(time.RFC3339Nano, line.Time)
	if err != nil {
		ts = time.Now().UTC()
	}
	data := map[string]interface{}{"silent": false, "timestamp": ts, "raw": line.Raw}
	for _, b := range line.Buffers {
		if b.ID > 0 {
			data["id"] = b.ID
			data["buffer"] = b.Name
			break
		}
	}
	return ServerMessage{Type: frameIRCLine, SessionID: sessionID(line.Network), Data: data}
}

// trackSelf updates our nick and joined channels from our own NICK/JOIN/PART and KICKs of us.
func (n *network) trackSelf(line wire.LineEvent) {
	switch line.Kind {
	case wire.LineNick:
		if line.Self {
			if to := lineParam(line.Raw, 0); to != "" {
				n.nick = to
			}
		}
	case wire.LineJoin:
		if line.Self && len(line.Buffers) > 0 {
			n.joined[line.Buffers[0].Key] = line.Buffers[0].Name
		}
	case wire.LinePart:
		if line.Self && len(line.Buffers) > 0 {
			delete(n.joined, line.Buffers[0].Key)
		}
	case wire.LineKick:
		if len(line.Buffers) > 0 && n.isMe(lineParam(line.Raw, 1)) {
			delete(n.joined, line.Buffers[0].Key)
		}
	}
}

// lineParam returns param i of a raw IRC line (tags and prefix skipped; trailing included).
func lineParam(raw string, i int) string {
	msg, err := irc.Parse(raw)
	if err != nil {
		return ""
	}
	return msg.Param(i)
}
