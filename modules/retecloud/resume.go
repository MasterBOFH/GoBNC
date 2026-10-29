package retecloud

import (
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// A brain reload drops only the module's brain connection: WebSockets stay open (sends answer
// "Bouncer is reloading" meanwhile). resume runs after every brain hello and brings each
// followed network back: a fresh subscription and snapshot, the connections' virtual clients
// re-attached, every subscribed buffer's missed lines sent from history as irc_line (newer than
// the last id that WebSocket got) before live lines continue, and badges recounted.
func (m *Module) resume() {
	followed := map[int64]bool{}
	for c := range m.conns {
		for id := range c.sessions {
			followed[id] = true
		}
	}
	for id := range followed {
		n := m.nets[id]
		if n == nil {
			m.dropNetwork(id)
			continue
		}
		snap, err := m.snapshot(id)
		if err != nil {
			m.log.Warn("resume: subscribe", "network", id, "err", err)
			continue
		}
		for c := range m.conns {
			if ls := c.sessions[id]; ls != nil {
				m.resumeConn(c, n, ls, snap)
			}
		}
	}
}

func (m *Module) resumeConn(c *wsConn, n *network, ls *liteSession, snap wire.Snapshot) {
	if err := m.call(wire.MethodClientsAttach, wire.ClientParams{Network: n.id, Client: c.clientName}, nil); err != nil {
		m.log.Warn("resume: clients.attach", "network", n.id, "err", err)
	}
	for _, sub := range ls.subs {
		after := sub.lastSent
		for {
			var res wire.HistoryMessagesResult
			err := m.call(wire.MethodHistoryMessages, wire.HistoryMessagesParams{Network: n.id, Buffer: sub.name,
				AfterID: &after, Limit: liteMaxHistory}, &res)
			if err != nil {
				m.log.Warn("resume: history", "network", n.id, "buffer", sub.name, "err", err)
				break
			}
			for _, hm := range res.Messages {
				c.send(gapLineFrame(n.id, hm))
				after = hm.ID
			}
			if !res.HasAfter || len(res.Messages) == 0 {
				break
			}
		}
		sub.lastSent = max(sub.lastSent, after)
		sub.historyMaxID = max(sub.historyMaxID, after)
		sub.fromSeq = snap.Seq
	}
	var activity wire.HistoryActivityResult
	if err := m.call(wire.MethodHistoryActivity, wire.HistoryActivityParams{Network: n.id, Device: c.deviceID}, &activity); err == nil {
		ls.asOfID = snap.LastID
		for _, a := range activity.Buffers {
			if _, subscribed := ls.subs[a.Key]; subscribed {
				continue
			}
			cur := ls.activity[a.Key]
			if cur == nil && a.Unread == 0 && a.Highlights == 0 {
				continue
			}
			if cur == nil || cur.unread != a.Unread || cur.highlights != a.Highlights {
				ls.activity[a.Key] = &liteActivity{name: a.Buffer, unread: a.Unread, highlights: a.Highlights, lastID: a.LastID}
				ls.dirty[a.Key] = true
			}
		}
		m.flushActivity(c, ls)
	}
}

// gapLineFrame is a history line sent as the live irc_line it would have been.
func gapLineFrame(network int64, hm wire.HistoryMessage) ServerMessage {
	ts, err := time.Parse(time.RFC3339Nano, hm.Time)
	if err != nil {
		ts = time.Now().UTC()
	}
	return ServerMessage{Type: frameIRCLine, SessionID: sessionID(network), Data: map[string]interface{}{
		"silent": false, "timestamp": ts, "raw": hm.Raw, "id": hm.ID, "buffer": hm.Buffer}}
}

// dropNetwork forgets a network the brain no longer has.
func (m *Module) dropNetwork(id int64) {
	for c := range m.conns {
		if ls := c.sessions[id]; ls != nil {
			ls.stopTimer()
			delete(c.sessions, id)
		}
	}
}
