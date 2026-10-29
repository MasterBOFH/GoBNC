package retecloud

import (
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// onNetworkEvent keeps the module's network list current and tells clients: session_created /
// session_deleted to every authenticated connection, and a state frame to the connections
// following the network when its connection state changes.
func (m *Module) onNetworkEvent(ev wire.NetworkEvent) {
	sid := sessionID(ev.Network)
	switch ev.Change {
	case wire.ChangeAdded:
		if m.nets[ev.Network] == nil {
			m.nets[ev.Network] = &network{id: ev.Network, joined: map[string]string{}}
		}
		n := m.nets[ev.Network]
		n.name, n.state, n.nick = ev.Name, ev.State, ev.Nick
		m.broadcastAuthed(ServerMessage{Type: "session_created", SessionID: sid,
			Data: map[string]interface{}{"name": ev.Name, "state": sessionState(ev.State)}})
		return
	case wire.ChangeRemoved:
		delete(m.nets, ev.Network)
		for c := range m.conns {
			if ls := c.sessions[ev.Network]; ls != nil {
				ls.stopTimer()
				delete(c.sessions, ev.Network)
			}
		}
		m.broadcastAuthed(ServerMessage{Type: "session_deleted", SessionID: sid})
		return
	}
	n := m.nets[ev.Network]
	if n == nil {
		return
	}
	n.name = ev.Name
	if ev.Change != wire.ChangeState || ev.State == n.state {
		return
	}
	n.state = ev.State
	if ev.Nick != "" {
		n.nick = ev.Nick
	}
	if ev.State != wire.NetRegistered {
		n.joined = map[string]string{} // a reconnect rejoins from scratch
	}
	data := map[string]interface{}{
		"connected":  ev.State == wire.NetRegistered || ev.State == wire.NetConnecting,
		"registered": ev.State == wire.NetRegistered,
		"timestamp":  time.Now().UTC().Format(time.RFC3339Nano),
	}
	m.broadcastFollowers(ev.Network, ServerMessage{Type: "state", SessionID: sid, Data: data})
}

func (m *Module) broadcastAuthed(msg ServerMessage) {
	for c := range m.conns {
		if c.authed {
			c.send(msg)
		}
	}
}

// broadcastFollowers sends msg to every connection that synced network.
func (m *Module) broadcastFollowers(network int64, msg ServerMessage) {
	for c := range m.conns {
		if c.sessions[network] != nil {
			c.send(msg)
		}
	}
}
