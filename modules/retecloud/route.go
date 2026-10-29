package retecloud

import (
	"encoding/json"

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

// onLine keeps the network's own state (our nick, joined channels) in step with the stream.
func (m *Module) onLine(line wire.LineEvent) {
	n := m.nets[line.Network]
	if n == nil || line.Seq <= n.lastSeq {
		return
	}
	n.lastSeq = line.Seq
	n.trackSelf(line)
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
