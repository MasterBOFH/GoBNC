package retecloud

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// handleEvent applies one brain event, in stream order.
func (m *Module) handleEvent(ev wire.Event) {
	switch ev.Event {
	case wire.EventReload:
		m.onReloadEvent(ev.Data)
	case wire.EventReady:
		// A brain that was still booting at hello answered not_ready; follow again now.
		m.brainConnected()
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
	case wire.EventClientLine:
		var cl wire.ClientLineEvent
		if json.Unmarshal(ev.Data, &cl) == nil {
			m.onClientLine(cl)
		}
	case wire.EventReadMarker:
		var rm wire.ReadMarkerEvent
		if json.Unmarshal(ev.Data, &rm) == nil {
			m.onReadMarker(rm)
		}
	case wire.EventBufferOpen:
		var bo wire.BufferOpenEvent
		if json.Unmarshal(ev.Data, &bo) == nil {
			m.onBufferOpen(bo)
		}
	case wire.EventBufferClose:
		var bc wire.BufferCloseEvent
		if json.Unmarshal(ev.Data, &bc) == nil {
			m.onBufferClose(bc)
		}
	case wire.EventBufferRename:
		var br wire.BufferRenameEvent
		if json.Unmarshal(ev.Data, &br) == nil {
			m.onBufferRename(br)
		}
	case wire.EventDeviceNetwork:
		var dn wire.DeviceNetworkEvent
		if json.Unmarshal(ev.Data, &dn) == nil {
			m.onDeviceNetwork(dn)
		}
	case wire.EventState:
		var st wire.StateEvent
		if json.Unmarshal(ev.Data, &st) == nil {
			switch st.Kind {
			case wire.StateMembers:
				if n := m.nets[st.Network]; n != nil && n.members != nil {
					n.setMembers(st.Key, st.Members)
				}
				m.refreshBufferState(st.Network, st.Buffer, true)
			case wire.StateSilence:
				m.broadcastSilence(st.Network, st.Silence)
			case wire.StateChannel: // modes/topic became known or changed by a reply
				m.refreshBufferState(st.Network, st.Buffer, false)
			}
		}
	}
}

// onLine routes one line to every connection following its network (lite routing tiers:
// self/status traffic always, subscribed buffers' lines, activity counts for the rest), then
// keeps the network's own state (our nick, joined channels) in step.
func (m *Module) onLine(line wire.LineEvent) {
	n := m.nets[line.Network]
	if n == nil || line.Seq <= n.lastSeq {
		return
	}
	n.lastSeq = line.Seq
	r := n.route(line)
	frame := ircLineFrame(line)
	var anyPending *bool // computed once, only for unsolicited error numerics
	for c := range m.conns {
		ls := c.sessions[n.id]
		if ls == nil {
			continue
		}
		if r.numeric >= 400 && r.numeric < 600 && anyPending == nil && !ls.hasPending() {
			v := m.anyPendingExcept(n.id, c)
			anyPending = &v
		}
		elsewhere := anyPending != nil && *anyPending
		if m.liteAllows(c, n, ls, line, r, elsewhere) {
			c.send(frame)
		}
	}
	m.pushLine(n, line)
	n.trackSelf(line)
	n.trackMembers(line)
	if (line.Kind == wire.LinePart && line.Self) || (line.Kind == wire.LineKick && r.always) {
		if len(line.Buffers) > 0 {
			m.refreshBufferState(n.id, line.Buffers[0].Name, false)
		}
	}
}

// lineRoute is retecloud-api's LineRoute for one line, from the brain's envelope.
type lineRoute struct {
	always               bool   // self / status traffic every lite connection gets
	activity             bool   // a stored message or notice from someone else: counts as unread
	numeric              int    // numeric replies (0 otherwise)
	renameFrom, renameTo string // a NICK: a query subscription follows the nick
}

func (n *network) route(line wire.LineEvent) lineRoute {
	var r lineRoute
	inServer := false
	for _, b := range line.Buffers {
		if b.Name == wire.ServerBuffer {
			inServer = true
		}
	}
	switch line.Kind {
	case wire.LineNumeric:
		r.numeric, _ = strconv.Atoi(lineCommand(line.Raw))
		r.always = len(line.Buffers) == 0 || inServer
	case wire.LineNick:
		if line.Self {
			r.always = true
		} else {
			r.renameFrom, r.renameTo = line.Nick, lineParam(line.Raw, 0)
		}
	case wire.LineJoin, wire.LinePart:
		r.always = line.Self
	case wire.LineKick:
		r.always = n.isMe(lineParam(line.Raw, 1)) || line.Self
	case wire.LineMode:
		r.always = len(line.Buffers) == 0 // user modes are ours
	case wire.LineInvite, wire.LineError, wire.LineOther:
		r.always = true
	case wire.LineAway, wire.LineAccount, wire.LineChghost, wire.LineSetname:
		r.always = line.Self
	case wire.LineMessage, wire.LineNotice, wire.LineTagmsg, wire.LineCTCP, wire.LineCTCPReply:
		r.always = inServer
		stored := len(line.Buffers) > 0 && line.Buffers[0].ID > 0
		if (line.Kind == wire.LineMessage || line.Kind == wire.LineNotice) && stored && !line.Self && !inServer {
			r.activity = true
		}
	default:
		r.always = len(line.Buffers) == 0
	}
	return r
}

// liteAllows decides whether a lite connection gets a line, as retecloud-api's liteAllows; an
// unsubscribed buffer's message instead bumps its activity count.
func (m *Module) liteAllows(c *wsConn, n *network, ls *liteSession, line wire.LineEvent, r lineRoute, pendingElsewhere bool) bool {
	if r.numeric > 0 {
		if allow, handled := ls.replyAllowed(r.numeric, pendingElsewhere); handled {
			return allow
		}
	}
	if r.renameFrom != "" {
		if sub, ok := ls.subs[n.key(r.renameFrom)]; ok {
			deliver := line.Seq > sub.fromSeq
			delete(ls.subs, n.key(r.renameFrom))
			sub.name = r.renameTo
			ls.subs[n.key(r.renameTo)] = sub
			if deliver {
				return true
			}
		}
	}
	if r.always {
		return true
	}
	for _, b := range line.Buffers {
		sub := ls.subs[b.Key]
		if sub == nil {
			continue
		}
		if line.Seq <= sub.fromSeq || (b.ID > 0 && b.ID <= sub.historyMaxID) {
			return false // already in the subscribe's snapshot or history
		}
		sub.lastSent = max(sub.lastSent, b.ID)
		return true
	}
	if r.activity && len(line.Buffers) > 0 {
		m.countActivity(c, n, ls, line)
	}
	return false
}

// countActivity bumps the unread (and highlight) count of a line's buffer for a connection
// that does not show its lines, flushing at once for a highlight or a DM.
func (m *Module) countActivity(c *wsConn, n *network, ls *liteSession, line wire.LineEvent) {
	b := line.Buffers[0]
	if b.ID <= ls.asOfID || !ls.shows(n, b.Key) {
		return
	}
	a := ls.activity[b.Key]
	if a == nil {
		a = &liteActivity{name: b.Name}
		ls.activity[b.Key] = a
	}
	a.unread++
	// A server-sourced line (a server notice to a channel) counts but never highlights; the
	// synced switch and rules decide the rest.
	hl := n.isHighlight(line.Source, line.Raw, b.Name, b.Key)
	if hl {
		a.highlights++
	}
	a.lastID = max(a.lastID, b.ID)
	ls.dirty[b.Key] = true
	if hl {
		m.flushActivity(c, ls) // highlights and DMs are what a badge is for: now
		return
	}
	m.scheduleFlush(c, ls)
}

// shows reports whether the device's sidebar has the buffer: Server, a joined channel, or open.
func (ls *liteSession) shows(n *network, key string) bool {
	if key == n.key(wire.ServerBuffer) {
		return true
	}
	if _, joined := n.joined[key]; joined {
		return true
	}
	return ls.open[key]
}

// scheduleFlush arms the coalescing timer; it fires on the loop.
func (m *Module) scheduleFlush(c *wsConn, ls *liteSession) {
	if ls.flushTimer != nil {
		return
	}
	ls.flushTimer = time.AfterFunc(liteActivityFlushDelay, func() {
		m.post(func() {
			if c.sessions[ls.network] == ls {
				m.flushActivity(c, ls)
			}
		})
	})
}

// anyPendingExcept reports whether another connection following network awaits a reply.
func (m *Module) anyPendingExcept(network int64, except *wsConn) bool {
	for c := range m.conns {
		if c == except {
			continue
		}
		if ls := c.sessions[network]; ls != nil && ls.hasPending() {
			return true
		}
	}
	return false
}

// onClientLine delivers what the brain sent one connection's virtual client (a solicited reply,
// an ACK, a bouncer notice) to that connection only.
func (m *Module) onClientLine(cl wire.ClientLineEvent) {
	for c := range m.conns {
		if c.clientName == cl.Client && c.sessions[cl.Network] != nil {
			c.send(ServerMessage{Type: frameIRCLine, SessionID: sessionID(cl.Network),
				Data: map[string]interface{}{"silent": false, "timestamp": time.Now().UTC(), "raw": cl.Raw}})
			return
		}
	}
}

// onReadMarker is a read cursor moving (from any device or IRC client): read_cursor to every
// follower but the one that set it, and badges it covers are cleared.
func (m *Module) onReadMarker(rm wire.ReadMarkerEvent) {
	n := m.nets[rm.Network]
	if n == nil {
		return
	}
	origin, device := m.takeOwnAction("read_cursor", rm.Key)
	m.onReadMove(rm, device)
	msg := ServerMessage{Type: "read_cursor", SessionID: sessionID(rm.Network),
		Data: map[string]interface{}{"buffer": rm.Key, "last_message_id": rm.ID, "client_id": device}}
	for c := range m.conns {
		ls := c.sessions[rm.Network]
		if ls == nil {
			continue
		}
		if a := ls.activity[rm.Key]; a != nil && rm.ID >= a.lastID && (a.unread > 0 || a.highlights > 0) {
			a.unread, a.highlights = 0, 0
			if _, subscribed := ls.subs[rm.Key]; !subscribed {
				ls.dirty[rm.Key] = true
				m.scheduleFlush(c, ls)
			}
		}
		if c != origin {
			c.send(msg)
		}
	}
}

// onBufferOpen is a buffer opening on a device: the device's connections update their open set
// and hear open_buffer (reason "message" when a new line reopened it).
func (m *Module) onBufferOpen(bo wire.BufferOpenEvent) {
	origin, _ := m.takeOwnAction("open_buffer", bo.Key)
	data := map[string]interface{}{"buffer": bo.Buffer, "client_id": bo.ClientID}
	if bo.Reason == wire.OpenMessage {
		data = map[string]interface{}{"buffer": bo.Buffer, "client_id": "", "reason": "message"}
	}
	for c := range m.conns {
		ls := c.sessions[bo.Network]
		if ls == nil || c.deviceID != bo.ClientID {
			continue
		}
		ls.open[bo.Key] = true
		if c != origin && bo.Reason != wire.OpenSeed {
			c.send(ServerMessage{Type: "open_buffer", SessionID: sessionID(bo.Network), Data: data})
		}
	}
}

// onBufferClose is an intentional close on a device: its connections drop the buffer and the
// badge up to the close.
func (m *Module) onBufferClose(bc wire.BufferCloseEvent) {
	origin, _ := m.takeOwnAction("close_buffer", bc.Key)
	for c := range m.conns {
		ls := c.sessions[bc.Network]
		if ls == nil || c.deviceID != bc.ClientID {
			continue
		}
		delete(ls.open, bc.Key)
		if a := ls.activity[bc.Key]; a != nil && a.lastID <= bc.ClosedAtID {
			delete(ls.activity, bc.Key)
		}
		if c != origin {
			c.send(ServerMessage{Type: "close_buffer", SessionID: sessionID(bc.Network),
				Data: map[string]interface{}{"buffer": bc.Buffer, "client_id": bc.ClientID}})
		}
	}
}

// onBufferRename is a query peer's NICK renaming the device's buffer row: its connections move
// the open set to the new name and hear rename_buffer, ahead of the NICK line. (The client keeps
// the buffer's unread count across the rename; activity counts stay per name, as the store's.)
func (m *Module) onBufferRename(br wire.BufferRenameEvent) {
	for c := range m.conns {
		ls := c.sessions[br.Network]
		if ls == nil || c.deviceID != br.ClientID {
			continue
		}
		if ls.open[br.FromKey] {
			delete(ls.open, br.FromKey)
			ls.open[br.ToKey] = true
		}
		c.send(ServerMessage{Type: "rename_buffer", SessionID: sessionID(br.Network),
			Data: map[string]interface{}{"from": br.From, "to": br.To}})
	}
}

// noteOwnAction marks the connection behind the next matching brain event.
func (m *Module) noteOwnAction(c *wsConn, event, key string) {
	m.ownAction = ownAction{conn: c, event: event, device: c.deviceID, key: key}
}

func (m *Module) takeOwnAction(event, key string) (*wsConn, string) {
	a := m.ownAction
	if a.event == event && a.key == key {
		m.ownAction = ownAction{}
		return a.conn, a.device
	}
	return nil, ""
}

// refreshBufferState sends a fresh buffer_state to the connections subscribed to buffer (after
// a NAMES completes, modes or topic become known, or our own PART/KICK). withMembers=false
// sends topic/modes only.
// broadcastSilence sends every connection following the network its whole SILENCE list, tier 0
// (the brain emits the state event ahead of the SILENCE line that changed it).
func (m *Module) broadcastSilence(network int64, masks []string) {
	if masks == nil {
		masks = []string{}
	}
	msg := ServerMessage{Type: frameSilence, SessionID: sessionID(network), Data: map[string]interface{}{"masks": masks}}
	for c := range m.conns {
		if c.sessions[network] != nil {
			c.send(msg)
		}
	}
}

func (m *Module) refreshBufferState(network int64, buffer string, withMembers bool) {
	n := m.nets[network]
	if n == nil {
		return
	}
	key := n.key(buffer)
	var subscribers []*wsConn
	for c := range m.conns {
		if ls := c.sessions[network]; ls != nil && ls.subs[key] != nil {
			subscribers = append(subscribers, c)
		}
	}
	if len(subscribers) == 0 {
		return
	}
	snap, err := m.snapshot(network)
	if err != nil {
		return
	}
	for _, c := range subscribers {
		ls := c.sessions[network]
		if ls == nil || ls.subs[key] == nil {
			continue
		}
		mode := membersNone
		if withMembers {
			mode = ls.subs[key].members
		}
		c.send(ServerMessage{Type: frameBufferState, SessionID: sessionID(network), Data: bufferState(n, snap, buffer, mode)})
	}
}

// lineTarget is one entry of irc_line's targets: a buffer the line was stored under and the id
// of that buffer's copy.
type lineTarget struct {
	Buffer string `json:"buffer"`
	ID     int64  `json:"id"`
}

// ircLineFrame is a line as retecloud-api's irc_line: raw, timestamp, and the message id (and
// buffer) when it was stored. A line stored under several buffers (a QUIT or NICK, once per
// shared channel) also carries targets, each stored buffer with its own id; buffer/id are the
// first of them.
func ircLineFrame(line wire.LineEvent) ServerMessage {
	ts, err := time.Parse(time.RFC3339Nano, line.Time)
	if err != nil {
		ts = time.Now().UTC()
	}
	data := map[string]interface{}{"silent": false, "timestamp": ts, "raw": line.Raw}
	var targets []lineTarget
	for _, b := range line.Buffers {
		if b.ID > 0 {
			targets = append(targets, lineTarget{Buffer: b.Name, ID: b.ID})
		}
	}
	if len(targets) > 0 {
		data["id"] = targets[0].ID
		data["buffer"] = targets[0].Buffer
	}
	if len(targets) > 1 {
		data["targets"] = targets
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

func lineCommand(raw string) string {
	msg, err := irc.Parse(raw)
	if err != nil {
		return ""
	}
	return msg.Command
}
