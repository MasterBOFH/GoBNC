package retecloud

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Lite mode, ported from retecloud-api websocket/lite.go: a connection bootstraps a network with
// identity + a buffer list, then gets live lines only for buffers it subscribed to; the rest
// becomes per-buffer activity counts.

const (
	syncModeLite         = "lite"
	liteDefaultHistory   = 50
	liteMaxHistory       = 500
	liteMaxSubscriptions = 20
)

// liteActivityFlushDelay coalesces plain unread bumps; highlights and DMs flush immediately.
var liteActivityFlushDelay = 2 * time.Second

// liteSession is one connection's view of one followed network. Loop-owned.
type liteSession struct {
	network    int64
	asOfID     int64 // activity seeded up to this id; live lines <= it were counted
	stopped    bool  // bootstrapped while the brain ran no Session for the network
	subs       map[string]*liteSub
	activity   map[string]*liteActivity
	dirty      map[string]bool
	open       map[string]bool // the device's open buffers (keys)
	pending    []pendingReply  // queries this connection sent whose broadcast replies are due
	flushTimer *time.Timer
}

type liteSub struct {
	name         string
	fromSeq      uint64      // line events with seq <= this are already in the snapshot
	historyMaxID int64       // live lines with id <= this were already sent as history
	lastSent     int64       // newest message id sent to the connection for this buffer
	members      membersMode // member form for buffer_state refreshes
}

type liteActivity struct {
	name       string
	unread     int
	highlights int
	lastID     int64
}

func newLiteSession(network int64) *liteSession {
	return &liteSession{network: network, subs: map[string]*liteSub{}, activity: map[string]*liteActivity{},
		dirty: map[string]bool{}, open: map[string]bool{}}
}

func (ls *liteSession) stopTimer() {
	if ls.flushTimer != nil {
		ls.flushTimer.Stop()
		ls.flushTimer = nil
	}
}

// handleSyncSession is sync_session: only sync_mode "lite". It turns the network on for the
// device, attaches the connection's virtual client, and answers sync_start,
// sync_session_success (the bootstrap), sync_end, then each inline subscribe's
// subscribe_buffer_success.
func (m *Module) handleSyncSession(c *wsConn, msg *ClientMessage) {
	var req SyncSessionRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		c.send(errorFrame("sync_session", msg.RequestID, "Invalid request data"))
		return
	}
	if req.ClientID == "" {
		c.send(errorFrame("sync_session", msg.RequestID, "client_id is required"))
		return
	}
	n := m.networkFor(msg.SessionID)
	if n == nil {
		c.send(errorFrame("sync_session", msg.RequestID, "No access to session"))
		return
	}
	if mode := strings.ToLower(strings.TrimSpace(req.SyncMode)); mode != syncModeLite {
		c.send(errorFrame("sync_session", msg.RequestID, "Only lite mode is supported"))
		return
	}
	if !m.brainUp {
		c.send(ServerMessage{Type: "sync_session_retry", RequestID: msg.RequestID, SessionID: msg.SessionID,
			Data: map[string]interface{}{"retry": true, "retry_in_secs": 2}})
		return
	}
	snap, ls, data, err := m.bootstrap(c, n, req.ClientID, req.Cache)
	if err != nil {
		m.log.Warn("lite bootstrap", "network", n.id, "err", err)
		c.send(errorFrame("sync_session", msg.RequestID, "Bouncer is reloading"))
		return
	}
	if old := c.sessions[n.id]; old != nil {
		old.stopTimer()
	}
	c.sessions[n.id] = ls
	success := ServerMessage{Type: "sync_session_success", RequestID: msg.RequestID, SessionID: msg.SessionID, Data: data}
	size := 0
	if b, err := json.Marshal(success); err == nil {
		size = len(b)
	}
	c.send(ServerMessage{Type: "sync_start", RequestID: msg.RequestID, SessionID: msg.SessionID,
		Data: map[string]interface{}{"total_bytes": size, "total_messages": 0, "sync_mode": syncModeLite}})
	c.send(success)
	c.send(ServerMessage{Type: "sync_end", RequestID: msg.RequestID, SessionID: msg.SessionID})
	inline := &snap
	if ls.stopped { // no snapshot to start from: each subscribe looks for itself
		inline = nil
	}
	for _, sub := range req.Subscribe {
		m.requestSubscribe(c, n, msg.RequestID, sub, inline)
	}
}

// bootstrap prepares a lite session for the device on n and builds sync_session_success's data.
// A stopped network (disconnect_session, or never started) has no Session in the brain, so its
// network-scoped calls answer not_found: it bootstraps as disconnected, from the device's open
// buffers and the activity the brain counts from the store (history.activity and
// read_markers.get need no Session), and is followed again when the brain restarts it
// (onNetworkEvent).
func (m *Module) bootstrap(c *wsConn, n *network, deviceID string, cache map[string]string) (wire.Snapshot, *liteSession, map[string]interface{}, error) {
	if err := m.call(wire.MethodDevicesSetNetwork, wire.DeviceSetNetworkParams{ClientID: deviceID, Network: n.id, Subscribed: true}, nil); err != nil {
		return wire.Snapshot{}, nil, nil, err
	}
	var snap wire.Snapshot
	var activity wire.HistoryActivityResult
	stopped, err := m.bootstrapLive(c, n, deviceID, &snap, &activity)
	if err != nil {
		return wire.Snapshot{}, nil, nil, err
	}
	if stopped {
		// A brain older than the store-read activity answers not_found here too: no counts.
		if err := m.call(wire.MethodHistoryActivity, wire.HistoryActivityParams{Network: n.id, Device: deviceID}, &activity); err != nil {
			if !isNotFound(err) {
				return wire.Snapshot{}, nil, nil, err
			}
			activity = wire.HistoryActivityResult{}
		}
	}
	var bufs wire.DeviceBuffersResult
	if err := m.call(wire.MethodDevicesBuffers, wire.DeviceNetworkParams{ClientID: deviceID, Network: n.id}, &bufs); err != nil {
		return wire.Snapshot{}, nil, nil, err
	}

	ls := newLiteSession(n.id)
	ls.asOfID, ls.stopped = snap.LastID, stopped
	if stopped {
		// No snapshot bounds a stopped network's counts: they run to the newest stored line,
		// and nothing newer is stored until the network restarts.
		for _, a := range activity.Buffers {
			ls.asOfID = max(ls.asOfID, a.LastID)
		}
	}
	for _, b := range bufs.Buffers {
		if b.Open {
			ls.open[b.Key] = true
		}
	}
	connected := !stopped && (n.state == wire.NetRegistered || n.state == wire.NetConnecting)
	out := map[string]interface{}{
		"client_id":  deviceID,
		"connected":  connected,
		"registered": !stopped && n.state == wire.NetRegistered,
	}
	order := []string{}
	byKey := map[string]*LiteBuffer{}
	add := func(name string) *LiteBuffer {
		key := n.key(name)
		if b, ok := byKey[key]; ok {
			return b
		}
		b := &LiteBuffer{Name: name, Kind: n.bufferKind(name)}
		byKey[key] = b
		order = append(order, key)
		return b
	}
	if connected {
		caps := snap.Caps
		if caps == nil {
			caps = []string{}
		}
		putSection(out, cache, "capabilities", caps)
		putSection(out, cache, "isupport", snap.ISupport)
		putSection(out, cache, "identity", identityMap(snap.Self))
		for _, ch := range snap.Channels {
			add(ch.Name).Joined = true
		}
	}
	var renamed []wire.DeviceBuffer
	for _, b := range bufs.Buffers {
		if b.Open {
			add(b.Buffer).RenamedFrom = b.RenamedFrom
		}
		if b.RenamedFrom != "" {
			renamed = append(renamed, b)
		}
	}
	if len(renamed) > 0 {
		// Reported once: this bootstrap clears every rename it read, a closed row's unreported.
		_ = m.call(wire.MethodDevicesClearRenames, wire.DeviceClearRenamesParams{ClientID: deviceID, Network: n.id, Buffers: renamed}, nil)
	}
	for _, a := range activity.Buffers {
		if a.Unread <= 0 && a.Highlights <= 0 {
			continue
		}
		b := add(a.Buffer)
		b.Unread, b.Highlights = a.Unread, a.Highlights
		ls.activity[a.Key] = &liteActivity{name: a.Buffer, unread: a.Unread, highlights: a.Highlights, lastID: ls.asOfID}
	}
	for _, key := range order {
		var mark wire.ReadMarkerGetResult
		if err := m.call(wire.MethodReadMarkersGet, wire.ReadMarkerGetParams{Network: n.id, Buffer: byKey[key].Name}, &mark); err == nil {
			byKey[key].ReadID = mark.ID
		}
	}
	out["as_of_id"] = ls.asOfID
	buffers := make([]*LiteBuffer, 0, len(order))
	for _, key := range order {
		buffers = append(buffers, byKey[key])
	}
	out["buffers"] = buffers
	return snap, ls, out, nil
}

// bootstrapLive makes the bootstrap's calls on the network's Session: seed the device's open
// buffers, attach the virtual client, count activity, take the snapshot. stopped reports a
// network the brain runs no Session for (not_found); any other failure, not_ready included, is
// an error.
func (m *Module) bootstrapLive(c *wsConn, n *network, deviceID string, snap *wire.Snapshot, activity *wire.HistoryActivityResult) (stopped bool, err error) {
	calls := []func() error{
		func() error {
			return m.call(wire.MethodDevicesSeed, wire.DeviceNetworkParams{ClientID: deviceID, Network: n.id}, nil)
		},
		func() error {
			return m.call(wire.MethodClientsAttach, wire.ClientParams{Network: n.id, Client: c.clientName}, nil)
		},
		func() error {
			return m.call(wire.MethodHistoryActivity, wire.HistoryActivityParams{Network: n.id, Device: deviceID}, activity)
		},
		func() (err error) { *snap, err = m.snapshot(n.id); return err },
	}
	for _, call := range calls {
		if err := call(); err != nil {
			if isNotFound(err) {
				*snap, *activity = wire.Snapshot{}, wire.HistoryActivityResult{}
				return true, nil
			}
			return false, err
		}
	}
	return false, nil
}

// isNotFound reports a brain call answered not_found (for a network: no Session runs it).
func isNotFound(err error) bool {
	var we *wire.Error
	return errors.As(err, &we) && we.Code == wire.CodeNotFound
}

// identityMap is the bootstrap's identity section.
func identityMap(self wire.Self) map[string]interface{} {
	return map[string]interface{}{
		"nick": self.Nick, "username": self.User, "hostname": self.Host, "realname": "",
		"modes": self.UModes, "account": self.Account, "away": self.Away, "awaymessage": "",
		"secure": false, "bot": false, "oper": false,
	}
}

// sectionHash identifies a bootstrap section's content (json.Marshal sorts map keys).
func sectionHash(v interface{}) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// putSection adds name and name_hash, leaving the value out when the client's cached hash
// still matches.
func putSection(out map[string]interface{}, cache map[string]string, name string, v interface{}) {
	h := sectionHash(v)
	out[name+"_hash"] = h
	if cache[name] != h {
		out[name] = v
	}
}

// bufferKind is a buffer list entry's kind.
func (n *network) bufferKind(name string) string {
	switch {
	case name == wire.ServerBuffer:
		return "server"
	case n.isChannel(name):
		return "channel"
	}
	return "query"
}

// handleUnsyncSession turns a network off for the device: the toggle is stored false, the
// connection's virtual client goes, and nothing for that session reaches the connection until
// it syncs again.
func (m *Module) handleUnsyncSession(c *wsConn, msg *ClientMessage) {
	var req UnsyncSessionRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		c.send(errorFrame("unsync_session", msg.RequestID, "Invalid request data"))
		return
	}
	if msg.SessionID == "" {
		c.send(errorFrame("unsync_session", msg.RequestID, "session_id is required"))
		return
	}
	if req.ClientID == "" {
		c.send(errorFrame("unsync_session", msg.RequestID, "client_id is required"))
		return
	}
	n := m.networkFor(msg.SessionID)
	if n == nil {
		c.send(errorFrame("unsync_session", msg.RequestID, "No access to session"))
		return
	}
	if err := m.call(wire.MethodDevicesSetNetwork, wire.DeviceSetNetworkParams{ClientID: req.ClientID, Network: n.id, Subscribed: false}, nil); err != nil {
		c.send(errorFrame("unsync_session", msg.RequestID, "Bouncer is reloading"))
		return
	}
	if ls := c.sessions[n.id]; ls != nil {
		ls.stopTimer()
		delete(c.sessions, n.id)
		_ = m.call(wire.MethodClientsDetach, wire.ClientParams{Network: n.id, Client: c.clientName}, nil)
	}
	c.send(ServerMessage{Type: "unsync_session_success", RequestID: msg.RequestID, SessionID: msg.SessionID,
		Data: map[string]interface{}{}})
}

// networkFor resolves a session_id to a known network.
func (m *Module) networkFor(sid string) *network {
	id, ok := parseSessionID(sid)
	if !ok {
		return nil
	}
	return m.nets[id]
}
