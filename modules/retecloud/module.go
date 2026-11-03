package retecloud

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Features are the module API features gobnc-retecloud uses (grant them in gobnc.json).
var Features = []string{wire.FeatureNetworks, wire.FeatureEvents, wire.FeatureHistory, wire.FeatureReadMarkers,
	wire.FeatureClients, wire.FeatureSend, wire.FeatureDevices, wire.FeatureAuth, wire.FeatureStorage}

// Brain is what the module needs from its brain connection (*moduleapi.Client).
type Brain interface {
	Call(ctx context.Context, method string, params, result any) error
	Events() <-chan wire.Event
	Ready() bool
	Run(ctx context.Context, onConnect func(ctx context.Context, c *moduleapi.Client) error) error
}

// callTimeout bounds one brain call made from the loop.
const callTimeout = 10 * time.Second

// authTimeout is how long a WebSocket may stay open without authenticating.
var authTimeout = 30 * time.Second

// Module is gobnc-retecloud's core. One loop goroutine owns all of its state: WebSocket requests
// arrive as ops posted in order, brain events are consumed in order, and brain calls made from
// the loop keep reading events into a backlog while they wait (see call), so the loop is the
// only place frames for a WebSocket are queued and their order follows the brain's stream.
type Module struct {
	cfg   Config
	log   *slog.Logger
	brain Brain

	ctx     context.Context
	ops     chan func()
	backlog []wire.Event

	// Loop-owned.
	conns     map[*wsConn]bool
	nextConn  uint64
	nets      map[int64]*network
	brainUp   bool // between a brain hello and its loss
	ownAction ownAction
	reloading bool // a reloader is running
	handedOff bool // a replacement took over: no new WebSockets, Run returns ErrHandedOff

	reloader  Reloader // set before Run
	onServing func()   // set before Run; cleared once called
}

// ownAction remembers the connection that just opened or closed a buffer, so the brain's
// resulting buffer_open/buffer_close is not echoed back to it.
type ownAction struct {
	conn   *wsConn
	event  string
	device string
	key    string
}

// network is the module's view of one GoBNC network, from its latest snapshot plus the lines
// since.
type network struct {
	id        int64
	name      string
	state     string
	nick      string
	isupport  map[string]string
	caps      []string
	self      wire.Self
	cm        irc.CaseMapping
	chantypes string
	statusmsg string
	joined    map[string]string // key → display name
	lastSeq   uint64            // seq of the last line event processed
	followed  bool              // events.subscribe done on the current brain connection
}

// New builds the module over a brain client.
func New(cfg Config, brain Brain, log *slog.Logger) *Module {
	if log == nil {
		log = slog.Default()
	}
	return &Module{cfg: cfg, log: log, brain: brain, ops: make(chan func(), 1024),
		conns: map[*wsConn]bool{}, nets: map[int64]*network{}}
}

// Run serves until ctx ends: the brain connection (reconnecting across brain reloads) and the
// module loop.
func (m *Module) Run(ctx context.Context) error {
	m.ctx = ctx
	go func() {
		_ = m.brain.Run(ctx, func(context.Context, *moduleapi.Client) error {
			m.post(m.brainConnected)
			return nil
		})
	}()
	for {
		if len(m.backlog) > 0 {
			ev := m.backlog[0]
			m.backlog = m.backlog[1:]
			m.handleEvent(ev)
			continue
		}
		if m.handedOff {
			m.drain()
			return ErrHandedOff
		}
		select {
		case <-ctx.Done():
			for c := range m.conns {
				c.closeWith(closeGoingAway, "going away")
			}
			m.drain()
			return nil
		case op := <-m.ops:
			op()
		case ev := <-m.brain.Events():
			m.handleEvent(ev)
		}
	}
}

// post queues f for the loop; false once the module stops.
func (m *Module) post(f func()) bool {
	ctx := m.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case m.ops <- f:
		return true
	case <-ctx.Done():
		return false
	}
}

// call is a brain call from the loop. Events arriving meanwhile go to the backlog, so a full
// event channel can never stall the response.
func (m *Module) call(method string, params, result any) error {
	ctx, cancel := context.WithTimeout(m.ctx, callTimeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.brain.Call(ctx, method, params, result) }()
	for {
		select {
		case err := <-done:
			return err
		case ev := <-m.brain.Events():
			m.backlog = append(m.backlog, ev)
		}
	}
}

// catchUp processes, in order, every pending event that the brain emitted before a snapshot of
// network taken at seq: it stops before the first line of that network with a higher seq,
// leaving it and everything after it pending.
func (m *Module) catchUp(network int64, seq uint64) {
	for {
		select {
		case ev := <-m.brain.Events():
			m.backlog = append(m.backlog, ev)
			continue
		default:
		}
		break
	}
	for len(m.backlog) > 0 {
		ev := m.backlog[0]
		if ev.Event == wire.EventLine {
			var probe struct {
				Network int64  `json:"network"`
				Seq     uint64 `json:"seq"`
			}
			if json.Unmarshal(ev.Data, &probe) == nil && probe.Network == network && probe.Seq > seq {
				return
			}
		}
		m.backlog = m.backlog[1:]
		m.handleEvent(ev)
	}
}

// snapshot (re)subscribes to network and returns its fresh snapshot, placed in the stream:
// everything emitted before it has been processed when it returns.
func (m *Module) snapshot(id int64) (wire.Snapshot, error) {
	var res wire.SubscribeResult
	if err := m.call(wire.MethodEventsSubscribe, wire.SubscribeParams{Network: id}, &res); err != nil {
		return wire.Snapshot{}, err
	}
	m.catchUp(id, res.Snapshot.Seq)
	n := m.nets[id]
	if n == nil {
		n = &network{id: id}
		m.nets[id] = n
	}
	n.applySnapshot(res.Snapshot)
	n.followed = true
	return res.Snapshot, nil
}

func (n *network) applySnapshot(s wire.Snapshot) {
	n.state = s.State
	n.self = s.Self
	n.nick = s.Self.Nick
	n.isupport = s.ISupport
	n.caps = s.Caps
	n.cm = irc.ParseCaseMapping(s.ISupport["CASEMAPPING"])
	n.chantypes = s.ISupport["CHANTYPES"]
	if _, ok := s.ISupport["CHANTYPES"]; !ok {
		n.chantypes = "#&"
	}
	n.statusmsg = s.ISupport["STATUSMSG"]
	n.joined = map[string]string{}
	for _, ch := range s.Channels {
		n.joined[ch.Key] = ch.Name
	}
	n.lastSeq = s.Seq
}

// key folds a buffer name the way the network compares names.
func (n *network) key(name string) string { return n.cm.Canonical(strings.TrimSpace(name)) }

func (n *network) isChannel(name string) bool {
	return name != "" && strings.IndexByte(n.chantypes, name[0]) >= 0
}

func (n *network) isMe(nick string) bool {
	return nick != "" && n.nick != "" && n.cm.Equal(nick, n.nick)
}

// brainConnected runs after every brain hello (first connect and each reconnect).
func (m *Module) brainConnected() {
	m.brainUp = true
	var nets wire.NetworksListResult
	if err := m.call(wire.MethodNetworksList, wire.Empty{}, &nets); err != nil {
		m.log.Warn("networks.list", "err", err)
		return
	}
	seen := map[int64]bool{}
	for _, info := range nets.Networks {
		seen[info.ID] = true
		n := m.nets[info.ID]
		known := n != nil
		if !known {
			n = &network{id: info.ID, joined: map[string]string{}}
			m.nets[info.ID] = n
		}
		before := sessionState(n.state)
		// A new brain numbers its line events from scratch.
		n.name, n.state, n.followed, n.lastSeq = info.Name, info.State, false, 0
		if info.Nick != "" && n.nick == "" {
			n.nick = info.Nick
		}
		if known { // the state may have changed while the brain was away
			m.sessionStateChanged(n, before)
		}
	}
	for id := range m.nets {
		if !seen[id] {
			delete(m.nets, id)
		}
	}
	m.resume()
	m.serving()
}

// Accept serves one WebSocket (the Listener's callback) until it closes.
func (m *Module) Accept(ws *websocket.Conn, certSHA256, remoteIP string) {
	done := make(chan *wsConn, 1)
	if !m.post(func() {
		m.nextConn++
		c := newWSConn(m, m.nextConn, ws, certSHA256, remoteIP)
		m.conns[c] = true
		if m.handedOff { // accepted just before the listener stopped
			c.closeWith(closeServiceRestart, "service restart")
		}
		done <- c
	}) {
		ws.Close()
		return
	}
	c := <-done
	// A connection that never authenticates must not keep a slot.
	authTimer := time.AfterFunc(authTimeout, func() {
		m.post(func() {
			if !c.authed {
				c.closeWith(closeAuthTimeout, "authentication timeout")
			}
		})
	})
	defer authTimer.Stop()
	go c.writeLoop()
	c.readLoop()
	m.post(func() { m.dropConn(c) })
}

// dropConn forgets a closed WebSocket and its virtual clients.
func (m *Module) dropConn(c *wsConn) {
	if !m.conns[c] {
		return
	}
	delete(m.conns, c)
	for id, ls := range c.sessions {
		ls.stopTimer()
		if m.brainUp {
			_ = m.call(wire.MethodClientsDetach, wire.ClientParams{Network: id, Client: c.clientName}, nil)
		}
	}
	c.sessions = map[int64]*liteSession{}
}

// handle runs one client frame on the loop.
func (m *Module) handle(c *wsConn, msg *ClientMessage) {
	if !m.conns[c] {
		return
	}
	if msg.Type == "authenticate" {
		m.handleAuthenticate(c, msg)
		return
	}
	if !c.authed {
		c.send(errorFrame(msg.Type, msg.RequestID, "Not authenticated"))
		return
	}
	switch msg.Type {
	case "sync_session":
		m.handleSyncSession(c, msg)
	case "unsync_session":
		m.handleUnsyncSession(c, msg)
	case frameSubscribeBuffer:
		m.handleSubscribeBuffer(c, msg)
	case frameUnsubscribeBuffer:
		m.handleUnsubscribeBuffer(c, msg)
	case "get_messages":
		m.handleGetMessages(c, msg)
	case "send":
		m.handleSend(c, msg)
	case "read_cursor":
		m.handleReadCursor(c, msg)
	case "open_buffer":
		m.handleOpenBuffer(c, msg)
	case "close_buffer":
		m.handleCloseBuffer(c, msg)
	case "connect_session":
		m.handleConnectSession(c, msg)
	case "disconnect_session":
		m.handleDisconnectSession(c, msg)
	case "create_session":
		c.send(errorFrame("create_session", msg.RequestID, "Not supported"))
	default:
		c.send(errorFrame("unknown", msg.RequestID, "Unknown message type"))
	}
}

func (m *Module) compressionLevel() int {
	if m.cfg.CompressionLevel >= 1 && m.cfg.CompressionLevel <= 9 {
		return m.cfg.CompressionLevel
	}
	return 1
}
