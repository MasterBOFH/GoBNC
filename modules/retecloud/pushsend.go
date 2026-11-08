package retecloud

import (
	"context"
	"sort"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/modules/retecloud/push"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// The push trigger (WEBSOCKET_PROTOCOL_LITE.md, "When a device is pushed"), coalescing a flood
// (PUSH_ENCRYPTION.md, "Coalescing a flood"; push/coalesce.go) and the clears of notifications
// read elsewhere (PUSH_ENCRYPTION.md, "Clearing notifications read elsewhere").

var (
	// readingElsewhereWindow: a read cursor moved by another client this recently means someone
	// is reading the network.
	readingElsewhereWindow = 5 * time.Minute
	// clearWindow coalesces clears: at most one per (device, buffer) per window.
	clearWindow = 30 * time.Second
	// pushMaxAge: a line older than this is not pushed (the relay refuses it as stale).
	pushMaxAge = time.Hour
	// relayConcurrency bounds relay requests in flight.
	relayConcurrency = 16
)

// pushLine pushes one live stored line to every device it qualifies for.
func (m *Module) pushLine(n *network, line wire.LineEvent) {
	if len(m.push.devices) == 0 || (line.Kind != wire.LineMessage && line.Kind != wire.LineNotice) || line.Self {
		return
	}
	if len(line.Buffers) == 0 || line.Buffers[0].ID <= 0 || session.ServerSource(line.Source) {
		return
	}
	b := line.Buffers[0]
	if b.Name == wire.ServerBuffer || n.isMe(line.Nick) || !n.isHighlight(line.Source, line.Raw, b.Name, b.Key) {
		return
	}
	now := m.now()
	at, err := time.Parse(time.RFC3339Nano, line.Time)
	if err != nil {
		at = now
	}
	if now.Sub(at) > pushMaxAge {
		return
	}
	msg, err := irc.Parse(line.Raw)
	if err != nil {
		return
	}
	text, action := msg.Param(1), false
	if t, ok := ctcpAction(text); ok {
		text, action = t, true
	}
	kind := push.KindHighlight
	if !n.isChannel(b.Name) {
		kind = push.KindDM
	}
	ids := make([]string, 0, len(m.push.devices))
	for id := range m.push.devices {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		d := m.push.devices[id]
		if !d.active() || !d.networks[n.id] || m.deviceForeground(id) || m.readingElsewhere(n.id, id) ||
			m.relayPausedNow(d.reg.RelayURL) {
			continue
		}
		decision := m.push.coalesce.Decide(push.Conversation{ClientID: id, SessionID: sessionID(n.id), Buffer: b.Key,
			Sender: n.key(line.Nick)}, now)
		if !decision.Send {
			continue // over the buffer's cap: the conversation's next push reports it
		}
		eid := push.NewEventID()
		pt, err := push.MessagePlaintext(push.Message{EID: eid, Src: push.SourceGoBNC, Profile: d.reg.Profile,
			SessionID: sessionID(n.id), Network: n.name, Buffer: b.Name, Kind: kind, Sender: line.Nick, Text: text,
			Action: action, Time: at.Unix(), ID: b.ID, Skipped: decision.Skipped})
		if err != nil {
			m.log.Warn("push: content", "network", n.id, "id", b.ID, "err", err)
			continue
		}
		blob, err := push.Seal(d.pub, pt)
		if err != nil {
			m.log.Warn("push: seal", "client_id", id, "err", err)
			continue
		}
		m.notePushed(id, n.id, b.Key, b.ID)
		req := push.NewRelayRequest(push.TypeMessage, eid, at.Unix(), blob)
		req.CollapseID, req.Silent = decision.CollapseID, decision.Silent
		m.relaySend(id, d.reg, req)
	}
}

// readingElsewhere reports another client moving a read cursor on network within the window.
func (m *Module) readingElsewhere(network int64, device string) bool {
	now := m.now()
	for origin, at := range m.push.readMoves[network] {
		if origin != device && now.Sub(at) < readingElsewhereWindow {
			return true
		}
	}
	return false
}

func (m *Module) relayPausedNow(relayURL string) bool {
	until, ok := m.push.relayPaused[relayURL]
	return ok && m.now().Before(until)
}

// relaySend posts one push off the loop; the answer comes back to the loop.
func (m *Module) relaySend(device string, reg pushRegistration, req push.RelayRequest) {
	if m.relaySem == nil {
		m.relaySem = make(chan struct{}, relayConcurrency)
	}
	relay, sem, ctx := m.relay, m.relaySem, m.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		res := relay.Send(ctx, reg.RelayURL, reg.RelayKey, req)
		<-sem
		m.post(func() { m.relayAnswered(device, reg, req, res) })
	}()
}

// relayAnswered acts on a relay answer: a refused key marks the registration rejected (stored,
// so push_register with the same key is refused), a 503 pauses that relay for an hour.
func (m *Module) relayAnswered(device string, reg pushRegistration, req push.RelayRequest, res push.RelayResult) {
	switch res.Outcome {
	case push.Sent:
		return
	case push.Rejected:
		m.log.Info("push: relay refused the key", "client_id", device, "status", res.Status, "error", res.Code)
		d := m.push.devices[device]
		if d == nil || d.reg.RelayKey != reg.RelayKey || d.reg.Rejected {
			return
		}
		d.reg.Rejected = true
		stored, found, err := m.readPushRegistration(device)
		if err == nil && found && stored.RelayKey == reg.RelayKey {
			stored.Rejected = true
			err = m.writePushRegistration(device, stored)
		}
		if err != nil {
			m.log.Warn("push: store rejected key", "client_id", device, "err", err)
		}
	case push.Unavailable:
		m.log.Warn("push: relay unavailable; pausing", "relay", reg.RelayURL)
		m.push.relayPaused[reg.RelayURL] = m.now().Add(relayUnavailablePause)
	default:
		m.log.Warn("push: not delivered", "client_id", device, "type", req.Type, "event_id", req.EventID,
			"outcome", res.Outcome.String(), "status", res.Status, "error", res.Code, "err", res.Err)
	}
}

// --- clears ---

type clearKey struct {
	device  string
	network int64
	key     string // folded buffer
}

// clearState is what one device was pushed in one buffer, and its clears.
type clearState struct {
	pushed   []int64 // ids pushed above cleared, ascending
	cleared  int64   // the last read_id cleared (or moved by the device itself)
	lastSent time.Time
	pending  int64 // a read_id waiting for the window to end
	pendAt   time.Time
	timer    *time.Timer
}

func (st *clearState) stop() {
	if st.timer != nil {
		st.timer.Stop()
		st.timer = nil
	}
}

// coversPushed reports a pushed id in (cleared, readID].
func (st *clearState) coversPushed(readID int64) bool {
	for _, id := range st.pushed {
		if id > st.cleared && id <= readID {
			return true
		}
	}
	return false
}

func (st *clearState) markCleared(readID int64) {
	st.cleared = max(st.cleared, readID)
	kept := st.pushed[:0]
	for _, id := range st.pushed {
		if id > st.cleared {
			kept = append(kept, id)
		}
	}
	st.pushed = kept
	if st.pending <= st.cleared {
		st.pending = 0
	}
}

func (m *Module) notePushed(device string, network int64, key string, id int64) {
	k := clearKey{device, network, key}
	st := m.push.clears[k]
	if st == nil {
		st = &clearState{}
		m.push.clears[k] = st
	}
	st.pushed = append(st.pushed, id)
}

// onReadMove records a read cursor moving (for the reading-elsewhere rule) and clears the
// notifications it covers on the other devices pushed in that buffer.
func (m *Module) onReadMove(rm wire.ReadMarkerEvent, origin string) {
	now := m.now()
	moves := m.push.readMoves[rm.Network]
	if moves == nil {
		moves = map[string]time.Time{}
		m.push.readMoves[rm.Network] = moves
	}
	moves[origin] = now
	at, err := time.Parse(time.RFC3339Nano, rm.Time)
	if err != nil {
		at = now
	}
	for k, st := range m.push.clears {
		if k.network != rm.Network || k.key != rm.Key {
			continue
		}
		if k.device == origin { // read on the phone itself: cleared there, no push
			st.markCleared(rm.ID)
			if st.pending == 0 {
				st.stop()
			}
			continue
		}
		if !st.coversPushed(max(rm.ID, st.pending)) {
			continue
		}
		if st.timer == nil && now.Sub(st.lastSent) >= clearWindow {
			m.sendClear(k, st, rm.ID, at)
			continue
		}
		if rm.ID > st.pending {
			st.pending, st.pendAt = rm.ID, at
		}
		if st.timer == nil {
			k, st := k, st
			st.timer = time.AfterFunc(st.lastSent.Add(clearWindow).Sub(now), func() {
				m.post(func() { m.clearWindowEnded(k, st) })
			})
		}
	}
}

func (m *Module) clearWindowEnded(k clearKey, st *clearState) {
	if m.push.clears[k] != st {
		return
	}
	st.timer = nil
	id, at := st.pending, st.pendAt
	st.pending = 0
	if id > 0 && st.coversPushed(id) {
		m.sendClear(k, st, id, at)
	}
}

// sendClear seals and relays a clear of k's buffer up to readID.
func (m *Module) sendClear(k clearKey, st *clearState, readID int64, at time.Time) {
	st.markCleared(readID)
	st.lastSent = m.now()
	d := m.push.devices[k.device]
	if !d.active() || m.relayPausedNow(d.reg.RelayURL) {
		return
	}
	eid := push.NewEventID()
	pt, err := push.ClearPlaintext(push.Clear{EID: eid, Src: push.SourceGoBNC, Profile: d.reg.Profile,
		SessionID: sessionID(k.network), Buffer: k.key, ReadID: readID, Time: at.Unix()})
	if err != nil {
		m.log.Warn("push: clear content", "err", err)
		return
	}
	blob, err := push.Seal(d.pub, pt)
	if err != nil {
		m.log.Warn("push: seal clear", "err", err)
		return
	}
	m.relaySend(k.device, d.reg, push.NewRelayRequest(push.TypeClear, eid, at.Unix(), blob))
}
