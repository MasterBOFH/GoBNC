package retecloud

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/gorilla/websocket"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Hot reload (gobnc module reload retecloud): the brain sends a reload event; the reloader
// (cmd/gobnc-retecloud) starts the replacement with the WSS listener and waits for it to
// confirm. On success every WebSocket is closed with 1012 "service restart" (clients reconnect
// to the replacement and resume with after_id) and Run returns ErrHandedOff; on failure the
// module keeps serving.

// ErrHandedOff is returned by Run once a replacement took over; the process releases its brain
// client (moduleapi.Client.Release) and exits.
var ErrHandedOff = errors.New("retecloud: handed off to a replacement")

// Close codes sent to WebSockets the module ends.
const (
	closeServiceRestart = websocket.CloseServiceRestart  // 1012: a replacement took over
	closeGoingAway      = websocket.CloseGoingAway       // 1001: the module is shutting down
	closeTryAgainLater  = websocket.CloseTryAgainLater   // 1013: the send queue overflowed
	closeAuthTimeout    = websocket.ClosePolicyViolation // 1008: no authenticate within authTimeout
)

// drainTimeout bounds how long Run waits for WebSockets to write their close frames.
var drainTimeout = 2 * time.Second

// Reloader starts a replacement for ev and returns once it confirmed (nil) or failed. It runs
// off the module loop.
type Reloader func(ev wire.ReloadEvent) error

// OnReload sets the reloader (call before Run). Without one, reload events are ignored.
func (m *Module) OnReload(f Reloader) { m.reloader = f }

// OnServing sets f to run once, on the loop, the first time the module is connected to a ready
// brain and knows its networks (a handoff replacement confirms then).
func (m *Module) OnServing(f func()) { m.onServing = f }

// onReloadEvent starts the reloader in its own goroutine so the loop keeps serving meanwhile.
func (m *Module) onReloadEvent(data json.RawMessage) {
	var ev wire.ReloadEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		m.log.Warn("reload: bad event", "err", err)
		return
	}
	if m.reloader == nil || m.reloading || m.handedOff {
		m.log.Warn("reload ignored", "reloader", m.reloader != nil, "in_progress", m.reloading)
		return
	}
	m.reloading = true
	f := m.reloader
	go func() {
		err := f(ev)
		m.post(func() { m.reloadDone(err) })
	}()
}

func (m *Module) reloadDone(err error) {
	m.reloading = false
	if err != nil {
		m.log.Warn("reload failed; still serving", "err", err)
		return
	}
	m.log.Info("replacement took over; closing WebSockets")
	m.handedOff = true
	for c := range m.conns {
		c.closeWith(closeServiceRestart, "service restart")
	}
}

// serving runs the OnServing callback once the brain is ready.
func (m *Module) serving() {
	if m.onServing == nil || !m.brain.Ready() {
		return
	}
	f := m.onServing
	m.onServing = nil
	f()
}

// drain waits up to drainTimeout for every WebSocket's writer to finish.
func (m *Module) drain() {
	deadline := time.After(drainTimeout)
	for c := range m.conns {
		select {
		case <-c.writeDone:
		case <-deadline:
			return
		}
	}
}
