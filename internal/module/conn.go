package module

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// QueueSize is the number of frames a module connection may have queued for writing. The brain
// never blocks on a module: a full queue closes the connection.
const QueueSize = 8192

// overflowWriteTimeout bounds the best-effort overflow notice and unblocks a writer stuck on a
// module that stopped reading.
const overflowWriteTimeout = 250 * time.Millisecond

// Conn is one module connection after a successful hello. Frames are written in Enqueue order
// by a single writer goroutine; requests are read and dispatched one at a time.
type Conn struct {
	id      uint64
	srv     *Server
	module  string
	granted map[string]bool
	nc      net.Conn
	out     chan any

	closed     chan struct{}
	closeOnce  sync.Once
	overflowed atomic.Bool

	mu      sync.Mutex
	onClose []func()
	subs    map[int64]bool // networks this connection subscribed to (events.subscribe)

	readySent bool // guarded by Server.mu: hello said ready, or the ready event was queued
}

func newConn(id uint64, srv *Server, nc net.Conn, module string, granted []string) *Conn {
	g := make(map[string]bool, len(granted))
	for _, f := range granted {
		g[f] = true
	}
	return &Conn{id: id, srv: srv, module: module, granted: g, nc: nc,
		out: make(chan any, QueueSize), closed: make(chan struct{})}
}

// ID is unique per connection for the brain's lifetime.
func (c *Conn) ID() uint64 { return c.id }

// Module is the module name from hello.
func (c *Conn) Module() string { return c.module }

// Granted reports whether hello granted feature.
func (c *Conn) Granted(feature string) bool { return c.granted[feature] }

// OnClose registers f to run once when the connection closes (or immediately if it already has).
func (c *Conn) OnClose(f func()) {
	c.mu.Lock()
	select {
	case <-c.closed:
		c.mu.Unlock()
		f()
		return
	default:
	}
	c.onClose = append(c.onClose, f)
	c.mu.Unlock()
}

// Enqueue queues a frame (wire.Response or wire.Event) without blocking. It returns false when
// the connection is closed or the queue is full; a full queue closes the connection after a
// best-effort overflow event.
func (c *Conn) Enqueue(frame any) bool {
	select {
	case <-c.closed:
		return false
	default:
	}
	select {
	case c.out <- frame:
		return true
	default:
		c.overflowed.Store(true)
		c.Close()
		return false
	}
}

// EnqueueEvent marshals data and queues it as an event.
func (c *Conn) EnqueueEvent(name string, data any) bool {
	ev, err := wire.NewEvent(name, data)
	if err != nil {
		c.srv.log.Error("module: marshal event", "event", name, "err", err)
		return false
	}
	return c.Enqueue(ev)
}

// Close closes the connection; the writer goroutine finishes it and OnClose callbacks run.
func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		close(c.closed)
		c.mu.Unlock()
		// Unblock a writer stuck on a module that stopped reading.
		_ = c.nc.SetWriteDeadline(time.Now().Add(overflowWriteTimeout))
	})
}

// Closed is closed once the connection is closing.
func (c *Conn) Closed() <-chan struct{} { return c.closed }

// run serves the connection until it closes: the writer here, the reader in its own goroutine.
func (c *Conn) run(ctx context.Context, r *bufio.Reader) {
	go c.readLoop(ctx, r)
	c.writeLoop()
	_ = c.nc.Close()
	c.mu.Lock()
	callbacks := c.onClose
	c.onClose = nil
	c.mu.Unlock()
	for _, f := range callbacks {
		f()
	}
}

func (c *Conn) writeLoop() {
	for {
		select {
		case <-c.closed:
			c.finishWrites()
			return
		case f := <-c.out:
			if err := wire.WriteFrame(c.nc, f); err != nil {
				c.Close()
				c.finishWrites()
				return
			}
		}
	}
}

// finishWrites drains what is queued (a response written just before a requested close) unless
// the queue overflowed, in which case only the overflow event is attempted.
func (c *Conn) finishWrites() {
	_ = c.nc.SetWriteDeadline(time.Now().Add(overflowWriteTimeout))
	if c.overflowed.Load() {
		ev, _ := wire.NewEvent(wire.EventOverflow, wire.Empty{})
		_ = wire.WriteFrame(c.nc, ev)
		return
	}
	for {
		select {
		case f := <-c.out:
			if wire.WriteFrame(c.nc, f) != nil {
				return
			}
		default:
			return
		}
	}
}

func (c *Conn) readLoop(ctx context.Context, r *bufio.Reader) {
	defer c.Close()
	for {
		f, err := wire.ReadFrame(r)
		if err != nil {
			if errors.Is(err, wire.ErrFrameTooLarge) {
				c.Enqueue(errorResponse(0, wire.Errorf(wire.CodeBadFrame, "%v", err)))
			}
			return
		}
		if f.Kind != wire.KindRequest {
			c.Enqueue(errorResponse(0, wire.Errorf(wire.CodeBadFrame, "modules send requests only")))
			return
		}
		if resp, ok := c.srv.dispatch(ctx, c, f.Request); ok {
			c.Enqueue(resp)
		}
	}
}
