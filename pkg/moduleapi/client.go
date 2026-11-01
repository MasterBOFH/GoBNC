// Package moduleapi is the Go client for GoBNC's module API: a module dials the brain's module
// socket, says hello, calls methods and receives events, reconnecting across brain reloads. See
// docs/module-api-design.md for the protocol and pkg/moduleapi/wire for its types.
package moduleapi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Options configure a module's connection. Empty Socket / Module come from GOBNC_MODULE_SOCKET /
// GOBNC_MODULE_NAME, which the brain sets when it starts a module.
type Options struct {
	Socket   string
	Module   string
	Features []string
	// SkipLock leaves the module lock alone (tests running several clients of one module).
	SkipLock bool
	// Handoff says this module can replace itself on a reload event (see Client.Handoff).
	Handoff bool
}

// ErrAlreadyRunning is returned by Dial when another process of this module holds its lock.
var ErrAlreadyRunning = errors.New("moduleapi: this module is already running")

// ErrNotConnected is returned by Call while the brain connection is down.
var ErrNotConnected = errors.New("moduleapi: not connected to the brain")

// Reconnect backoff: 200 ms, doubling, at most 5 s.
const (
	backoffMin = 200 * time.Millisecond
	backoffMax = 5 * time.Second
)

// EventBuffer is the capacity of the Events channel.
const EventBuffer = 8192

// LockPath is the lock file a running module holds so the brain never starts it twice.
func LockPath(socket, module string) string {
	return filepath.Join(filepath.Dir(socket), "gobnc-module-"+module+".lock")
}

// Config returns the module's config JSON from GOBNC_MODULE_CONFIG ("{}" when unset).
func Config() json.RawMessage {
	if v := os.Getenv("GOBNC_MODULE_CONFIG"); v != "" {
		return json.RawMessage(v)
	}
	return json.RawMessage("{}")
}

// Client is a module's connection to the brain. It survives brain reloads: Run redials and calls
// its onConnect again after every successful hello.
type Client struct {
	opts Options
	lock *os.File
	// lockInherited: lock came from a handoff; until ConfirmHandoff the previous process still
	// relies on it, so Close must not unlock it.
	lockInherited bool
	confirmed     atomic.Bool
	events        chan wire.Event
	ready         atomic.Bool

	mu      sync.Mutex
	conn    net.Conn
	hello   wire.HelloResult
	nextID  uint64
	pending map[uint64]chan wire.Response
	down    chan struct{} // closed when the current connection ends
	writeMu sync.Mutex
}

// Dial takes the module lock, connects and says hello.
func Dial(ctx context.Context, opts Options) (*Client, error) {
	if opts.Socket == "" {
		opts.Socket = os.Getenv("GOBNC_MODULE_SOCKET")
	}
	if opts.Module == "" {
		opts.Module = os.Getenv("GOBNC_MODULE_NAME")
	}
	if opts.Socket == "" || opts.Module == "" {
		return nil, errors.New("moduleapi: no socket or module name (GOBNC_MODULE_SOCKET / GOBNC_MODULE_NAME)")
	}
	c := &Client{opts: opts, events: make(chan wire.Event, EventBuffer)}
	if f := Inherited("lock"); f != nil && !opts.SkipLock {
		// A handoff replacement: the lock is the previous process's open file, already locked.
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			return nil, ErrAlreadyRunning
		}
		c.lock, c.lockInherited = f, true
	} else if !opts.SkipLock {
		f, err := os.OpenFile(LockPath(opts.Socket, opts.Module), os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return nil, fmt.Errorf("moduleapi: lock: %w", err)
		}
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			f.Close()
			return nil, ErrAlreadyRunning
		}
		c.lock = f
	}
	if err := c.connect(ctx); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// connect dials the socket and runs hello.
func (c *Client) connect(ctx context.Context) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", c.opts.Socket)
	if err != nil {
		return err
	}
	down := make(chan struct{})
	c.mu.Lock()
	c.conn, c.pending, c.down = conn, map[uint64]chan wire.Response{}, down
	c.mu.Unlock()
	go c.readLoop(conn, down)
	var hello wire.HelloResult
	if err := c.Call(ctx, wire.MethodHello, wire.HelloParams{API: wire.APIVersion, Module: c.opts.Module, Features: c.opts.Features,
		PID: os.Getpid(), Handoff: c.opts.Handoff}, &hello); err != nil {
		conn.Close()
		<-down
		return err
	}
	c.mu.Lock()
	c.hello = hello
	c.mu.Unlock()
	if hello.Ready {
		c.ready.Store(true)
	}
	return nil
}

func (c *Client) readLoop(conn net.Conn, down chan struct{}) {
	defer func() {
		c.ready.Store(false)
		c.mu.Lock()
		pending := c.pending
		c.pending = nil
		if c.conn == conn {
			c.conn = nil
		}
		c.mu.Unlock()
		for _, ch := range pending {
			close(ch)
		}
		close(down)
	}()
	r := bufio.NewReader(conn)
	for {
		f, err := wire.ReadFrame(r)
		if err != nil {
			return
		}
		switch f.Kind {
		case wire.KindResponse:
			c.mu.Lock()
			ch := c.pending[f.Response.ID]
			delete(c.pending, f.Response.ID)
			c.mu.Unlock()
			if ch != nil {
				ch <- *f.Response
			}
		case wire.KindEvent:
			if f.Event.Event == wire.EventReady {
				c.ready.Store(true)
			}
			c.events <- *f.Event
		}
	}
}

// Call sends a request and decodes its result into result (which may be nil). An error
// response comes back as *wire.Error.
func (c *Client) Call(ctx context.Context, method string, params, result any) error {
	b, err := json.Marshal(params)
	if err != nil {
		return err
	}
	ch := make(chan wire.Response, 1)
	c.mu.Lock()
	conn := c.conn
	if conn == nil || c.pending == nil {
		c.mu.Unlock()
		return ErrNotConnected
	}
	c.nextID++
	id := c.nextID
	c.pending[id] = ch
	c.mu.Unlock()
	c.writeMu.Lock()
	err = wire.WriteFrame(conn, wire.Request{ID: id, Method: method, Params: b})
	c.writeMu.Unlock()
	if err != nil {
		return err
	}
	select {
	case resp, ok := <-ch:
		if !ok {
			return ErrNotConnected
		}
		if resp.Error != nil {
			return resp.Error
		}
		if result != nil && len(resp.Result) > 0 {
			return json.Unmarshal(resp.Result, result)
		}
		return nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return ctx.Err()
	}
}

// Events delivers every event from the brain, across reconnects. Read it promptly: a module
// that falls behind is disconnected by the brain.
func (c *Client) Events() <-chan wire.Event { return c.events }

// Ready reports whether the brain is ready (LiveReady sent) on the current connection.
func (c *Client) Ready() bool { return c.ready.Load() }

// Hello returns the latest hello result (granted features, brain version).
func (c *Client) Hello() wire.HelloResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hello
}

// Run calls onConnect for the current connection and after every reconnect (re-subscribe there),
// redialling with backoff whenever the brain goes away, until ctx ends. An onConnect error drops
// the connection and retries.
func (c *Client) Run(ctx context.Context, onConnect func(ctx context.Context, c *Client) error) error {
	backoff := backoffMin
	for {
		c.mu.Lock()
		down := c.down
		connected := c.conn != nil
		c.mu.Unlock()
		if connected {
			if err := onConnect(ctx, c); err != nil {
				c.dropConn()
			} else {
				backoff = backoffMin
			}
			select {
			case <-down:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			if err := c.connect(ctx); err == nil {
				break
			}
			backoff = min(backoff*2, backoffMax)
		}
	}
}

func (c *Client) dropConn() {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
}

// Close drops the connection and releases the module lock (a handoff replacement that has not
// confirmed leaves the lock to the previous process, which shares it).
func (c *Client) Close() error {
	c.dropConn()
	if c.lock != nil {
		if !c.lockInherited || c.confirmed.Load() {
			_ = syscall.Flock(int(c.lock.Fd()), syscall.LOCK_UN)
		}
		_ = c.lock.Close()
		c.lock = nil
	}
	return nil
}
