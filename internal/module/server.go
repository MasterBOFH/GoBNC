// Package module is the brain side of the module API: separate processes ("modules") connect
// to a unix socket, say hello, and use the features their gobnc.json entry grants. See
// docs/module-api-design.md and pkg/moduleapi/wire for the protocol.
package module

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Backend is what the module server needs from the brain.
type Backend interface {
	BrainVersion() string
	// Ready reports whether the brain has sent LiveReady (writes to uplinks are possible).
	Ready() bool
	// Networks lists every configured network.
	Networks() []wire.NetworkInfo
	// Session returns a network's Session.
	Session(id int64) (*session.Session, bool)
	// Store is the brain's database (history, read markers, devices, module storage).
	Store() *store.Store
	// Config is the brain's current configuration (auth settings for auth.check).
	Config() config.Config
	// ConnectNetwork / DisconnectNetwork / ReconnectNetwork start, stop or redial a network.
	ConnectNetwork(id int64) error
	DisconnectNetwork(id int64) error
	ReconnectNetwork(id int64) error
}

// Server accepts module connections on the module socket.
type Server struct {
	backend Backend
	log     *slog.Logger
	uid     int

	mu      sync.Mutex
	cfg     config.ModulesConfig
	conns   map[uint64]*Conn
	nextID  uint64
	changed chan struct{} // closed and replaced when a connection registers or goes (Changed)

	commands    commandRegistry
	onListening func()

	authMu    sync.Mutex
	authFails map[string][]time.Time // auth.check failures per remote IP (rate limit)
	now       func() time.Time
}

// NewServer returns a server for cfg (Socket already resolved, see config.ResolvedModules).
func NewServer(cfg config.ModulesConfig, backend Backend, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{cfg: cfg, backend: backend, log: log, uid: os.Getuid(), conns: map[uint64]*Conn{},
		changed: make(chan struct{}), now: time.Now}
}

// SetConfig replaces the module list after a rehash. Connections already past hello keep the
// grants they got; new connections use the new list.
func (s *Server) SetConfig(cfg config.ModulesConfig) {
	s.mu.Lock()
	s.cfg.List = cfg.List
	s.mu.Unlock()
}

func (s *Server) config() config.ModulesConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg
}

// Serve listens on the module socket until ctx is canceled. A stale socket file (or a previous
// brain's, during a reload) is replaced; the socket is created mode 0600 and left in place on
// return.
func (s *Server) Serve(ctx context.Context) error {
	path := s.config().Socket
	if path == "" {
		return errors.New("module server: no socket path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("module server: %w", err)
	}
	// A stale socket (or a previous brain's, still listening during a reload) is replaced:
	// modules connecting from now on reach this brain.
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("module server: %w", err)
	}
	// The socket file is left in place on exit: during a reload the replacement brain may
	// already have put its own socket at path, and the next brain to start replaces a stale one.
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	defer ln.Close()
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("module server: %w", err)
	}
	s.mu.Lock()
	onListening := s.onListening
	s.mu.Unlock()
	if onListening != nil {
		onListening()
	}
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	if !peerCredSupported {
		s.log.Warn("module server: peer credential verification is not supported on this platform; relying on the socket's 0600 mode alone")
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if !s.authorizePeer(c) {
			_ = c.Close()
			continue
		}
		go s.serveConn(ctx, c)
	}
}

// authorizePeer accepts only processes running as the brain's own uid.
func (s *Server) authorizePeer(c net.Conn) bool {
	if !peerCredSupported {
		return true
	}
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return false
	}
	uid, err := peerUID(uc)
	if err != nil {
		s.log.Warn("module server: peer credential check failed, rejecting connection", "err", err)
		return false
	}
	if uid != s.uid {
		s.log.Warn("module server: rejected connection from unexpected uid", "uid", uid, "want", s.uid)
		return false
	}
	return true
}

func (s *Server) serveConn(ctx context.Context, nc net.Conn) {
	r := bufio.NewReader(nc)
	hello, err := s.handshake(r, nc)
	if err != nil {
		s.log.Debug("module server: hello failed", "err", err)
		_ = nc.Close()
		return
	}
	// Register the connection and queue the hello response under s.mu, the lock MarkReady
	// takes: a module learns readiness exactly once, from hello or from the ready event.
	s.mu.Lock()
	s.nextID++
	c := newConn(s.nextID, s, nc, hello)
	hello.result.Ready = s.backend.Ready()
	c.readySent = hello.result.Ready
	resp, err := resultResponse(hello.id, hello.result)
	if err != nil {
		s.mu.Unlock()
		_ = nc.Close()
		return
	}
	c.Enqueue(resp)
	c.since = s.now()
	s.conns[c.id] = c
	s.notifyChangedLocked()
	s.mu.Unlock()
	s.log.Info("module connected", "module", hello.module, "pid", hello.pid, "handoff", hello.handoff, "granted", hello.granted)
	c.OnClose(func() {
		s.mu.Lock()
		delete(s.conns, c.id)
		s.notifyChangedLocked()
		s.mu.Unlock()
		s.log.Info("module disconnected", "module", hello.module, "overflow", c.overflowed.Load())
	})
	stop := context.AfterFunc(ctx, c.Close)
	defer stop()
	c.run(ctx, r)
}

// OnListening sets f to run once Serve's socket accepts connections (call before Serve).
func (s *Server) OnListening(f func()) {
	s.mu.Lock()
	s.onListening = f
	s.mu.Unlock()
}

// ModuleConn describes one module connection past hello.
type ModuleConn struct {
	ID      uint64
	Module  string
	PID     int
	Handoff bool
	Granted []string
	Since   time.Time
}

// Modules lists every connection past hello, oldest (lowest ID) first.
func (s *Server) Modules() []ModuleConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ModuleConn, 0, len(s.conns))
	for _, c := range s.conns {
		granted := make([]string, 0, len(c.granted))
		for f := range c.granted {
			granted = append(granted, f)
		}
		slices.Sort(granted)
		out = append(out, ModuleConn{ID: c.id, Module: c.module, PID: c.pid, Handoff: c.handoff, Granted: granted, Since: c.since})
	}
	slices.SortFunc(out, func(a, b ModuleConn) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// SendEvent queues event name with data on one connection.
func (s *Server) SendEvent(connID uint64, name string, data any) error {
	s.mu.Lock()
	c := s.conns[connID]
	s.mu.Unlock()
	if c == nil || !c.EnqueueEvent(name, data) {
		return fmt.Errorf("no module connection %d", connID)
	}
	return nil
}

// Changed returns a channel closed at the next connection registering or going; re-read
// Modules after it fires and call Changed again for the next one.
func (s *Server) Changed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed
}

func (s *Server) notifyChangedLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// Connected reports whether a module of that name is connected (past hello).
func (s *Server) Connected(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		if c.module == name {
			return true
		}
	}
	return false
}

// Conns returns the connections past hello.
func (s *Server) Conns() []*Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Conn, 0, len(s.conns))
	for _, c := range s.conns {
		out = append(out, c)
	}
	return out
}

// Broadcast sends an event to every connection granted feature.
func (s *Server) Broadcast(feature, event string, data any) {
	for _, c := range s.Conns() {
		if c.Granted(feature) {
			c.EnqueueEvent(event, data)
		}
	}
}

type helloInfo struct {
	module  string
	granted []string
	pid     int
	handoff bool
	id      uint64 // the hello request's id
	result  wire.HelloResult
}

// MarkReady tells every connected module that has not yet learnt it that the brain is ready
// (LiveReady sent). Backend.Ready must already report true.
func (s *Server) MarkReady() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		if !c.readySent {
			c.readySent = true
			c.EnqueueEvent(wire.EventReady, wire.Empty{})
		}
	}
}

// handshake reads the first frame, which must be a valid hello, and returns what to answer. On
// any error the error response has been written and the caller closes the connection.
func (s *Server) handshake(r *bufio.Reader, c net.Conn) (helloInfo, error) {
	f, err := wire.ReadFrame(r)
	if err != nil {
		writeError(c, 0, wire.CodeBadFrame, err.Error())
		return helloInfo{}, err
	}
	if f.Kind != wire.KindRequest || f.Request.Method != wire.MethodHello {
		var id uint64
		if f.Request != nil {
			id = f.Request.ID
		}
		writeError(c, id, wire.CodeHelloRequired, "the first request must be hello")
		return helloInfo{}, errors.New(wire.CodeHelloRequired)
	}
	req := f.Request
	var p wire.HelloParams
	if err := json.Unmarshal(req.Params, &p); err != nil {
		writeError(c, req.ID, wire.CodeBadParams, err.Error())
		return helloInfo{}, err
	}
	if p.API != wire.APIVersion {
		writeError(c, req.ID, wire.CodeUnsupportedAPI, fmt.Sprintf("api %d (this brain speaks %d)", p.API, wire.APIVersion))
		return helloInfo{}, errors.New(wire.CodeUnsupportedAPI)
	}
	mod, ok := s.config().Module(p.Module)
	if !ok {
		writeError(c, req.ID, wire.CodeNotGranted, fmt.Sprintf("module %q is not configured", p.Module))
		return helloInfo{}, errors.New(wire.CodeNotGranted)
	}
	granted, denied := grant(p.Features, mod.Features)
	res := wire.HelloResult{API: wire.APIVersion, BrainVersion: s.backend.BrainVersion(),
		Granted: granted, Denied: denied}
	return helloInfo{module: p.Module, granted: granted, pid: p.PID, handoff: p.Handoff, id: req.ID, result: res}, nil
}

// grant splits the requested features into those the module's config allows and the rest,
// in request order and without duplicates.
func grant(requested, configured []string) (granted, denied []string) {
	granted, denied = []string{}, []string{}
	for _, f := range requested {
		if slices.Contains(granted, f) || slices.Contains(denied, f) {
			continue
		}
		if slices.Contains(configured, f) {
			granted = append(granted, f)
		} else {
			denied = append(denied, f)
		}
	}
	return granted, denied
}

func writeError(c net.Conn, id uint64, code, msg string) {
	_ = wire.WriteFrame(c, wire.Response{ID: id, Error: &wire.Error{Code: code, Message: msg}})
}
