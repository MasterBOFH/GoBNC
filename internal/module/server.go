// Package module is the brain side of the module API: separate processes ("modules") connect
// to a unix socket, say hello, and use the features their gobnc.json entry grants. See
// docs/module-api-design.md and pkg/moduleapi/wire for the protocol.
package module

import (
	"bufio"
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

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/session"
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
}

// Server accepts module connections on the module socket.
type Server struct {
	backend Backend
	log     *slog.Logger
	uid     int

	mu     sync.Mutex
	cfg    config.ModulesConfig
	conns  map[uint64]*Conn
	nextID uint64
}

// NewServer returns a server for cfg (Socket already resolved, see config.ResolvedModules).
func NewServer(cfg config.ModulesConfig, backend Backend, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{cfg: cfg, backend: backend, log: log, uid: os.Getuid(), conns: map[uint64]*Conn{}}
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

// Serve listens on the module socket until ctx is canceled. A stale socket file is removed
// first; the socket is created mode 0600 and removed on return.
func (s *Server) Serve(ctx context.Context) error {
	path := s.config().Socket
	if path == "" {
		return errors.New("module server: no socket path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("module server: %w", err)
	}
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("module server: %w", err)
	}
	defer func() {
		_ = ln.Close()
		_ = os.Remove(path)
	}()
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("module server: %w", err)
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
	c := newConn(s.nextID, s, nc, hello.module, hello.granted)
	hello.result.Ready = s.backend.Ready()
	c.readySent = hello.result.Ready
	resp, err := resultResponse(hello.id, hello.result)
	if err != nil {
		s.mu.Unlock()
		_ = nc.Close()
		return
	}
	c.Enqueue(resp)
	s.conns[c.id] = c
	s.mu.Unlock()
	s.log.Info("module connected", "module", hello.module, "granted", hello.granted)
	c.OnClose(func() {
		s.mu.Lock()
		delete(s.conns, c.id)
		s.mu.Unlock()
		s.log.Info("module disconnected", "module", hello.module, "overflow", c.overflowed.Load())
	})
	stop := context.AfterFunc(ctx, c.Close)
	defer stop()
	c.run(ctx, r)
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
	return helloInfo{module: p.Module, granted: granted, id: req.ID, result: res}, nil
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
