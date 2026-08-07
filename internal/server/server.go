// Package server wires store, sessions, uplink, and downlink.
package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/control"
	"github.com/MasterBOFH/GoBNC/internal/downlink"
	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/uplink"
)

// Server is the bouncer process.
type Server struct {
	cfg       config.Config
	log       *slog.Logger
	store     *store.Store
	hist      *history.Store
	mu        sync.RWMutex
	sess      map[string]*session.Session
	netCancel map[string]context.CancelFunc
	runCtx    context.Context
	wg        sync.WaitGroup
	cancel    context.CancelFunc

	certs *certHolder
	dl    *downlink.Listener
}

// New opens the DB and prepares sessions.
func New(cfg config.Config, log *slog.Logger) (*Server, error) {
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	return &Server{
		cfg:       cfg,
		log:       log,
		store:     st,
		hist:      history.NewWithLimits(st, 100, cfg.LegacyPlaybackMax),
		sess:      make(map[string]*session.Session),
		netCancel: make(map[string]context.CancelFunc),
		certs:     &certHolder{},
	}, nil
}

// Store returns the DB.
func (s *Server) Store() *store.Store { return s.store }

// Session implements downlink.Manager.
func (s *Server) Session(network string) (*session.Session, error) {
	s.mu.RLock()
	sess, ok := s.sess[network]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("network %q not running", network)
	}
	return sess, nil
}

// Close cancels the run context, stops networks, and closes the DB.
func (s *Server) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.stopAllNetworks()
	s.wg.Wait()
	return s.store.Close()
}

// Run starts uplinks, control socket, and the TLS listener until ctx is done.
func (s *Server) Run(ctx context.Context) error {
	ctx, s.cancel = context.WithCancel(ctx)
	s.runCtx = ctx

	nets, err := s.store.ListNetworks(ctx)
	if err != nil {
		return err
	}
	for _, n := range nets {
		if !n.Enabled {
			continue
		}
		s.mu.Lock()
		err := s.startNetworkLocked(n)
		s.mu.Unlock()
		if err != nil {
			s.cancel()
			return err
		}
	}

	if err := s.serveControl(ctx); err != nil {
		s.cancel()
		return err
	}

	// Always run; pruneHistory no-ops when HistoryRetentionDays <= 0 (supports rehash enable).
	go s.retentionLoop(ctx)

	if err := s.certs.Load(s.cfg.TLSCert, s.cfg.TLSKey); err != nil {
		s.cancel()
		return err
	}
	tlsCfg := &tls.Config{
		GetCertificate: s.certs.GetCertificate,
		ClientAuth:     tls.RequestClientCert,
		MinVersion:     tls.VersionTLS12,
	}
	ln, err := tls.Listen("tcp", s.cfg.ListenAddr, tlsCfg)
	if err != nil {
		s.cancel()
		return err
	}
	defer ln.Close()

	dl := downlink.NewListener(s.cfg, s.store, s, tlsCfg, s.log)
	s.mu.Lock()
	s.dl = dl
	s.mu.Unlock()
	errCh := make(chan error, 1)
	go func() { errCh <- dl.Serve(ctx, ln) }()

	select {
	case <-ctx.Done():
		_ = ln.Close()
		s.gracefulShutdown()
		s.stopAllNetworks()
		s.wg.Wait()
		return nil
	case err := <-errCh:
		s.cancel()
		s.gracefulShutdown()
		s.stopAllNetworks()
		s.wg.Wait()
		return err
	}
}

// gracefulShutdown flushes uplink send queues and sends QUIT, bounded by ShutdownTimeout.
func (s *Server) gracefulShutdown() {
	timeout := config.ShutdownTimeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	s.mu.RLock()
	reason := s.cfg.QuitReason()
	s.mu.RUnlock()
	s.log.Info("graceful shutdown", "quit", reason, "timeout", timeout)

	s.mu.RLock()
	sessions := make([]*session.Session, 0, len(s.sess))
	for _, sess := range s.sess {
		sessions = append(sessions, sess)
	}
	s.mu.RUnlock()

	var wg sync.WaitGroup
	for _, sess := range sessions {
		wg.Add(1)
		go func(sess *session.Session) {
			defer wg.Done()
			sess.GracefulQuit(ctx, reason)
		}(sess)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.log.Info("shutdown timeout; proceeding to close uplinks")
	}
}

func (s *Server) stopAllNetworks() {
	s.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(s.netCancel))
	for name, cancel := range s.netCancel {
		cancels = append(cancels, cancel)
		delete(s.netCancel, name)
		delete(s.sess, name)
	}
	s.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// RetentionInterval is how often history prune runs (overridable in tests).
var RetentionInterval = time.Hour

func (s *Server) retentionLoop(ctx context.Context) {
	s.pruneHistory(ctx)
	t := time.NewTicker(RetentionInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.pruneHistory(ctx)
		}
	}
}

func (s *Server) pruneHistory(ctx context.Context) {
	s.mu.RLock()
	days := s.cfg.HistoryRetentionDays
	s.mu.RUnlock()
	if days <= 0 {
		return
	}
	cutoff := time.Now().UTC().Add(-time.Duration(days) * 24 * time.Hour)
	nets, err := s.store.ListNetworks(ctx)
	if err != nil {
		s.log.Debug("retention list networks", "err", err)
		return
	}
	for _, n := range nets {
		deleted, err := s.store.DeleteOlderThan(ctx, n.ID, cutoff)
		if err != nil {
			s.log.Debug("retention prune", "network", n.Name, "err", err)
			continue
		}
		if deleted > 0 {
			s.log.Info("history pruned", "network", n.Name, "deleted", deleted, "older_than_days", days)
		}
	}
}

func (s *Server) serveControl(ctx context.Context) error {
	path := s.cfg.ResolvedControlSocket()
	if path == "" {
		return nil
	}
	ln, err := control.ListenUnix(path)
	if err != nil {
		return fmt.Errorf("control socket: %w", err)
	}
	s.log.Info("control socket listening", "path", path)
	go func() {
		<-ctx.Done()
		_ = ln.Close()
		_ = os.Remove(path)
	}()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					s.log.Debug("control accept", "err", err)
					return
				}
			}
			go s.handleControl(c)
		}
	}()
	return nil
}

func (s *Server) handleControl(c net.Conn) {
	defer c.Close()
	uid, err := control.PeerUID(c)
	if err != nil {
		s.log.Debug("control peer uid", "err", err)
		return
	}
	if uid != os.Getuid() {
		s.log.Debug("control peer uid mismatch", "peer", uid, "self", os.Getuid())
		return
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return
	}
	line = strings.TrimSpace(line)
	parts := strings.Fields(line)
	reply := "ERR unknown command"
	if len(parts) > 0 {
		switch parts[0] {
		case control.CmdPing:
			reply = "OK"
		case control.CmdStartNetwork:
			if len(parts) < 2 {
				reply = "ERR usage: START_NETWORK <name>"
			} else if err := s.StartNetworkByName(parts[1]); err != nil {
				reply = "ERR " + err.Error()
			} else {
				reply = "OK"
			}
		case control.CmdStopNetwork:
			if len(parts) < 2 {
				reply = "ERR usage: STOP_NETWORK <name>"
			} else if err := s.StopNetwork(parts[1]); err != nil {
				reply = "ERR " + err.Error()
			} else {
				reply = "OK"
			}
		case control.CmdReloadNetwork:
			if len(parts) < 2 {
				reply = "ERR usage: RELOAD_NETWORK <name>"
			} else if err := s.ReloadNetworkConfig(parts[1]); err != nil {
				reply = "ERR " + err.Error()
			} else {
				reply = "OK"
			}
		}
	}
	_, _ = c.Write([]byte(reply + "\n"))
}

// StartNetworkByName loads a network from the DB and starts its uplink.
func (s *Server) StartNetworkByName(name string) error {
	if s.runCtx == nil {
		return fmt.Errorf("server not running")
	}
	n, err := s.store.NetworkByName(s.runCtx, name)
	if err != nil {
		return err
	}
	if !n.Enabled {
		return fmt.Errorf("network %q disabled", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sess[name]; ok {
		return fmt.Errorf("network %q already running", name)
	}
	return s.startNetworkLocked(n)
}

// caller must hold s.mu
func (s *Server) startNetworkLocked(n store.Network) error {
	chs, err := s.store.ListChannels(s.runCtx, n.ID)
	if err != nil {
		return err
	}
	nctx, cancel := context.WithCancel(context.Background())
	sess := session.New(n, s.store, s.hist, s.log.With("network", n.Name))
	u := uplink.New(uplink.Config{
		Network:       n,
		Channels:      chs,
		Logger:        s.log.With("uplink", n.Name),
		MaxFloodQueue: s.cfg.MaxFloodQueue,
	}, sess)
	sess.SetUplink(u)
	s.sess[n.Name] = sess
	s.netCancel[n.Name] = cancel
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		_ = u.Run(nctx)
	}()
	s.log.Info("network started", "name", n.Name)
	return nil
}

// StopNetwork stops a running network uplink.
func (s *Server) StopNetwork(name string) error {
	s.mu.Lock()
	cancel, ok := s.netCancel[name]
	if ok {
		delete(s.netCancel, name)
		delete(s.sess, name)
	}
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("network %q not running", name)
	}
	cancel()
	s.log.Info("network stopped", "name", name)
	return nil
}

// ReloadNetworkConfig loads network settings from the DB into a running session.
// The current uplink stays up; host/port/TLS/SASL apply on the next reconnect.
func (s *Server) ReloadNetworkConfig(name string) error {
	if s.runCtx == nil {
		return fmt.Errorf("server not running")
	}
	n, err := s.store.NetworkByName(s.runCtx, name)
	if err != nil {
		return err
	}
	s.mu.RLock()
	sess := s.sess[name]
	s.mu.RUnlock()
	if sess == nil {
		// Not running yet — next START_NETWORK / process start will use DB.
		return nil
	}
	sess.ApplyNetworkConfig(n)
	s.log.Info("network config reloaded", "name", name, "host", n.Host, "port", n.Port, "tls", n.TLS)
	return nil
}

// Rehash reloads gobnc.json and refreshes running network rows from SQLite.
// Existing downlink/uplink connections are not dropped. Listener TLS certs are
// hot-swapped for new handshakes via GetCertificate.
func (s *Server) Rehash(cfgPath string) error {
	if s.runCtx == nil {
		return fmt.Errorf("server not running")
	}
	newCfg, err := config.LoadJSON(cfgPath)
	if err != nil {
		return err
	}
	if err := newCfg.Validate(); err != nil {
		return err
	}

	s.mu.RLock()
	old := s.cfg
	s.mu.RUnlock()

	if newCfg.ListenAddr != old.ListenAddr {
		s.log.Warn("rehash: listen_addr change ignored (restart required)", "old", old.ListenAddr, "new", newCfg.ListenAddr)
		newCfg.ListenAddr = old.ListenAddr
	}
	if newCfg.DBPath != old.DBPath {
		s.log.Warn("rehash: db_path change ignored (restart required)", "old", old.DBPath, "new", newCfg.DBPath)
		newCfg.DBPath = old.DBPath
	}
	if newCfg.ResolvedControlSocket() != old.ResolvedControlSocket() {
		s.log.Warn("rehash: control_socket change ignored (restart required)",
			"old", old.ResolvedControlSocket(), "new", newCfg.ResolvedControlSocket())
		newCfg.ControlSocket = old.ControlSocket
	}
	if newCfg.LogFile != old.LogFile {
		s.log.Warn("rehash: log_file change ignored (restart required)", "old", old.LogFile, "new", newCfg.LogFile)
		newCfg.LogFile = old.LogFile
	}
	if newCfg.LogLevel != old.LogLevel {
		s.log.Warn("rehash: log_level change ignored (restart required)", "old", old.LogLevel, "new", newCfg.LogLevel)
		newCfg.LogLevel = old.LogLevel
	}

	if err := s.certs.Load(newCfg.TLSCert, newCfg.TLSKey); err != nil {
		return err
	}

	s.mu.Lock()
	s.cfg = newCfg
	dl := s.dl
	names := make([]string, 0, len(s.sess))
	sessions := make([]*session.Session, 0, len(s.sess))
	for name, sess := range s.sess {
		names = append(names, name)
		sessions = append(sessions, sess)
	}
	s.mu.Unlock()

	if dl != nil {
		dl.SetConfig(newCfg)
	}
	s.hist.SetLegacyPlaybackMax(newCfg.LegacyPlaybackMax)
	for _, sess := range sessions {
		if u := sess.Uplink(); u != nil {
			u.SetMaxFloodQueue(newCfg.MaxFloodQueue)
		}
	}
	for _, name := range names {
		if err := s.ReloadNetworkConfig(name); err != nil {
			s.log.Warn("rehash: network reload failed", "name", name, "err", err)
		}
	}
	s.log.Info("rehash complete", "tls_cert", newCfg.TLSCert, "networks", len(names))
	return nil
}

// StartNetwork starts a single network uplink (for tests).
func (s *Server) StartNetwork(ctx context.Context, n store.Network, cfg uplink.Config) (*session.Session, *uplink.Uplink) {
	s.runCtx = ctx
	chs, _ := s.store.ListChannels(ctx, n.ID)
	sess := session.New(n, s.store, s.hist, s.log)
	cfg.Network = n
	cfg.Channels = chs
	if cfg.Logger == nil {
		cfg.Logger = s.log
	}
	u := uplink.New(cfg, sess)
	sess.SetUplink(u)
	s.mu.Lock()
	s.sess[n.Name] = sess
	s.mu.Unlock()
	return sess, u
}

// ListenTLS is a helper for tests.
func ListenTLS(addr string, cfg *tls.Config) (net.Listener, error) {
	return tls.Listen("tcp", addr, cfg)
}
