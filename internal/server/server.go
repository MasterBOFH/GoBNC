// Package server wires store, sessions, the shared keeper/brain uplink, and
// downlink.
package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/brain"
	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/control"
	"github.com/MasterBOFH/GoBNC/internal/downlink"
	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/keeperboot"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/version"
)

// Server is the bouncer process.
type Server struct {
	cfg         config.Config
	cfgPath     string // bootstrap JSON path for REHASH / SIGHUP
	log         *slog.Logger
	logSink     *gobnclog.Sink
	logCons     io.Writer // console writer for log Reload (default stderr)
	debugCons   bool      // serve -debug: keep console at debug across rehash
	daemonLog   bool      // resolve empty log_file to state-dir default
	store       *store.Store
	hist        *history.Store
	mu          sync.RWMutex
	sess        map[string]*session.Session
	sessByNetID map[keeper.NetworkID]*session.Session
	runCtx      context.Context
	wg          sync.WaitGroup
	cancel      context.CancelFunc

	// keeperClient / driver are the one shared attach and one shared
	// Driver every session on this process addresses by its own
	// keeper.NetworkID — internal/keeper and internal/brain were designed
	// for one connection serving every network a process holds, not one
	// attach per network (see docs/keeper-design.md). Set up once in Run,
	// via internal/keeperboot.
	keeperClient *keeper.AttachClient
	driver       *brain.Driver

	// resumedAtBoot records, for Run's initial startNetworkLocked pass
	// only, which networks the keeper already held live at attach time
	// (from keeperClient.Networks) — see startNetworkLocked's own comment
	// for why those must skip Dial. Each entry is deleted as it's
	// consumed, so a later admin-triggered StartNetworkByName/
	// ReconnectNetwork for the same network always dials for real.
	resumedAtBoot map[keeper.NetworkID]bool

	certs *certHolder
	dl    *downlink.Listener

	// shutdownKind is set by RequestReload/RequestDie before canceling
	// runCtx so cmd/gobnc can spawn a replacement brain or stop the keeper
	// after Run returns. Zero means an ordinary stop (keeper stays).
	shutdownKind shutdownKind
}

type shutdownKind int

const (
	shutdownStop shutdownKind = iota
	shutdownReload
	shutdownDie
)

// New opens the DB and prepares sessions.
func New(cfg config.Config, log *slog.Logger) (*Server, error) {
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	hist := history.NewWithLimits(st, cfg.ChatHistoryMax, cfg.LegacyPlaybackMax)
	hist.SetLogger(log)
	return &Server{
		cfg:         cfg,
		log:         log,
		store:       st,
		hist:        hist,
		sess:        make(map[string]*session.Session),
		sessByNetID: make(map[keeper.NetworkID]*session.Session),
		certs:       &certHolder{},
	}, nil
}

// bootstrapKeeper finds or starts the keeper process and attaches to it,
// mirroring cmd/brain-register-demo's own bootstrap exactly — this is the
// real bouncer's first caller of internal/keeperboot, closing the gap
// deliberately left open since keeperboot was built (see its package doc).
//
// Deliberately does NOT send LiveReady itself: that must wait until every
// network Run is about to start has already been registered (session
// created, driver.RegisterNetwork/sessByNetID populated) — see Run's own
// comment on why. Attach's own Hello/HelloAck handshake (inside
// keeperboot.EnsureRunning) is what populates res.Client.Networks, and
// that already happens before LiveReady is ever sent, so resumedAtBoot is
// accurate regardless of when LiveReady itself goes out.
func (s *Server) bootstrapKeeper(ctx context.Context) error {
	bootCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	res, err := keeperboot.EnsureRunning(bootCtx, keeperboot.Options{
		Hello:  keeper.HelloMsg{Mode: keeper.ModeLive},
		Logger: s.log,
	})
	if err != nil {
		return fmt.Errorf("keeperboot: %w", err)
	}
	if res.Spawned {
		s.log.Info("spawned a new keeper", "pid", res.KeeperPID)
	} else {
		s.log.Info("attached to an existing keeper")
	}
	s.keeperClient = res.Client
	s.driver = brain.NewDriver(s.keeperClient, brain.WithLogger(s.log))
	s.driver.SetMaxFloodQueue(s.cfg.MaxFloodQueue)

	s.resumedAtBoot = make(map[keeper.NetworkID]bool, len(res.Client.Networks))
	for _, st := range res.Client.Networks {
		if st.State == keeper.Connected {
			s.resumedAtBoot[st.ID] = true
		}
	}
	return nil
}

// SetConfigPath records the bootstrap JSON path used by Rehash (SIGHUP / REHASH).
func (s *Server) SetConfigPath(path string) {
	s.mu.Lock()
	s.cfgPath = path
	s.mu.Unlock()
}

// SetLogSink wires a reloadable log sink for rehash (log_level / log_file).
// console is the writer used on Reload (typically os.Stderr).
// debugConsole keeps the console at debug when serve -debug was set.
// daemonLogDefault uses ResolvedLogFile(true) when log_file is empty.
func (s *Server) SetLogSink(sink *gobnclog.Sink, console io.Writer, debugConsole, daemonLogDefault bool) {
	s.mu.Lock()
	s.logSink = sink
	s.logCons = console
	s.debugCons = debugConsole
	s.daemonLog = daemonLogDefault
	s.mu.Unlock()
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

// Close cancels the run context, detaches from the keeper (see
// detachFromKeeper — this never disconnects any uplink), and closes the DB.
func (s *Server) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.detachFromKeeper()
	s.wg.Wait()
	return s.store.Close()
}

// Run starts the shared keeper attach, sessions, control socket, and the
// TLS listener until ctx is done.
func (s *Server) Run(ctx context.Context) error {
	ctx, s.cancel = context.WithCancel(ctx)
	s.runCtx = ctx

	if err := s.bootstrapKeeper(ctx); err != nil {
		s.cancel()
		return err
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.driver.Run(ctx); err != nil {
			s.log.Debug("driver.Run exited", "err", err)
		}
	}()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runDemux(ctx)
	}()
	if s.logSink != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.logSink.DebugRegistry().Run(ctx)
		}()
	}

	nets, err := s.store.ListNetworks(ctx)
	if err != nil {
		return err
	}
	// Every enabled network's Session and driver bookkeeping must exist
	// before SendLiveReady goes out. The keeper starts streaming a
	// resumed network's backlog the instant LiveReady arrives — if that
	// happened before this brain had registered the network (RegisterNetwork
	// populating brain.Driver's own tracking, sessByNetID populating the
	// demux's routing table), the earliest replayed lines (including the
	// original 001) would arrive at Driver.handleLine/runDemux with nowhere
	// to go and be silently dropped, permanently stalling that Session's
	// own self-detected registration (see upstream.go's HandleLine). Dialing
	// a genuinely fresh network, by contrast, must happen *after*
	// LiveReady: the keeper's serveLive doesn't read DialRequest frames —
	// or anything else — until LiveReady is the first frame it sees.
	type pendingNet struct {
		n    store.Network
		sess *session.Session
	}
	pending := make([]pendingNet, 0, len(nets))
	s.mu.Lock()
	for _, n := range nets {
		if !n.Enabled {
			continue
		}
		sess, err := s.registerNetworkLocked(n)
		if err != nil {
			s.mu.Unlock()
			s.cancel()
			return err
		}
		pending = append(pending, pendingNet{n: n, sess: sess})
	}
	s.mu.Unlock()

	if err := s.keeperClient.SendLiveReady(); err != nil {
		s.cancel()
		return fmt.Errorf("keeper SendLiveReady: %w", err)
	}

	s.mu.Lock()
	for _, p := range pending {
		if err := s.dialNetworkLocked(p.n, p.sess); err != nil {
			s.mu.Unlock()
			s.cancel()
			return err
		}
	}
	s.mu.Unlock()

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

	dl := downlink.NewListener(s.cfg, s.store, s, tlsCfg, s.log)
	s.mu.Lock()
	s.dl = dl
	s.mu.Unlock()
	errCh := make(chan error, 1)
	go func() { errCh <- dl.Serve(ctx, ln) }()

	select {
	case <-ctx.Done():
		s.detachFromKeeper()
		s.wg.Wait()
		return nil
	case err := <-errCh:
		s.cancel()
		s.detachFromKeeper()
		s.wg.Wait()
		return err
	}
}

// detachFromKeeper closes the shared keeper attach (unblocking
// driver.Run/runDemux, per Driver.Run's own doc comment on why ctx alone
// can't stop it) and clears session bookkeeping. Deliberately does NOT
// send QUIT to any network and does NOT ask the keeper to close any
// uplink: gobnc (the brain) exiting — whether for a SIGTERM, a `gobnc
// stop`, or a code upgrade — must never be conflated with a deliberate
// per-network disconnect (see brain.Driver.QuitNetwork's doc comment,
// which states this as the one mistake that costs everything on this
// project). The keeper process and every uplink it holds keep running,
// completely unaffected by this call, ready for the next `gobnc` process
// to attach and resume — that persistence is this project's entire point.
// A real full stop (actually disconnecting from IRC) means stopping the
// keeper process itself, a separate, deliberate operator action this
// method does not take on the caller's behalf.
func (s *Server) detachFromKeeper() {
	if s.keeperClient != nil {
		_ = s.keeperClient.Close()
	}
	s.mu.Lock()
	s.sess = make(map[string]*session.Session)
	s.sessByNetID = make(map[keeper.NetworkID]*session.Session)
	s.mu.Unlock()
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
		case control.CmdReconnectNetwork:
			if len(parts) < 2 {
				reply = "ERR usage: RECONNECT_NETWORK <name>"
			} else if err := s.ReconnectNetwork(parts[1]); err != nil {
				reply = "ERR " + err.Error()
			} else {
				reply = "OK"
			}
		case control.CmdRehash:
			s.mu.RLock()
			path := s.cfgPath
			s.mu.RUnlock()
			if path == "" {
				reply = "ERR no config path (SetConfigPath required)"
			} else if err := s.Rehash(path); err != nil {
				reply = "ERR " + err.Error()
			} else {
				reply = "OK"
			}
		case control.CmdShutdown:
			reply = "OK"
			go s.RequestShutdown()
		case control.CmdReload:
			if err := s.RequestReload(); err != nil {
				reply = "ERR " + err.Error()
			} else {
				reply = "OK"
			}
		case control.CmdDie:
			reply = "OK"
			go func() { _ = s.RequestDie() }()
		case control.CmdStatus:
			reply = statusControlReply(s)
		}
	}
	_, _ = c.Write([]byte(reply + "\n"))
}

// RequestShutdown cancels the run context (graceful stop via SIGTERM path).
// The keeper is not signaled; uplinks stay up.
func (s *Server) RequestShutdown() {
	s.mu.RLock()
	cancel := s.cancel
	s.mu.RUnlock()
	if cancel != nil {
		cancel()
	}
}

// RequestReload asks the brain process to exit and be replaced by a fresh
// exec of the on-disk binary. The keeper is not touched. Fails without
// canceling if the running keeper is below this binary's MinKeeperVersion
// (the old brain stays attached). On that failure the operator runs
// gobnc die then starts this binary, which spawns a new keeper.
func (s *Server) RequestReload() error {
	s.mu.Lock()
	running := 0
	if s.keeperClient != nil {
		running = s.keeperClient.KeeperVersion
	}
	if version.CanUpgrade(running) == version.UpgradeMust {
		s.mu.Unlock()
		return fmt.Errorf("keeper is incompatible; gobnc die then start this binary")
	}
	s.shutdownKind = shutdownReload
	cancel := s.cancel
	s.mu.Unlock()
	if cancel == nil {
		return fmt.Errorf("server not running")
	}
	cancel()
	return nil
}

// RequestDie asks this brain to exit and, after Run returns, for cmd/gobnc
// to SIGTERM the keeper (QUIT every uplink).
func (s *Server) RequestDie() error {
	s.mu.Lock()
	s.shutdownKind = shutdownDie
	cancel := s.cancel
	s.mu.Unlock()
	if cancel == nil {
		return fmt.Errorf("server not running")
	}
	cancel()
	return nil
}

func (s *Server) WantReload() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.shutdownKind == shutdownReload
}

func (s *Server) WantDie() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.shutdownKind == shutdownDie
}

// ConfigPath is the bootstrap JSON path (for re-exec on reload).
func (s *Server) ConfigPath() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfgPath
}

// DebugConsole reports whether serve -debug was set (re-exec should keep it).
func (s *Server) DebugConsole() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.debugCons
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
	sess, err := s.registerNetworkLocked(n)
	if err != nil {
		return err
	}
	return s.dialNetworkLocked(n, sess)
}

// registerNetworkLocked creates n's Session and every piece of driver/demux
// bookkeeping a line arriving for it needs to be routed anywhere
// (brain.Driver.RegisterNetwork, sessByNetID) — everything short of
// actually dialing or resuming. Split out of startNetworkLocked for Run's
// own bootstrap sequence, which must register every network before
// SendLiveReady goes out (see Run's comment); the admin/StartNetworkByName
// path (startNetworkLocked, called long after LiveReady is already active)
// just calls this immediately followed by dialNetworkLocked.
//
// caller must hold s.mu
func (s *Server) registerNetworkLocked(n store.Network) (*session.Session, error) {
	chs, err := s.store.ListChannels(s.runCtx, n.ID)
	if err != nil {
		return nil, err
	}
	sess := session.New(n, s.store, s.hist, s.log.With("network", n.Name), s.driver)
	netID := sess.NetworkID()
	s.attachAdmin(sess)
	if s.logSink != nil {
		sess.SetDebugRegistry(s.logSink.DebugRegistry())
	}
	s.sess[n.Name] = sess
	s.sessByNetID[netID] = sess

	// A network the keeper already held live at this brain's own attach
	// (see resumedAtBoot's doc comment, and dialNetworkLocked's use of the
	// same flag) must not redrive registration — see
	// brain.Driver.RegisterNetwork's own doc comment for the hazard this
	// avoids (duplicate CAP negotiation actually reaching the live uplink,
	// duplicate auto-join/nick-recovery restart). Read here, before
	// dialNetworkLocked's own check of the same flag runs and consumes it
	// (deletes the entry) — registerNetworkLocked always runs first in
	// every caller (Run's boot pass, StartNetworkByName), so the flag is
	// still accurate at this point for whichever it is.
	if s.resumedAtBoot[netID] {
		s.driver.RegisterResumedNetwork(netID, s.networkConfigForLocked(n))
		// Gap-only delivery (see docs/keeper-design.md) means this
		// network's original registration burst will never be replayed —
		// nothing will otherwise ever make sess.Registered() true. Seed
		// directly from the blob snapshot HelloAck already delivered at
		// attach time instead of waiting for traffic that isn't coming.
		sess.SeedFromBlob(s.blobForLocked(netID))
	} else {
		s.driver.RegisterNetwork(netID, s.networkConfigForLocked(n))
	}
	s.driver.SetChannels(netID, channelJoinsFor(chs))
	s.driver.SetFloodParams(netID, n.FloodBurst, n.FloodRate)
	return sess, nil
}

// blobForLocked returns netID's blob snapshot from this brain's own
// attach-time HelloAck (s.keeperClient.Networks — see
// keeper.NetworkStatus.Blob's doc comment), or nil if netID isn't there
// (a network the keeper has never held, or one whose blob is empty).
// Caller must hold s.mu.
func (s *Server) blobForLocked(netID keeper.NetworkID) []keeper.BlobEntry {
	for _, st := range s.keeperClient.Networks {
		if st.ID == netID {
			return st.Blob
		}
	}
	return nil
}

// dialNetworkLocked dials n's uplink, unless the keeper already held it
// live at this brain's own attach (see resumedAtBoot), in which case it
// just records cfg for a later Reconnect without redialing. Caller must
// already have called registerNetworkLocked for sess (or equivalent) and
// must hold s.mu.
func (s *Server) dialNetworkLocked(n store.Network, sess *session.Session) error {
	netID := sess.NetworkID()
	if s.resumedAtBoot[netID] {
		// The keeper already holds this network's uplink live — this
		// brain process is resuming after a restart, not starting fresh.
		// Dial would only get keeper.ErrAlreadyConnected back, which
		// Driver's Dial-failure path can't tell apart from a genuine
		// failure: it would arm a perpetual auto-reconnect that retries
		// forever and never succeeds, since the connection was never
		// actually down (see brain.Driver.StartRegistration's doc
		// comment on why a resumed network must never reach it either).
		// Nothing else to do here: the keeper's own serveLive already
		// resumes streaming this network's lines to our attach from its
		// resume watermark (gap-only, not a full replay — see
		// docs/keeper-design.md), and registerNetworkLocked already seeded
		// Session directly from the delivered blob snapshot
		// (Session.SeedFromBlob) rather than waiting for registration
		// traffic that won't be replayed. UpdateDialConfig still records
		// cfg so a later real Reconnect has something to redial with.
		s.driver.UpdateDialConfig(netID, s.dialConfigForLocked(n))
		delete(s.resumedAtBoot, netID)
		s.log.Info("network resumed", "name", n.Name)
		// The blob never carried a channel roster, only name+key (see
		// Session.RefreshResumedChannelNames' doc comment) — this is the
		// earliest point after SendLiveReady (Run calls registerNetworkLocked
		// for every network, then SendLiveReady, then dialNetworkLocked; see
		// Run's own comment on that ordering) where writing to the uplink is
		// actually safe, which is why the write couldn't happen back in
		// registerNetworkLocked/SeedFromBlob itself. Same reasoning for
		// RefreshSelfUserHost — the blob's "cloak" key is often absent too
		// (see its own doc comment), and self.Host staying unknown forever
		// is what produces an RFC-invalid nick!user prefix on every JOIN
		// this session ever replays to a client. Same again for
		// RefreshSelfUModes — usermodes have no blob key, and without a
		// live MODE nick poll a resumed Attach would omit the own-MODE
		// line connecting clients need to learn their modes.
		// Self first, channels last — mirrors a normal live registration's
		// own ordering (welcome, self MODE, only then anything
		// channel-shaped), and means a downlink attaching mid-refresh is
		// more likely to already have real umodes/host by the time it
		// sees any channel traffic, rather than the reverse.
		sess.RefreshSelfUModes()
		sess.RefreshSelfUserHost()
		sess.RefreshResumedChannelNames()
		return nil
	}
	if err := s.driver.Dial(netID, s.dialConfigForLocked(n), 0); err != nil {
		delete(s.sess, n.Name)
		delete(s.sessByNetID, netID)
		return err
	}
	s.log.Info("network started", "name", n.Name)
	return nil
}

// StopNetwork stops a running network uplink — the connection closes and
// does not auto-redial (see brain.Driver.StopNetwork) until a later
// StartNetworkByName/ReconnectNetwork resumes it.
func (s *Server) StopNetwork(name string) error {
	s.mu.Lock()
	sess, ok := s.sess[name]
	if ok {
		delete(s.sess, name)
		delete(s.sessByNetID, sess.NetworkID())
	}
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("network %q not running", name)
	}
	if err := s.driver.StopNetwork(sess.NetworkID()); err != nil {
		return err
	}
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
	if sess == nil {
		s.mu.RUnlock()
		// Not running yet — next START_NETWORK / process start will use DB.
		return nil
	}
	cfg := s.networkConfigForLocked(n)
	s.mu.RUnlock()
	sess.ApplyNetworkConfig(n, cfg)
	s.log.Info("network config reloaded", "name", name, "host", n.Host, "port", n.Port, "tls", n.TLS)
	return nil
}

// ReconnectNetwork reloads network settings from the DB and drops the uplink
// connection so it dials again immediately. Downlinks stay attached.
// If the network is not running, it is started instead.
func (s *Server) ReconnectNetwork(name string) error {
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
	sess := s.sess[name]
	if sess == nil {
		err := s.startNetworkLocked(n)
		s.mu.Unlock()
		if err != nil {
			return err
		}
		s.log.Info("network started via reconnect", "name", name, "host", n.Host, "port", n.Port, "tls", n.TLS)
		return nil
	}
	s.mu.Unlock()

	s.mu.RLock()
	netCfg := s.networkConfigForLocked(n)
	dialCfg := s.dialConfigForLocked(n)
	s.mu.RUnlock()
	sess.ApplyNetworkConfig(n, netCfg)
	if chs, err := s.store.ListChannels(s.runCtx, n.ID); err == nil {
		s.driver.SetChannels(sess.NetworkID(), channelJoinsFor(chs))
	}
	s.driver.UpdateDialConfig(sess.NetworkID(), dialCfg)
	if err := s.driver.Reconnect(sess.NetworkID()); err != nil {
		return err
	}
	s.log.Info("network reconnect requested", "name", name, "host", n.Host, "port", n.Port, "tls", n.TLS)
	return nil
}

// Rehash reloads gobnc.json and refreshes running network rows from SQLite.
// Existing downlink/uplink connections are not dropped. Listener TLS certs are
// hot-swapped for new handshakes via GetCertificate. log_level, log_file, and
// listen_addr apply immediately (existing TLS clients stay on the old socket
// until they disconnect when listen_addr changes).
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
	debugCons := s.debugCons
	daemonLog := s.daemonLog
	logSink := s.logSink
	logCons := s.logCons
	s.mu.RUnlock()

	if newCfg.DBPath != old.DBPath {
		s.log.Warn("rehash: db_path change ignored (restart required)", "old", old.DBPath, "new", newCfg.DBPath)
		newCfg.DBPath = old.DBPath
	}
	if newCfg.ResolvedControlSocket() != old.ResolvedControlSocket() {
		s.log.Warn("rehash: control_socket change ignored (restart required)",
			"old", old.ResolvedControlSocket(), "new", newCfg.ResolvedControlSocket())
		newCfg.ControlSocket = old.ControlSocket
	}

	if err := s.certs.Load(newCfg.TLSCert, newCfg.TLSKey); err != nil {
		return err
	}

	if logSink != nil {
		consoleLevel := newCfg.LogLevel
		if debugCons {
			consoleLevel = "debug"
		}
		console := logCons
		if console == nil {
			console = os.Stderr
		}
		if err := logSink.Reload(gobnclog.Options{
			Level:     consoleLevel,
			FileLevel: newCfg.LogLevel,
			Console:   console,
			File:      newCfg.ResolvedLogFile(daemonLog),
		}); err != nil {
			return fmt.Errorf("rehash log: %w", err)
		}
	}

	s.mu.Lock()
	s.cfg = newCfg
	dl := s.dl
	names := make([]string, 0, len(s.sess))
	for name := range s.sess {
		names = append(names, name)
	}
	s.mu.Unlock()

	if dl != nil {
		dl.SetConfig(newCfg)
	}
	s.hist.SetMaxLimit(newCfg.ChatHistoryMax)
	s.hist.SetLegacyPlaybackMax(newCfg.LegacyPlaybackMax)
	// Global TLS client cert / bind host need no push: dialConfigForLocked
	// resolves them fresh from s.cfg (updated above) on each Dial/Reconnect,
	// matching the "keeper reads TLS material fresh per dial, brain caches
	// nothing" invariant one level up (see dialConfigForLocked's doc comment) —
	// unlike internal/uplink.Uplink, which cached these in its own Config
	// and needed them explicitly pushed on rehash. Guarded on non-nil: a
	// handful of tests drive Rehash directly against a *Server built via
	// New (never through Run, so bootstrapKeeper never ran) to exercise
	// TLS/listen_addr/log behavior in isolation from the keeper entirely.
	if s.driver != nil {
		s.driver.SetMaxFloodQueue(newCfg.MaxFloodQueue)
	}
	for _, name := range names {
		if err := s.ReloadNetworkConfig(name); err != nil {
			s.log.Warn("rehash: network reload failed", "name", name, "err", err)
		}
	}

	if dl != nil && newCfg.ListenAddr != old.ListenAddr {
		tlsCfg := dl.TLSConfig()
		if tlsCfg == nil {
			tlsCfg = &tls.Config{
				GetCertificate: s.certs.GetCertificate,
				ClientAuth:     tls.RequestClientCert,
				MinVersion:     tls.VersionTLS12,
			}
		}
		newLn, err := tls.Listen("tcp", newCfg.ListenAddr, tlsCfg)
		if err != nil {
			s.mu.Lock()
			s.cfg.ListenAddr = old.ListenAddr
			s.mu.Unlock()
			if dl != nil {
				cfg := newCfg
				cfg.ListenAddr = old.ListenAddr
				dl.SetConfig(cfg)
			}
			return fmt.Errorf("rehash listen_addr %q: %w", newCfg.ListenAddr, err)
		}
		dl.ReplaceListener(newLn)
		s.log.Info("rehash listen_addr updated", "old", old.ListenAddr, "new", newCfg.ListenAddr)
	}

	s.log.Info("rehash complete", "tls_cert", newCfg.TLSCert, "networks", len(names))
	return nil
}

// ListenTLS is a helper for tests.
func ListenTLS(addr string, cfg *tls.Config) (net.Listener, error) {
	return tls.Listen("tcp", addr, cfg)
}
