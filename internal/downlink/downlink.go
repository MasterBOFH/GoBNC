// Package downlink accepts TLS clients, authenticates, and attaches to sessions.
package downlink

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/auth"
	"github.com/MasterBOFH/GoBNC/internal/caps"
	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/connio"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// AdvertisedCaps is the full potential set offered to clients (see internal/caps).
// Prefer Session.OfferedCaps() for the currently available list.
var AdvertisedCaps = caps.AllOffer()

// CapServerName is the source prefix on CAP replies to clients (same as
// session.ServerName, including why it's a dotted hostname and not bare
// "gobnc" — see that constant's doc comment).
const CapServerName = "gobnc.masterbofh.org"

// Manager looks up sessions by network name.
type Manager interface {
	Session(network string) (*session.Session, error)
}

// Listener is the TLS client listener.
type Listener struct {
	cfgMu   sync.RWMutex
	cfg     config.Config
	store   *store.Store
	mgr     Manager
	log     *slog.Logger
	tlsCfg  *tls.Config
	lnMu    sync.Mutex
	ln      net.Listener
	lnGen   uint64 // incremented on ReplaceListener
	idSeq   uint64
	active  atomic.Int64
	authSem chan struct{} // limits concurrent argon2 verifies
}

// maxAuthVerify is the max concurrent password hash checks.
const maxAuthVerify = 4

// NewListener creates a downlink listener.
func NewListener(cfg config.Config, st *store.Store, mgr Manager, tlsCfg *tls.Config, log *slog.Logger) *Listener {
	if log == nil {
		log = slog.Default()
	}
	return &Listener{
		cfg:     cfg,
		store:   st,
		mgr:     mgr,
		log:     log,
		tlsCfg:  tlsCfg,
		authSem: make(chan struct{}, maxAuthVerify),
	}
}

// SetConfig updates runtime auth/limit settings (e.g. after SIGHUP rehash).
func (l *Listener) SetConfig(cfg config.Config) {
	l.cfgMu.Lock()
	l.cfg = cfg
	l.cfgMu.Unlock()
}

func (l *Listener) config() config.Config {
	l.cfgMu.RLock()
	defer l.cfgMu.RUnlock()
	return l.cfg
}

func (l *Listener) maxClients() int {
	cfg := l.config()
	if cfg.MaxClients <= 0 {
		return config.DefaultMaxClients
	}
	return cfg.MaxClients
}

func (l *Listener) keepaliveIdle() time.Duration {
	cfg := l.config()
	if cfg.PingIdleSeconds > 0 {
		return time.Duration(cfg.PingIdleSeconds) * time.Second
	}
	return KeepaliveIdle
}

func (l *Listener) keepaliveGrace() time.Duration {
	cfg := l.config()
	if cfg.PingGraceSeconds > 0 {
		return time.Duration(cfg.PingGraceSeconds) * time.Second
	}
	return KeepaliveGrace
}

// TLSConfig returns the shared listener TLS config (GetCertificate is hot-swappable).
func (l *Listener) TLSConfig() *tls.Config {
	return l.tlsCfg
}

// Addr returns the current listen address, or nil if not listening.
func (l *Listener) Addr() net.Addr {
	l.lnMu.Lock()
	defer l.lnMu.Unlock()
	if l.ln == nil {
		return nil
	}
	return l.ln.Addr()
}

// ReplaceListener swaps the accept socket and closes the previous one.
// Existing accepted connections are unaffected. Serve continues on the new listener.
func (l *Listener) ReplaceListener(ln net.Listener) {
	if ln == nil {
		return
	}
	l.lnMu.Lock()
	old := l.ln
	l.ln = ln
	l.lnGen++
	l.lnMu.Unlock()
	if old != nil {
		_ = old.Close()
	}
}

// Serve accepts connections until ctx cancelled.
// ln may be replaced later via ReplaceListener without stopping Serve.
func (l *Listener) Serve(ctx context.Context, ln net.Listener) error {
	l.lnMu.Lock()
	l.ln = ln
	l.lnMu.Unlock()
	go func() {
		<-ctx.Done()
		l.lnMu.Lock()
		cur := l.ln
		l.lnMu.Unlock()
		if cur != nil {
			_ = cur.Close()
		}
	}()
	for {
		l.lnMu.Lock()
		cur := l.ln
		gen := l.lnGen
		l.lnMu.Unlock()
		if cur == nil {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(10 * time.Millisecond):
				continue
			}
		}
		c, err := cur.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			l.lnMu.Lock()
			swapped := l.lnGen != gen
			l.lnMu.Unlock()
			if swapped {
				continue
			}
			return err
		}
		if !l.tryAcquireClient() {
			l.log.Debug("max clients reached; closing connection")
			_ = c.Close()
			continue
		}
		go l.handle(ctx, c)
	}
}

func (l *Listener) tryAcquireClient() bool {
	max := int64(l.maxClients())
	for {
		cur := l.active.Load()
		if cur >= max {
			return false
		}
		if l.active.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (l *Listener) handle(ctx context.Context, c net.Conn) {
	defer l.active.Add(-1)
	_ = c.SetDeadline(time.Now().Add(60 * time.Second))

	tc, ok := c.(*tls.Conn)
	if !ok {
		// Require TLS — if plain slipped through, handshake as TLS
		tc = tls.Server(c, l.tlsCfg)
		if err := tc.Handshake(); err != nil {
			l.log.Debug("tls handshake failed", "err", err)
			_ = c.Close()
			return
		}
	} else if err := tc.Handshake(); err != nil {
		l.log.Debug("tls handshake failed", "err", err)
		_ = c.Close()
		return
	}

	cl := &Client{
		id:       session.ClientID(fmt.Sprintf("c%d", atomic.AddUint64(&l.idSeq, 1))),
		conn:     tc,
		r:        bufio.NewReaderSize(tc, irc.MaxClientLine),
		caps:     make(map[string]bool),
		capsSeen: make(map[string]bool),
		log:      l.log,
	}
	// From here on, cl (not the raw conn) owns connection teardown: Close
	// stops new sends but lets writeLoop flush whatever's already queued
	// (e.g. an ERROR sent right before an auth-failure return below) before
	// it closes the actual connection — see writeLoop's own doc comment. A
	// bare defer c.Close() here would race that flush and could drop the
	// queued message entirely.
	defer cl.Close()

	// Cert-only: reject unknown/missing client certs before any IRC (CAP/PASS/…).
	cfg := l.config()
	peerFP, presentedCert := peerCertFingerprint(tc)
	if cfg.AllowCertAuth && !cfg.AllowPasswordAuth {
		if err := l.checkCertOnlyFingerprint(ctx, presentedCert, peerFP); err != nil {
			l.log.Info("auth failed", "ip", peerIP(c), "err", err)
			_ = cl.Send(irc.Message{Command: "ERROR", Params: []string{authFailedText(peerFP)}})
			return
		}
	}

	authed, network, err := l.authenticate(ctx, cl, tc)
	if err != nil || !authed {
		l.log.Info("auth failed", "ip", peerIP(c), "err", err)
		_ = cl.Send(irc.Message{Command: "ERROR", Params: []string{authFailedText(peerFP)}})
		return
	}
	_ = c.SetDeadline(time.Time{})

	sess, err := l.mgr.Session(network)
	if err != nil {
		l.log.Info("auth failed", "ip", peerIP(c), "network", network, "err", "unknown network")
		_ = cl.Send(irc.Message{Command: "ERROR", Params: []string{"unknown network: " + network}})
		return
	}
	cl.sess = sess
	if err := sess.Attach(cl); err != nil {
		return
	}
	defer sess.Detach(cl.ID())

	idle := l.keepaliveIdle()
	grace := l.keepaliveGrace()
	kaCtx, kaCancel := context.WithCancel(ctx)
	defer kaCancel()
	cl.touch()
	go cl.keepaliveLoop(kaCtx, idle, grace)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		line, err := cl.readLine(runtimeReadTimeout(idle, grace))
		if err != nil {
			if errors.Is(err, irc.ErrLineTooLong) {
				l.log.Warn("downlink line too long", "client", cl.id, "phase", "runtime")
				_ = cl.Send(irc.InputTooLong(cl.inputNick()))
				continue
			}
			l.log.Debug("downlink client disconnected", "client", cl.id, "err", err, "idle", time.Since(cl.lastRX()))
			return
		}
		cl.touch()
		msg, err := irc.Parse(line)
		if err != nil {
			l.log.Warn("parse error", "client", cl.id, "phase", "runtime", "line", gobnclog.RedactIRC(line), "err", err)
			continue
		}
		if err := l.dispatch(cl, sess, msg); err != nil {
			l.log.Debug("client msg error", "err", err)
		}
	}
}

func (l *Listener) authenticate(ctx context.Context, cl *Client, tc *tls.Conn) (bool, string, error) {
	var pass, nick string
	gotUser := false
	deadline := time.Now().Add(30 * time.Second)

	for time.Now().Before(deadline) {
		line, err := cl.readLine(time.Until(deadline))
		if err != nil {
			if errors.Is(err, irc.ErrLineTooLong) {
				l.log.Warn("downlink line too long", "client", cl.id, "phase", "auth")
				_ = cl.Send(irc.InputTooLong(cl.inputNick()))
				continue
			}
			return false, "", err
		}
		msg, err := irc.Parse(line)
		if err != nil {
			l.log.Warn("parse error", "client", cl.id, "phase", "auth", "line", gobnclog.RedactIRC(line), "err", err)
			continue
		}
		switch strings.ToUpper(msg.Command) {
		case "CAP":
			_ = handleClientCAP(cl, msg)
		case "PASS":
			pass = msg.ParamsText()
			// Resolve the network as soon as we know it so a CAP LS received
			// before PASS can be answered with one collated list (including
			// any uplink-backed caps already available) instead of CAP LS
			// followed later by CAP NEW.
			if net := networkFromPass(pass); net != "" {
				if sess, err := l.mgr.Session(net); err == nil {
					cl.provSess = sess
				}
			}
			if cl.pendingLS {
				cl.pendingLS = false
				_ = cl.sendCapLSReply()
			}
		case "NICK":
			nick = msg.Param(0)
			cl.nick = nick
		case "USER":
			gotUser = true
		case "QUIT":
			return false, "", fmt.Errorf("quit")
		}
		// NICK, USER, and CAP END may arrive in any order.
		if registrationReady(nick, gotUser, cl.capStarted, cl.capEnded, cl.pendingLS) {
			break
		}
	}
	if !registrationReady(nick, gotUser, cl.capStarted, cl.capEnded, cl.pendingLS) {
		if nick == "" || !gotUser {
			return false, "", fmt.Errorf("registration incomplete")
		}
		if cl.pendingLS {
			return false, "", fmt.Errorf("CAP LS not resolved (missing PASS)")
		}
		return false, "", fmt.Errorf("CAP negotiation incomplete (missing CAP END)")
	}

	fpOK := false
	cfg := l.config()
	peerFP, presentedCert := peerCertFingerprint(tc)
	if cfg.AllowCertAuth && presentedCert {
		ok, err := l.store.HasFingerprint(ctx, peerFP)
		if err != nil {
			return false, "", err
		}
		fpOK = ok
	}
	passOK := false
	passSecret := stripNetworkFromPass(pass)
	triedPassword := cfg.AllowPasswordAuth && passSecret != ""
	if triedPassword {
		hash, err := l.store.PasswordHash(ctx)
		if err != nil {
			return false, "", err
		}
		if hash != "" {
			l.authSem <- struct{}{}
			passOK = auth.VerifyPassword(hash, passSecret)
			<-l.authSem
		}
	}
	if !fpOK && !passOK {
		switch {
		case presentedCert && triedPassword:
			return false, "", fmt.Errorf("invalid password and fingerprint")
		case presentedCert:
			return false, "", fmt.Errorf("invalid fingerprint")
		case triedPassword:
			return false, "", fmt.Errorf("invalid password")
		default:
			return false, "", fmt.Errorf("no valid credentials")
		}
	}

	network := networkFromPass(pass)
	if network == "" {
		return false, "", fmt.Errorf("network not specified (use PASS network/password or PASS network/ for cert auth)")
	}
	return true, network, nil
}

// peerIP returns the remote host without port (for auth failure logs).
func peerIP(c net.Conn) string {
	if c == nil || c.RemoteAddr() == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(c.RemoteAddr().String())
	if err != nil {
		return c.RemoteAddr().String()
	}
	return host
}

// peerCertFingerprint returns the SHA-256 hex of the leaf client cert, if any.
func peerCertFingerprint(tc *tls.Conn) (fp string, presented bool) {
	if tc == nil {
		return "", false
	}
	state := tc.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return "", false
	}
	sum := sha256.Sum256(state.PeerCertificates[0].Raw)
	return hex.EncodeToString(sum[:]), true
}

// authFailedText is the downlink ERROR text on auth failure.
// When a client cert was presented, include its fingerprint so a legitimate
// user can register it (cert add).
func authFailedText(fp string) string {
	if fp == "" {
		return "Authentication failed"
	}
	return "Authentication failed (cert fingerprint: " + fp + ")"
}

// checkCertOnlyFingerprint enforces a known client cert when password auth is off.
func (l *Listener) checkCertOnlyFingerprint(ctx context.Context, presented bool, fp string) error {
	if !presented {
		return fmt.Errorf("client certificate required")
	}
	ok, err := l.store.HasFingerprint(ctx, fp)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("invalid fingerprint")
	}
	return nil
}

// stripNetworkFromPass returns the password portion of PASS.
// Format: "network/password" (password may contain '/'); "network/" or "network" for empty password.
func stripNetworkFromPass(pass string) string {
	i := strings.Index(pass, "/")
	if i < 0 {
		return ""
	}
	return pass[i+1:]
}

// networkFromPass extracts the network name from PASS (before the first '/').
// "network/password", "network/", or bare "network" (cert-only).
func networkFromPass(pass string) string {
	if pass == "" {
		return ""
	}
	i := strings.Index(pass, "/")
	if i < 0 {
		return pass
	}
	return pass[:i]
}

// registrationReady is true when NICK and USER are set, CAP END has been
// received if the client started CAP negotiation (CAP LS / CAP REQ), and any
// CAP LS received before the network was known (via PASS) has been answered.
// Order of NICK, USER, and CAP END does not matter.
func registrationReady(nick string, gotUser, capStarted, capEnded, lsPending bool) bool {
	if nick == "" || !gotUser {
		return false
	}
	if capStarted && !capEnded {
		return false
	}
	if lsPending {
		return false
	}
	return true
}

func handleClientCAP(cl *Client, msg irc.Message) error {
	sub := strings.ToUpper(msg.Param(0))
	switch sub {
	case "LS":
		cl.capStarted = true
		if msg.Param(1) == "302" {
			cl.cap302 = true
		}
		if cl.sess == nil && cl.provSess == nil {
			// Network not resolved yet (PASS not seen). Defer the reply so we
			// can answer with one collated list — instead of a bare CAP LS now
			// followed by a separate CAP NEW once the uplink's caps are known.
			cl.pendingLS = true
			return nil
		}
		return cl.sendCapLSReply()
	case "REQ":
		cl.capStarted = true
		offered := cl.offeredCaps()
		offeredSet := make(map[string]bool, len(offered))
		for _, c := range offered {
			offeredSet[caps.CapName(c)] = true
		}
		req := strings.Fields(msg.Trailing())
		if len(req) == 0 {
			req = strings.Fields(msg.Param(1))
		}
		var ack []string
		wantSASL := false
		for _, c := range req {
			name := caps.CapName(c)
			if name == "sasl" {
				// Passthrough SASL only works once the session is attached
				// (RequestClientSASL needs the real uplink); never fall
				// through to the generic offered-set ACK below.
				if cl.sess != nil && cl.sess.OffersPassthroughSASL() {
					wantSASL = true
				}
				continue
			}
			if offeredSet[name] {
				cl.mu.Lock()
				cl.caps[name] = true
				cl.mu.Unlock()
				ack = append(ack, name)
			}
		}
		if len(ack) > 0 {
			if err := cl.Send(irc.Message{
				Source:  CapServerName,
				Command: "CAP",
				Params:  []string{"*", "ACK", strings.Join(ack, " ")},
			}); err != nil {
				return err
			}
		}
		if wantSASL {
			return cl.sess.RequestClientSASL(cl)
		}
		return nil
	case "LIST":
		return cl.sendCapListReply()
	case "END":
		cl.capEnded = true
		return nil
	}
	return nil
}

func (l *Listener) dispatch(cl *Client, sess *session.Session, msg irc.Message) error {
	switch strings.ToUpper(msg.Command) {
	case "CAP":
		return handleClientCAP(cl, msg)
	default:
		return sess.HandleClientMessage(cl, msg)
	}
}

// Client is a connected downlink.
type Client struct {
	id       session.ClientID
	conn     net.Conn
	r        *bufio.Reader
	mu       sync.Mutex
	caps     map[string]bool
	capsSeen map[string]bool // cap names already advertised via CAP LS/NEW (avoids duplicate CAP NEW)
	sess     *session.Session
	// provSess is the network's session resolved from PASS, before the client
	// has finished authenticating/attaching. Used only to compute the current
	// capability offer for CAP LS/REQ; never used for message delivery.
	provSess   *session.Session
	nick       string // registration nick (before/without session)
	log        *slog.Logger
	wmu        sync.Mutex
	capStarted bool // client sent CAP LS or CAP REQ
	capEnded   bool // client sent CAP END
	cap302     bool // client sent CAP LS 302
	pendingLS  bool // CAP LS received before the network (PASS) was known
	lastRXUnix int64

	// outOnce lazily starts out/writeLoop on first Send/Close, rather than
	// requiring construction through a constructor — several tests build
	// &Client{} via a bare struct literal and must keep working unmodified.
	outOnce sync.Once
	// out is this client's own outbound queue, drained by writeLoop. Send
	// only ever enqueues here — it never performs the actual network
	// write — so a downlink client too slow to drain its own TCP window
	// can never block the demux goroutine that services every network's
	// line feed (see docs/keeper-design.md and this fix's own commit
	// message for why that matters: a slow *client* must never be able to
	// stall the keeper<->brain link, which acks a line once the brain has
	// seen it, not once it's been delivered downstream).
	out chan irc.Message
	// closed guards out/conn teardown, both under wmu, so Send racing a
	// concurrent Close (e.g. keepaliveLoop's timeout firing mid-Send)
	// can never send on or double-close out.
	closed bool
}

// Keepalive timing (overridable in tests).
//
// 300s/120s, not the old 120s/60s: a 180s total budget was tight enough
// that a client sending its own periodic keepalive ping (e.g. "PING
// :TIMEOUTCHECK") on a real-world interval of 3-5 minutes — comfortably
// under what a typical ircd tolerates — could still get closed by gobnc
// itself between two of its own pings, even though every ping it did send
// was answered. A 420s total budget gives room for a slower client-side
// interval while still catching genuinely dead connections well within
// what any real ircd would.
var (
	KeepaliveIdle  = 300 * time.Second
	KeepaliveGrace = 120 * time.Second
)

// runtimeReadTimeout bounds how long the post-attach loop blocks waiting for
// a client line. It must stay comfortably above KeepaliveIdle+KeepaliveGrace
// so keepaliveLoop — which pings an idle client and grants it a grace period,
// logging why it gave up — is always the one to close a genuinely idle
// connection. A shorter value here would silently race and preempt that
// grace period with a bare, unlogged read timeout.
func runtimeReadTimeout(idle, grace time.Duration) time.Duration {
	return idle + grace + 30*time.Second
}

func (c *Client) touch() {
	atomic.StoreInt64(&c.lastRXUnix, time.Now().UnixNano())
}

func (c *Client) lastRX() time.Time {
	ns := atomic.LoadInt64(&c.lastRXUnix)
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func (c *Client) keepaliveLoop(ctx context.Context, idle, grace time.Duration) {
	if idle <= 0 {
		return
	}
	tick := idle / 4
	if tick < 50*time.Millisecond {
		tick = 50 * time.Millisecond
	}
	if tick > 10*time.Second {
		tick = 10 * time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	var pingAt time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		last := c.lastRX()
		if last.IsZero() {
			continue
		}
		silent := time.Since(last)
		if silent < idle {
			pingAt = time.Time{}
			continue
		}
		if pingAt.IsZero() {
			_ = c.Send(irc.Message{Command: "PING", Params: []string{"gobnc"}})
			pingAt = time.Now()
			continue
		}
		if grace > 0 && time.Since(pingAt) >= grace && time.Since(last) >= idle {
			c.log.Info("downlink keepalive timeout; closing", "client", c.id)
			_ = c.Close()
			return
		}
	}
}

func (c *Client) ID() session.ClientID { return c.id }

// RemoteAddr returns the client's peer IP (no port), for connect-notice
// broadcasts. Empty if conn is nil (bare struct literal in tests).
func (c *Client) RemoteAddr() string { return peerIP(c.conn) }

func (c *Client) Caps() map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]bool, len(c.caps))
	for k, v := range c.caps {
		out[k] = v
	}
	return out
}

func (c *Client) HasCap(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.caps[name]
}

func (c *Client) ClearCap(name string) {
	c.mu.Lock()
	delete(c.caps, name)
	c.mu.Unlock()
}

// EnableCap marks a capability as negotiated (used for async CAP ACK, e.g. sasl).
func (c *Client) EnableCap(name string) {
	c.mu.Lock()
	c.caps[name] = true
	c.mu.Unlock()
}

// HasSeenCap reports whether name was already advertised to this client via
// CAP LS or CAP NEW (see session.notifyAttachCaps, which uses this to avoid
// re-announcing caps the client already learned about).
func (c *Client) HasSeenCap(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capsSeen[name]
}

// MarkSeenCap records that name was advertised to this client.
func (c *Client) MarkSeenCap(name string) {
	c.mu.Lock()
	if c.capsSeen == nil {
		c.capsSeen = make(map[string]bool)
	}
	c.capsSeen[name] = true
	c.mu.Unlock()
}

func (c *Client) markCapsSeen(list []string) {
	c.mu.Lock()
	if c.capsSeen == nil {
		c.capsSeen = make(map[string]bool)
	}
	for _, n := range list {
		c.capsSeen[caps.CapName(n)] = true
	}
	c.mu.Unlock()
}

// offeredCaps returns the capability set currently offered to this client:
// the attached session's live offer, the provisionally resolved session's
// offer (network known via PASS, pre-attach) with sasl stripped since
// passthrough SASL isn't wired up until the session is attached, or the
// bouncer-local always-on set if neither is known yet.
func (c *Client) offeredCaps() []string {
	if c.sess != nil {
		return c.sess.OfferedCaps()
	}
	if c.provSess != nil {
		return withoutSASL(c.provSess.OfferedCaps())
	}
	return caps.AlwaysOffer
}

func withoutSASL(list []string) []string {
	out := make([]string, 0, len(list))
	for _, c := range list {
		if caps.CapName(c) != "sasl" {
			out = append(out, c)
		}
	}
	return out
}

// sendCapLSReply answers CAP LS with the current offer (see offeredCaps).
// CAP LS 302 implies cap-notify support without an explicit CAP REQ (IRCv3).
func (c *Client) sendCapLSReply() error {
	offered := c.offeredCaps()
	list := offered
	if !c.cap302 {
		list = caps.WithoutValues(offered)
	}
	c.markCapsSeen(offered)
	if c.cap302 {
		c.EnableCap("cap-notify")
	}
	return c.Send(irc.Message{
		Source:  CapServerName,
		Command: "CAP",
		Params:  []string{"*", "LS", strings.Join(list, " ")},
	})
}

// sendCapListReply answers CAP LIST with the capabilities currently
// negotiated (enabled) for this client.
func (c *Client) sendCapListReply() error {
	c.mu.Lock()
	names := make([]string, 0, len(c.caps))
	for name, on := range c.caps {
		if on {
			names = append(names, name)
		}
	}
	c.mu.Unlock()
	sort.Strings(names)
	return c.Send(irc.Message{
		Source:  CapServerName,
		Command: "CAP",
		Params:  []string{"*", "LIST", strings.Join(names, " ")},
	})
}

// DownlinkOutQueueSize bounds how far a downlink client's own outbound
// queue may lag before it's judged too slow to keep and disconnected —
// sized to absorb a large channel's WHO/NAMES reply burst (thousands of
// lines) without a normal client ever tripping it. 16384 comfortably
// covers even the largest real-world channels (several thousand members)
// with headroom for other concurrent traffic during the burst. Worst-case
// memory per client stays bounded even so: irc.MaxServerLine (8703 bytes —
// the largest a single line can be, full combined IRCv3 tag section plus
// a 512-byte message) times this gives ~136MiB per fully-backed-up
// client, times config.DefaultMaxClients (32) gives a worst-case ceiling
// in the low GiBs — never reached in practice, since real traffic is far
// smaller than max-tag-size on every line at once. Overridable in tests.
var DownlinkOutQueueSize = 16384

// downlinkWriteTimeout bounds how long writeLoop may block on one write to
// a client that has stopped reading entirely — without this, a genuinely
// dead/wedged client would keep its writer goroutine (and its fd) alive
// forever. It must stay comfortably larger than any real momentary stall:
// DownlinkOutQueueSize is what absorbs ordinary burst-vs-drain-rate
// mismatches; this deadline only exists to eventually reclaim resources
// from a connection that will never read again. Overridable in tests.
var downlinkWriteTimeout = 30 * time.Second

func (c *Client) ensureWriter() {
	c.outOnce.Do(func() {
		c.out = make(chan irc.Message, DownlinkOutQueueSize)
		go c.writeLoop()
	})
}

// writeLoop is the one goroutine that ever writes to c.conn, draining out
// in order — Send/Close only ever enqueue or close(out), this does the
// actual (potentially slow) network write, off whatever goroutine called
// Send (normally the shared demux — see out's own doc comment). It, and
// only it, closes c.conn — once out is fully drained (normal shutdown) or
// a write fails/times out (dead client) — so a message enqueued right
// before a Send/Close (e.g. a final ERROR before disconnecting a client)
// is never dropped by an immediate close racing its own delivery.
func (c *Client) writeLoop() {
	for msg := range c.out {
		raw := msg.Wire()
		// A /bnc debug relay message must never be logged as ordinary outgoing
		// traffic — a raw/all-mode subscriber would otherwise see it fed right
		// back into the same subscription and relay again, forever (see
		// session.DebugSource's doc comment for how this was actually found).
		if msg.Source != session.DebugSource {
			gobnclog.IRC(c.log, c.logPeer(), ">>", raw)
		}
		_ = c.conn.SetWriteDeadline(time.Now().Add(downlinkWriteTimeout))
		if _, err := io.WriteString(c.conn, raw+"\r\n"); err != nil {
			c.log.Debug("downlink write failed; closing", "client", c.id, "err", err)
			break
		}
	}
	_ = c.conn.Close()
}

func (c *Client) Send(msg irc.Message) error {
	c.ensureWriter()
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	select {
	case c.out <- msg:
		return nil
	default:
		// This client's own queue is full — it's too slow to keep up with
		// its own traffic. Drop it, on its own: never the shared keeper
		// attach, never any other client or network sharing the demux.
		// writeLoop finishes flushing whatever was already queued (best
		// effort) and closes the connection itself once it does.
		c.log.Warn("downlink client too slow, buffer overflow; disconnecting", "client", c.id)
		c.closed = true
		close(c.out)
		return fmt.Errorf("downlink client too slow, buffer overflow")
	}
}

func (c *Client) Close() error {
	c.ensureWriter()
	c.wmu.Lock()
	if !c.closed {
		c.closed = true
		close(c.out)
	}
	c.wmu.Unlock()
	return nil
}

func (c *Client) inputNick() string {
	if c.sess != nil {
		if n := c.sess.Nick(); n != "" {
			return n
		}
	}
	if c.nick != "" {
		return c.nick
	}
	return "*"
}

func (c *Client) readLine(timeout time.Duration) (string, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(timeout))
	line, err := connio.ReadLimitedLine(c.r, irc.MaxClientLine)
	if err != nil {
		return "", err
	}
	gobnclog.IRC(c.log, c.logPeer(), "<<", line)
	return line, nil
}

// logPeer is network/clientID once attached (e.g. "Undernet/c1"), else downlink/id.
func (c *Client) logPeer() string {
	if c.sess != nil {
		return c.sess.Name() + "/" + string(c.id)
	}
	return "downlink/" + string(c.id)
}

// Cleartext rejected helper for tests: dial plain should fail handshake when only TLS listener.
func FingerprintSHA256(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
