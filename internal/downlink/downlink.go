// Package downlink accepts TLS clients, authenticates, and attaches to sessions.
package downlink

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/auth"
	"github.com/MasterBOFH/GoBNC/internal/caps"
	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// AdvertisedCaps is the full potential set offered to clients (see internal/caps).
// Prefer Session.OfferedCaps() for the currently available list.
var AdvertisedCaps = caps.AllOffer()

// CapServerName is the source prefix on CAP replies to clients (same as session.ServerName).
const CapServerName = "gobnc"

// Manager looks up sessions by network name.
type Manager interface {
	Session(network string) (*session.Session, error)
}

// Listener is the TLS client listener.
type Listener struct {
	cfg    config.Config
	store  *store.Store
	mgr    Manager
	log    *slog.Logger
	tlsCfg *tls.Config
	ln     net.Listener
	idSeq  uint64
}

// NewListener creates a downlink listener.
func NewListener(cfg config.Config, st *store.Store, mgr Manager, tlsCfg *tls.Config, log *slog.Logger) *Listener {
	if log == nil {
		log = slog.Default()
	}
	return &Listener{cfg: cfg, store: st, mgr: mgr, log: log, tlsCfg: tlsCfg}
}

// Serve accepts connections until ctx cancelled.
func (l *Listener) Serve(ctx context.Context, ln net.Listener) error {
	l.ln = ln
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}
		go l.handle(ctx, c)
	}
}

func (l *Listener) handle(ctx context.Context, c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(60 * time.Second))

	tc, ok := c.(*tls.Conn)
	if !ok {
		// Require TLS — if plain slipped through, handshake as TLS
		tc = tls.Server(c, l.tlsCfg)
		if err := tc.Handshake(); err != nil {
			l.log.Debug("tls handshake failed", "err", err)
			return
		}
	} else if err := tc.Handshake(); err != nil {
		l.log.Debug("tls handshake failed", "err", err)
		return
	}

	cl := &Client{
		id:   session.ClientID(fmt.Sprintf("c%d", atomic.AddUint64(&l.idSeq, 1))),
		conn: tc,
		r:    bufio.NewReader(tc),
		caps: make(map[string]bool),
		log:  l.log,
	}

	authed, network, err := l.authenticate(ctx, cl, tc)
	if err != nil || !authed {
		l.log.Info("auth failed", "err", err)
		_ = cl.Send(irc.Message{Command: "ERROR", Params: []string{"Authentication failed"}})
		return
	}
	_ = c.SetDeadline(time.Time{})

	sess, err := l.mgr.Session(network)
	if err != nil {
		_ = cl.Send(irc.Message{Command: "ERROR", Params: []string{"unknown network: " + network}})
		return
	}
	cl.sess = sess
	if err := sess.Attach(cl); err != nil {
		return
	}
	defer sess.Detach(cl.ID())

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		line, err := cl.readLine(5 * time.Minute)
		if err != nil {
			return
		}
		msg, err := irc.Parse(line)
		if err != nil {
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
			return false, "", err
		}
		msg, err := irc.Parse(line)
		if err != nil {
			continue
		}
		switch strings.ToUpper(msg.Command) {
		case "CAP":
			_ = handleClientCAP(cl, msg)
		case "PASS":
			pass = msg.Trailing()
			if pass == "" {
				pass = msg.Param(0)
			}
		case "NICK":
			nick = msg.Param(0)
		case "USER":
			gotUser = true
		case "QUIT":
			return false, "", fmt.Errorf("quit")
		}
		// NICK, USER, and CAP END may arrive in any order.
		if registrationReady(nick, gotUser, cl.capStarted, cl.capEnded) {
			break
		}
	}
	if !registrationReady(nick, gotUser, cl.capStarted, cl.capEnded) {
		if nick == "" || !gotUser {
			return false, "", fmt.Errorf("registration incomplete")
		}
		return false, "", fmt.Errorf("CAP negotiation incomplete (missing CAP END)")
	}

	fpOK := false
	if l.cfg.AllowCertAuth {
		if state := tc.ConnectionState(); len(state.PeerCertificates) > 0 {
			sum := sha256.Sum256(state.PeerCertificates[0].Raw)
			fp := hex.EncodeToString(sum[:])
			ok, err := l.store.HasFingerprint(ctx, fp)
			if err != nil {
				return false, "", err
			}
			fpOK = ok
		}
	}
	passOK := false
	passSecret := stripNetworkFromPass(pass)
	if l.cfg.AllowPasswordAuth && passSecret != "" {
		hash, err := l.store.PasswordHash(ctx)
		if err != nil {
			return false, "", err
		}
		if hash != "" && auth.VerifyPassword(hash, passSecret) {
			passOK = true
		}
	}
	if !fpOK && !passOK {
		return false, "", fmt.Errorf("no valid credentials")
	}

	network := networkFromPass(pass)
	if network == "" {
		return false, "", fmt.Errorf("network not specified (use PASS network/password or PASS network/ for cert auth)")
	}
	return true, network, nil
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

// registrationReady is true when NICK and USER are set, and CAP END has been
// received if the client started CAP negotiation (CAP LS / CAP REQ).
// Order of NICK, USER, and CAP END does not matter.
func registrationReady(nick string, gotUser, capStarted, capEnded bool) bool {
	if nick == "" || !gotUser {
		return false
	}
	if capStarted && !capEnded {
		return false
	}
	return true
}

func handleClientCAP(cl *Client, msg irc.Message) error {
	sub := strings.ToUpper(msg.Param(0))
	offered := caps.AlwaysOffer
	if cl.sess != nil {
		offered = cl.sess.OfferedCaps()
	}
	offeredSet := make(map[string]bool, len(offered))
	for _, c := range offered {
		offeredSet[caps.CapName(c)] = true
	}
	switch sub {
	case "LS":
		cl.capStarted = true
		if msg.Param(1) == "302" {
			cl.cap302 = true
		}
		list := offered
		if !cl.cap302 {
			list = caps.WithoutValues(offered)
		}
		return cl.Send(irc.Message{
			Source:  CapServerName,
			Command: "CAP",
			Params:  []string{"*", "LS", strings.Join(list, " ")},
		})
	case "REQ":
		cl.capStarted = true
		req := strings.Fields(msg.Trailing())
		if len(req) == 0 {
			req = strings.Fields(msg.Param(1))
		}
		var ack []string
		wantSASL := false
		for _, c := range req {
			name := caps.CapName(c)
			if name == "sasl" && cl.sess != nil && cl.sess.OffersPassthroughSASL() {
				wantSASL = true
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
	id         session.ClientID
	conn       net.Conn
	r          *bufio.Reader
	mu         sync.Mutex
	caps       map[string]bool
	sess       *session.Session
	log        *slog.Logger
	wmu        sync.Mutex
	capStarted bool // client sent CAP LS or CAP REQ
	capEnded   bool // client sent CAP END
	cap302     bool // client sent CAP LS 302
}

func (c *Client) ID() session.ClientID { return c.id }

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

func (c *Client) Send(msg irc.Message) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	raw := msg.Wire()
	gobnclog.IRC(c.log, c.logPeer(), ">>", raw)
	_, err := io.WriteString(c.conn, raw+"\r\n")
	return err
}

func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) readLine(timeout time.Duration) (string, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(timeout))
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
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
