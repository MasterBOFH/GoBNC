// Package uplink maintains a persistent connection to an IRC network.
package uplink

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/MasterBOFH/GoBNC/internal/connio"
	"github.com/MasterBOFH/GoBNC/internal/flood"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/xdg-go/scram"
)

var (
	errNotConnected = fmt.Errorf("not connected")
	errInvalidUTF8  = fmt.Errorf("invalid UTF-8 under UTF8ONLY")
)

// DesiredCaps requested when available.
var DesiredCaps = []string{
	"cap-notify",
	"message-tags",
	"server-time",
	"batch",
	"labeled-response",
	"account-tag",
	"account-notify",
	"extended-join",
	"echo-message",
	"away-notify",
	"chghost",
	"invite-notify",
	"sasl",
	"chathistory",
	"draft/chathistory",
}

// Config for an uplink connection.
type Config struct {
	Network  store.Network
	Channels []store.Channel
	Dial     func(ctx context.Context, network, addr string) (net.Conn, error) // optional override
	TLSConf  *tls.Config
	Logger   *slog.Logger
	// MaxFloodQueue caps paced outbound queue depth (0 = unlimited).
	MaxFloodQueue int
	// Backoff for reconnect tests
	MinBackoff time.Duration
	MaxBackoff time.Duration
}

// Handler receives registered events from the uplink.
type Handler interface {
	OnRegistered(u *Uplink)
	OnMessage(u *Uplink, msg irc.Message)
	OnDisconnect(u *Uplink, err error)
	// OnRegistrationLine is called for client-visible lines during register()
	// (before Registered is true), e.g. NOTICE AUTH and welcome numerics.
	OnRegistrationLine(u *Uplink, msg irc.Message)
	// OnCapsChanged is called after registration when uplink caps are ACK'd or DEL'd.
	OnCapsChanged(u *Uplink, added, removed []string)
	// OnSASLOffer is called when uplink advertises or withdraws sasl availability
	// without the bouncer necessarily having REQed it (passthrough mode).
	OnSASLOffer(u *Uplink, available bool)
	// OnCapNAK is called after registration when a CAP REQ is rejected.
	OnCapNAK(u *Uplink, names []string)
}

// Uplink is a live IRC server connection.
type Uplink struct {
	cfg Config
	log *slog.Logger

	mu       sync.RWMutex
	conn     *connio.Conn
	nick     string
	isupport *irc.ISUPPORT
	caps          map[string]bool
	saslAvailable bool     // seen in CAP LS/NEW (not necessarily ACK'd)
	saslMechs     []string // from CAP 302 sasl=PLAIN,EXTERNAL (empty = unspecified)
	saslMech      string   // mechanism in progress (bouncer-owned SASL)
	scramConv     *scram.ClientConversation
	account       string // services account from RPL_LOGGEDIN (900)
	reg           bool
	// Welcome numerics from uplink (params after nick).
	rpl002 []string
	rpl003 []string
	rpl004 []string
	umodes map[byte]bool

	handler Handler

	writeMu sync.Mutex

	flood     *flood.ByteBucket
	floodMu   sync.Mutex
	floodQ    []string
	floodWake chan struct{}
	floodStop chan struct{}

	lastRXUnix int64 // atomic unix nano of last inbound line

	nickRecMu               sync.Mutex
	nickRecStop             chan struct{}
	isonPending             bool
	nickRecoveryUserStopped bool
}

// New creates an uplink (not yet connected).
func New(cfg Config, h Handler) *Uplink {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	if cfg.MinBackoff == 0 {
		cfg.MinBackoff = time.Second
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = 60 * time.Second
	}
	u := &Uplink{
		cfg:      cfg,
		log:      log,
		handler:  h,
		nick:     cfg.Network.Nick,
		isupport: irc.NewISUPPORT(),
		caps:     make(map[string]bool),
		umodes:   make(map[byte]bool),
	}
	u.initFlood()
	return u
}

// Account returns the services account from the last RPL_LOGGEDIN (900), or "".
func (u *Uplink) Account() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.account
}

// Nick returns the current nick.
func (u *Uplink) Nick() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.nick
}

// ISUPPORT returns negotiated ISUPPORT.
func (u *Uplink) ISUPPORT() *irc.ISUPPORT {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.isupport
}

// Caps returns negotiated upstream caps.
func (u *Uplink) Caps() map[string]bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	out := make(map[string]bool, len(u.caps))
	for k, v := range u.caps {
		out[k] = v
	}
	return out
}

// SetChannels updates the auto-join list used on (re)connect.
func (u *Uplink) SetChannels(chs []store.Channel) {
	u.mu.Lock()
	u.cfg.Channels = append([]store.Channel(nil), chs...)
	u.mu.Unlock()
}

// SetNetwork updates dial/register settings used on the next (re)connect.
// The current connection is left open. Flood pacing applies immediately.
func (u *Uplink) SetNetwork(n store.Network) {
	u.mu.Lock()
	u.cfg.Network = n
	u.mu.Unlock()
	u.setFloodParams(n.FloodBurst, n.FloodRate)
	if !n.NickRecovery {
		u.stopNickRecovery()
	} else {
		u.maybeStartNickRecovery()
	}
}

// SetMaxFloodQueue updates the paced send-queue depth cap (0 = unlimited).
func (u *Uplink) SetMaxFloodQueue(n int) {
	u.mu.Lock()
	u.cfg.MaxFloodQueue = n
	u.mu.Unlock()
}

// HasCap reports whether a capability is enabled.
func (u *Uplink) HasCap(name string) bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.caps[name]
}

// Registered reports welcome received.
func (u *Uplink) Registered() bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.reg
}

// Welcome returns stored 002/003/004 parameter tails (after nick), copying slices.
func (u *Uplink) Welcome() (rpl002, rpl003, rpl004 []string) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return append([]string(nil), u.rpl002...), append([]string(nil), u.rpl003...), append([]string(nil), u.rpl004...)
}

// UserModes returns the current usermode string (e.g. "+iw"), or empty.
func (u *Uplink) UserModes() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return irc.UserModeString(u.umodes)
}

// WriteMessage encodes and sends a message (flood-paced when configured).
func (u *Uplink) WriteMessage(msg irc.Message) error {
	return u.WriteRaw(msg.Encode())
}

// WriteRaw queues a raw line for flood-paced send when pacing is enabled;
// otherwise writes immediately. Returns an error if MaxFloodQueue is set and full.
func (u *Uplink) WriteRaw(line string) error {
	if u.floodEnabled() {
		return u.enqueueFlood(line)
	}
	return u.writeImmediate(line)
}

// writeImmediate sends a line now, bypassing the flood queue and bucket.
// Used for PONG (and when flood pacing is disabled).
func (u *Uplink) writeImmediate(line string) error {
	if u.utf8Only() && !utf8.ValidString(line) {
		u.log.Warn("refusing non-UTF-8 uplink write under UTF8ONLY")
		return errInvalidUTF8
	}
	u.writeMu.Lock()
	defer u.writeMu.Unlock()
	u.mu.RLock()
	c := u.conn
	u.mu.RUnlock()
	if c == nil {
		return errNotConnected
	}
	return c.WriteLine(line)
}

func (u *Uplink) utf8Only() bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.isupport != nil && u.isupport.UTF8Only
}

// RequestCap sends CAP REQ for the given capability names (post-registration).
func (u *Uplink) RequestCap(names ...string) error {
	if len(names) == 0 {
		return nil
	}
	return u.WriteRaw("CAP REQ :" + strings.Join(names, " "))
}

// SASLAvailable reports whether the uplink advertised sasl (LS/NEW), and any mechs.
func (u *Uplink) SASLAvailable() (mechs []string, ok bool) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if !u.saslAvailable {
		return nil, false
	}
	return append([]string(nil), u.saslMechs...), true
}

// OwnsSASL reports whether the bouncer will perform SASL with stored credentials/cert.
func (u *Uplink) OwnsSASL() bool {
	return u.saslWanted()
}

// Run connects and reconnects until ctx is cancelled.
func (u *Uplink) Run(ctx context.Context) error {
	backoff := u.cfg.MinBackoff
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := u.session(ctx)
		if u.handler != nil {
			u.handler.OnDisconnect(u, err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		u.log.Info("uplink disconnected; reconnecting", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > u.cfg.MaxBackoff {
			backoff = u.cfg.MaxBackoff
		}
	}
}

func (u *Uplink) session(ctx context.Context) error {
	u.resetNickRecoveryState()
	u.mu.Lock()
	u.reg = false
	u.caps = make(map[string]bool)
	u.saslAvailable = false
	u.saslMechs = nil
	u.saslMech = ""
	u.scramConv = nil
	u.account = ""
	u.isupport = irc.NewISUPPORT()
	u.nick = u.cfg.Network.Nick
	u.rpl002, u.rpl003, u.rpl004 = nil, nil, nil
	u.umodes = make(map[byte]bool)
	u.mu.Unlock()

	conn, err := u.dial(ctx)
	if err != nil {
		return err
	}
	c := connio.New(conn, irc.MaxServerLine)
	u.mu.RLock()
	peer := u.cfg.Network.Name
	if peer == "" {
		peer = "uplink"
	}
	u.mu.RUnlock()
	c.SetLogger(u.log, peer)
	u.mu.Lock()
	u.conn = c
	u.mu.Unlock()
	u.startFloodDrain(ctx)
	u.startKeepalive(ctx)
	defer func() {
		u.stopNickRecovery()
		u.stopFloodDrain()
		_ = c.Close()
		u.mu.Lock()
		u.conn = nil
		u.mu.Unlock()
	}()

	go func() {
		<-ctx.Done()
		_ = c.Close()
	}()

	if err := u.register(ctx, c); err != nil {
		return err
	}
	for {
		line, err := c.ReadLine(time.Now().Add(5 * time.Minute))
		if err != nil {
			if errors.Is(err, irc.ErrLineTooLong) {
				u.log.Warn("uplink line too long; dropped", "phase", "runtime")
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		u.noteRX()
		msg, err := irc.Parse(line)
		if err != nil {
			u.log.Warn("parse error", "phase", "runtime", "line", gobnclog.RedactIRC(line), "err", err)
			continue
		}
		if err := u.handle(ctx, c, msg); err != nil {
			return err
		}
		// Uplink PING/PONG stay ringfenced — never fan out to downlinks.
		if msg.Command == "PING" || msg.Command == "PONG" {
			continue
		}
		if u.handleRecoveryNumeric(msg) {
			continue
		}
		if u.handler != nil && u.Registered() {
			u.handler.OnMessage(u, msg)
		}
	}
}

func (u *Uplink) dial(ctx context.Context) (net.Conn, error) {
	u.mu.RLock()
	n := u.cfg.Network
	customDial := u.cfg.Dial
	tlsConf := u.cfg.TLSConf
	u.mu.RUnlock()

	addr := net.JoinHostPort(n.Host, strconv.Itoa(n.Port))
	if customDial != nil {
		return customDial(ctx, "tcp", addr)
	}
	d := net.Dialer{Timeout: 30 * time.Second}
	if n.TLS {
		if tlsConf == nil {
			tlsConf = &tls.Config{ServerName: n.Host, MinVersion: tls.VersionTLS12}
		}
		return tls.DialWithDialer(&d, "tcp", addr, tlsConf)
	}
	return d.DialContext(ctx, "tcp", addr)
}

func (u *Uplink) register(ctx context.Context, c *connio.Conn) error {
	u.mu.RLock()
	n := u.cfg.Network
	u.mu.RUnlock()
	if err := c.WriteLine("CAP LS 302"); err != nil {
		return err
	}
	if n.Pass != "" {
		if err := c.WriteLine("PASS " + n.Pass); err != nil {
			return err
		}
	}
	if err := c.WriteLine("NICK " + n.Nick); err != nil {
		return err
	}
	u.mu.Lock()
	u.nick = n.Nick
	u.mu.Unlock()
	user := n.Username
	if user == "" {
		user = "gobnc"
	}
	real := n.Realname
	if real == "" {
		real = "GoBNC"
	}
	if err := c.WriteLine(fmt.Sprintf("USER %s 0 * :%s", user, real)); err != nil {
		return err
	}

	saslWanted := u.saslWanted()
	gotWelcome := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line, err := c.ReadLine(time.Now().Add(60 * time.Second))
		if err != nil {
			if errors.Is(err, irc.ErrLineTooLong) {
				u.log.Warn("uplink line too long; dropped", "phase", "register")
				continue
			}
			return err
		}
		u.noteRX()
		msg, err := irc.Parse(line)
		if err != nil {
			u.log.Warn("parse error", "phase", "register", "line", gobnclog.RedactIRC(line), "err", err)
			continue
		}
		switch msg.Command {
		case "CAP":
			if err := u.handleCAP(c, msg, saslWanted); err != nil {
				return err
			}
		case "AUTHENTICATE":
			if err := u.handleAuthenticate(c, msg); err != nil {
				return err
			}
		case "900":
			// Logged-in notification; wait for 903 before CAP END.
			u.noteAccountFrom900(msg)
		case "903", "904", "905", "906", "907": // SASL outcomes
			u.clearSASLExchange()
			if msg.Command != "903" && msg.Command != "907" && n.SASLRequired {
				return fmt.Errorf("SASL failed: %s %v", msg.Command, msg.Params)
			}
			_ = c.WriteLine("CAP END")
		case "001":
			u.mu.Lock()
			if len(msg.Params) > 0 {
				u.nick = msg.Params[0]
			}
			u.mu.Unlock()
			gotWelcome = true
			u.emitRegistrationLine(msg)
		case "002":
			u.storeWelcomeTail(&u.rpl002, msg.Params)
			u.emitRegistrationLine(msg)
		case "003":
			u.storeWelcomeTail(&u.rpl003, msg.Params)
			u.emitRegistrationLine(msg)
		case "004":
			u.storeWelcomeTail(&u.rpl004, msg.Params)
			u.emitRegistrationLine(msg)
		case "005":
			u.mu.Lock()
			u.isupport.Parse005(msg.Params)
			u.mu.Unlock()
			u.emitRegistrationLine(msg)
		case "221":
			u.applyUmodeParam(msg.Param(1))
			u.emitRegistrationLine(msg)
		case "MODE":
			u.handleUserMode(msg)
			u.emitRegistrationLine(msg)
		case "PING":
			_ = c.WriteLine("PONG :" + msg.Trailing())
		case "NOTICE":
			u.emitRegistrationLine(msg)
		case "375", "372", "376", "422":
			u.emitRegistrationLine(msg)
			if (msg.Command == "376" || msg.Command == "422") && gotWelcome {
				return u.finishRegister(c)
			}
		case "432", "433": // erroneous / nick in use
			bad := msg.Param(1)
			ok, err := u.tryNextRegisterNick(c, bad)
			if err != nil {
				return err
			}
			if ok {
				// Swallow mid-ladder nick errors (do not race awaiting clients).
				continue
			}
			u.emitRegistrationLine(msg)
			return fmt.Errorf("nick error: %s %v", msg.Command, msg.Params)
		case "437": // nick/channel temporarily unavailable
			// Pre-welcome nick form: first param is "*" (ZNC treats like 433).
			if msg.Param(0) == "*" || !gotWelcome {
				bad := msg.Param(1)
				ok, err := u.tryNextRegisterNick(c, bad)
				if err != nil {
					return err
				}
				if ok {
					continue
				}
				u.emitRegistrationLine(msg)
				return fmt.Errorf("nick error: %s %v", msg.Command, msg.Params)
			}
			u.emitRegistrationLine(msg)
		case "ERROR":
			return fmt.Errorf("server ERROR: %s", msg.Trailing())
		default:
			if isRegistrationVisible(msg.Command) {
				u.emitRegistrationLine(msg)
			}
		}
	}
}

func (u *Uplink) emitRegistrationLine(msg irc.Message) {
	if u.handler != nil {
		u.handler.OnRegistrationLine(u, msg)
	}
}

// isRegistrationVisible reports whether a command should be relayed to clients
// waiting on uplink registration (excludes CAP/AUTHENTICATE/SASL/PING).
func isRegistrationVisible(cmd string) bool {
	switch strings.ToUpper(cmd) {
	case "CAP", "AUTHENTICATE", "PING", "PONG",
		"900", "901", "902", "903", "904", "905", "906", "907", "908":
		return false
	}
	if len(cmd) == 3 {
		for i := 0; i < 3; i++ {
			if cmd[i] < '0' || cmd[i] > '9' {
				return false
			}
		}
		return true
	}
	return false
}

func (u *Uplink) finishRegister(c *connio.Conn) error {
	u.mu.Lock()
	u.reg = true
	u.mu.Unlock()
	if u.handler != nil {
		u.handler.OnRegistered(u)
	}
	u.joinChannels()
	u.maybeStartNickRecovery()
	return nil
}

func (u *Uplink) storeWelcomeTail(dst *[]string, params []string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(params) <= 1 {
		*dst = nil
		return
	}
	*dst = append([]string(nil), params[1:]...)
}

func (u *Uplink) applyUmodeParam(s string) {
	if s == "" {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.umodes == nil {
		u.umodes = make(map[byte]bool)
	}
	// 221 may be "+iw" or "iw"
	if s[0] != '+' && s[0] != '-' {
		s = "+" + s
	}
	irc.ApplyUserModes(u.umodes, s)
}

func (u *Uplink) handleUserMode(msg irc.Message) {
	target := msg.Param(0)
	if target == "" || target[0] == '#' || target[0] == '&' || target[0] == '+' || target[0] == '!' {
		return
	}
	u.mu.RLock()
	nick := u.nick
	u.mu.RUnlock()
	if !irc.CaseRFC1459.Equal(target, nick) {
		return
	}
	modestring := msg.Param(1)
	if modestring == "" {
		modestring = msg.Trailing()
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.umodes == nil {
		u.umodes = make(map[byte]bool)
	}
	irc.ApplyUserModes(u.umodes, modestring)
}

func (u *Uplink) handleCAP(c *connio.Conn, msg irc.Message, saslWanted bool) error {
	// CAP * LS / ACK / NAK / NEW / DEL
	if len(msg.Params) < 2 {
		return nil
	}
	sub := strings.ToUpper(msg.Params[1])
	trailing := msg.Trailing()
	switch sub {
	case "LS":
		available := parseCapList(trailing)
		if v, ok := available["sasl"]; ok {
			u.noteSASLOffer(v, true)
		}
		var req []string
		for _, want := range DesiredCaps {
			if want == "sasl" && !saslWanted {
				continue
			}
			if _, ok := available[want]; ok {
				req = append(req, want)
			}
		}
		if len(req) == 0 {
			return c.WriteLine("CAP END")
		}
		return c.WriteLine("CAP REQ :" + strings.Join(req, " "))
	case "ACK":
		var added []string
		for _, raw := range strings.Fields(trailing) {
			raw = strings.TrimPrefix(raw, "-")
			name, val, _ := strings.Cut(raw, "=")
			if name == "sasl" && val != "" {
				u.noteSASLOffer(val, true)
			}
			u.mu.Lock()
			if !u.caps[name] {
				added = append(added, name)
			}
			u.caps[name] = true
			u.mu.Unlock()
		}
		if u.Registered() {
			if u.handler != nil && len(added) > 0 {
				u.handler.OnCapsChanged(u, added, nil)
			}
			for _, name := range added {
				if name == "sasl" && saslWanted {
					return u.startSASL(c)
				}
			}
			return nil
		}
		if u.HasCap("sasl") && saslWanted {
			if err := u.startSASL(c); err != nil {
				return err
			}
			return nil
		}
		return c.WriteLine("CAP END")
	case "NAK":
		if !u.Registered() {
			if saslWanted && u.cfg.Network.SASLRequired {
				return fmt.Errorf("CAP NAK: %s", trailing)
			}
			return c.WriteLine("CAP END")
		}
		u.log.Info("CAP NAK", "caps", trailing)
		if u.handler != nil {
			var names []string
			for _, raw := range strings.Fields(trailing) {
				raw = strings.TrimPrefix(raw, "-")
				name, _, _ := strings.Cut(raw, "=")
				names = append(names, name)
			}
			u.handler.OnCapNAK(u, names)
		}
		return nil
	case "NEW":
		if !u.Registered() {
			return nil
		}
		available := parseCapList(trailing)
		if v, ok := available["sasl"]; ok {
			u.noteSASLOffer(v, true)
			if !saslWanted && u.handler != nil {
				u.handler.OnSASLOffer(u, true)
			}
		}
		var req []string
		for _, want := range DesiredCaps {
			if want == "sasl" && !saslWanted {
				continue
			}
			if _, ok := available[want]; ok && !u.HasCap(want) {
				req = append(req, want)
			}
		}
		if len(req) == 0 {
			return nil
		}
		return c.WriteLine("CAP REQ :" + strings.Join(req, " "))
	case "DEL":
		if !u.Registered() {
			return nil
		}
		var removed []string
		saslDel := false
		for _, name := range strings.Fields(trailing) {
			name = strings.TrimPrefix(name, "-")
			name, _, _ = strings.Cut(name, "=")
			u.mu.Lock()
			if u.caps[name] {
				delete(u.caps, name)
				removed = append(removed, name)
			}
			if name == "sasl" {
				if u.saslAvailable {
					saslDel = true
				}
				u.saslAvailable = false
				u.saslMechs = nil
			}
			u.mu.Unlock()
		}
		if u.handler != nil && len(removed) > 0 {
			u.handler.OnCapsChanged(u, nil, removed)
		}
		if saslDel && !saslWanted && u.handler != nil {
			u.handler.OnSASLOffer(u, false)
		}
		return nil
	}
	return nil
}

// parseCapList parses a CAP 302 list into name → value (value may be empty).
func parseCapList(s string) map[string]string {
	out := make(map[string]string)
	for _, p := range strings.Fields(s) {
		name, val, _ := strings.Cut(p, "=")
		out[name] = val
	}
	return out
}

func (u *Uplink) joinChannels() {
	u.mu.RLock()
	chs := append([]store.Channel(nil), u.cfg.Channels...)
	u.mu.RUnlock()
	for _, ch := range chs {
		var line string
		if ch.Key != "" {
			line = "JOIN " + ch.Name + " " + ch.Key
		} else {
			line = "JOIN " + ch.Name
		}
		// Share the flood bucket with client traffic.
		_ = u.WriteRaw(line)
	}
}

func (u *Uplink) handle(ctx context.Context, c *connio.Conn, msg irc.Message) error {
	switch msg.Command {
	case "PING":
		// PONG never enters the flood queue or bucket.
		return u.writeImmediate("PONG :" + msg.Trailing())
	case "002":
		u.storeWelcomeTail(&u.rpl002, msg.Params)
	case "003":
		u.storeWelcomeTail(&u.rpl003, msg.Params)
	case "004":
		u.storeWelcomeTail(&u.rpl004, msg.Params)
	case "005":
		u.mu.Lock()
		u.isupport.Parse005(msg.Params)
		u.mu.Unlock()
	case "221":
		u.applyUmodeParam(msg.Param(1))
	case "MODE":
		u.handleUserMode(msg)
	case "NICK":
		if irc.CaseRFC1459.Equal(msg.Nick(), u.Nick()) {
			u.mu.Lock()
			u.nick = msg.Trailing()
			if u.nick == "" && len(msg.Params) > 0 {
				u.nick = msg.Params[0]
			}
			newNick := u.nick
			u.mu.Unlock()
			u.onSelfNickChange(newNick)
		}
	case "AUTHENTICATE":
		return u.handleAuthenticate(c, msg)
	case "900":
		u.noteAccountFrom900(msg)
		return u.handleSASLOutcome(msg)
	case "901":
		u.mu.Lock()
		u.account = ""
		u.mu.Unlock()
		return u.handleSASLOutcome(msg)
	case "903", "904", "905", "906", "907":
		u.clearSASLExchange()
		return u.handleSASLOutcome(msg)
	case "CAP":
		return u.handleCAP(c, msg, u.saslWanted())
	}
	_ = ctx
	return nil
}

// handleSASLOutcome logs post-registration SASL results; pre-registration
// outcomes are handled in register() (including CAP END).
func (u *Uplink) handleSASLOutcome(msg irc.Message) error {
	if !u.Registered() {
		return nil
	}
	switch msg.Command {
	case "900", "903":
		u.log.Info("SASL authentication successful")
	case "901":
		u.log.Info("logged out of services account")
	case "907":
		u.log.Debug("SASL already authenticated")
	default:
		u.log.Info("SASL authentication failed", "numeric", msg.Command, "text", msg.Trailing())
		u.mu.RLock()
		required := u.cfg.Network.SASLRequired
		u.mu.RUnlock()
		if required {
			return fmt.Errorf("SASL failed: %s %v", msg.Command, msg.Params)
		}
	}
	return nil
}
