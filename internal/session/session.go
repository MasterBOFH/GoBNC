package session

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/brain"
	"github.com/MasterBOFH/GoBNC/internal/caps"
	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/version"
)

// Downlink is the session's view of a connected client.
type Downlink interface {
	ID() ClientID
	Caps() map[string]bool
	HasCap(name string) bool
	ClearCap(name string)
	// HasSeenCap/MarkSeenCap track caps already advertised via CAP LS/NEW,
	// distinct from HasCap/ClearCap (which track negotiated/enabled caps).
	// Used to avoid re-announcing a cap as CAP NEW right after attach when
	// it was already included in the client's initial CAP LS reply.
	HasSeenCap(name string) bool
	MarkSeenCap(name string)
	Send(msg irc.Message) error
	Close() error
}

// ChannelState is per-channel cached state.
type ChannelState struct {
	Name    string
	Key     string // channel key (+k), persisted for auto-rejoin
	Topic   string
	Modes   *irc.ChannelModes
	Members map[string]struct{} // folded nick → present (details in Session.users)
}

// Session is one network: one uplink, many downlinks.
//
// Layout of this package:
//   - upstream.go / state_*.go — ingest and cache traffic from the IRC server
//   - replay.go / downstream.go — attach burst and client→uplink forwarding
//   - user.go — nick/user/host cache
type Session struct {
	Network store.Network
	log     *slog.Logger
	store   *store.Store
	hist    *history.Store
	driver  *brain.Driver
	netID   keeper.NetworkID
	tracker *RequestTracker

	mu       sync.RWMutex
	self     *User
	users    map[string]*User         // folded nick → user
	channels map[string]*ChannelState // folded name
	// pendingJoinKeys remembers keys from client JOIN until the uplink self-JOIN
	// confirms the channel (so refused joins are not persisted for auto-rejoin).
	pendingJoinKeys map[string]string // folded name → key (may be "")
	downlinks       map[ClientID]Downlink
	isupport        *irc.ISUPPORT
	upCaps          map[string]bool
	// upSASLAvailable / upSASLMechs track whether the uplink currently
	// offers sasl at all (CAP LS/NEW/DEL, post-registration too) — the
	// Session-owned equivalent of internal/uplink.Uplink's own
	// saslAvailable/saslMechs fields, needed because registration.State's
	// Offered map is frozen at registration completion (see HandleLine's
	// CAP interpretation for why this has to live here now instead).
	upSASLAvailable bool
	upSASLMechs     []string
	// saslOffer is the CAP token advertised for passthrough SASL ("" if none).
	saslOffer      string
	saslWaiters    []ClientID
	saslReqPending bool
	saslClient     ClientID // downlink mid AUTHENTICATE exchange
	loggedIn       bool     // true after RPL_LOGGEDIN until RPL_LOGGEDOUT
	rpl002         []string // params after nick
	rpl003         []string
	rpl004         []string
	uplinkServer   string // source prefix from uplink 001 when known
	ircd           string // detected IRCd family (irc.IRCd*)
	registered     bool   // true after uplink OnRegistered until OnDisconnect
	// gotWelcome tracks 001 pre-registration, mirroring
	// registration.State.GotWelcome — completeRegistration is only ever
	// triggered by 376/422 after 001 has actually been seen, matching
	// registration.Step's own guard (see HandleRegistrationLine).
	gotWelcome bool
	// lastNickErrorLine / hasLastNickErrorLine stash the most recent
	// 432/433/437 seen pre-registration, surfaced by HandleDisconnect only
	// when it turns out to be the terminal one (see HandleRegistrationLine's
	// "432", "433", "437" case).
	lastNickErrorLine    irc.Message
	hasLastNickErrorLine bool
	// lineSeq is the keeper.LineMsg.Seq of whatever line HandleLine is
	// currently dispatching, read via currentLineSeq() by maybeStoreHistory's
	// HandleMessage call sites to give a stored message a stable,
	// replay-safe identity — see store.Message.KeeperSeq's doc comment.
	// Safe as ambient per-Session state, not an explicit parameter threaded
	// through HandleMessage, because only one goroutine ever drives a given
	// Session's HandleLine calls (internal/server's demux — see its own
	// doc comment) and each is fully synchronous before the next begins.
	lineSeq uint64
	// awaitingUplink marks downlinks that attached before uplink registration
	// finished and should receive live/buffered registration traffic.
	awaitingUplink map[ClientID]bool
	// regBuffer holds client-visible registration lines until uplink is registered
	// (for mid-registration attach catch-up).
	regBuffer []irc.Message
	// readMarkers is an in-memory fallback when store is nil or network ID unset.
	readMarkers map[string]string
	// playbackCursors is an in-memory fallback for legacy attach playback watermarks.
	playbackCursors map[string]string
	// selfEcho maps uplink label → client label for PRIVMSG/NOTICE/TAGMSG when
	// the uplink has labeled-response + echo-message (bouncer-owned upstream labels).
	selfEcho    map[string]pendingSelfEcho
	selfEchoSeq uint64
	// heldUntilReg queues client→uplink commands received after a synthetic 001
	// but before the uplink finishes registration (avoids 451 / stuck solicitous).
	heldUntilReg []heldClientMsg
	// heldFlushing is true while flushHeldAfterRegister is draining the queue.
	heldFlushing bool
	// heldFlushCancel fingerprints the client already re-sent during flush;
	// flush must not also forward them.
	heldFlushCancel map[ClientID]map[string]struct{}
	// heldFlushSent fingerprints recently forwarded by flush; a matching client
	// re-send after the real 001 is dropped once (see heldFlushSentTTL).
	heldFlushSent map[ClientID]map[string]time.Time
	// admin handles the in-band BNC IRC command (nil if unset).
	admin AdminFunc
	// debugRegistry routes this network's raw traffic / log lines to
	// clients that opted in via /bnc debug (nil if unset — matches admin's
	// own nil-safe fallback for test/harness paths that never wire it up).
	// debugTargets tracks the live subscription per requesting downlink,
	// so /bnc debug off and Detach can find the exact same DebugTarget
	// instance to unsubscribe (see debug.go).
	debugRegistry *gobnclog.DebugRegistry
	debugTargets  map[ClientID]*sessionDebugTarget
}

// pendingSelfEcho correlates an uplink-labeled self-echo with the originating client.
type pendingSelfEcho struct {
	Client ClientID
	Label  string
}

// heldClientMsg is a client command waiting for uplink registration.
type heldClientMsg struct {
	Client ClientID
	Msg    irc.Message
}

// AdminFunc runs a BNC management command and returns NOTICE text lines.
type AdminFunc func(args []string) (lines []string, err error)

// New creates a session bound to one network's slice of a shared
// *brain.Driver — driver serves every network internal/server holds in this
// process (see docs/keeper-design.md's one-Driver-per-process shape), so
// unlike the old *uplink.Uplink (one instance per network, constructed and
// owned by the Session itself), driver is handed in already live and
// running; Session only ever addresses it by netID.
func New(net store.Network, st *store.Store, hist *history.Store, log *slog.Logger, driver *brain.Driver) *Session {
	if log == nil {
		log = slog.Default()
	}
	self := &User{
		Nick:   net.Nick,
		User:   net.Username,
		UModes: make(map[byte]bool),
	}
	users := map[string]*User{
		irc.CaseRFC1459.Canonical(net.Nick): self,
	}
	return &Session{
		Network:         net,
		log:             log,
		store:           st,
		hist:            hist,
		driver:          driver,
		netID:           keeper.NetworkID(net.ID),
		tracker:         NewRequestTracker(),
		self:            self,
		users:           users,
		channels:        make(map[string]*ChannelState),
		pendingJoinKeys: make(map[string]string),
		downlinks:       make(map[ClientID]Downlink),
		awaitingUplink:  make(map[ClientID]bool),
		isupport:        irc.NewISUPPORT(),
		upCaps:          make(map[string]bool),
		debugTargets:    make(map[ClientID]*sessionDebugTarget),
	}
}

// SetAdmin registers the handler for the IRC BNC command.
func (s *Session) SetAdmin(fn AdminFunc) { s.admin = fn }

// SetDebugRegistry wires the process-wide /bnc-debug routing table onto
// this session (mirrors SetAdmin's own post-construction wiring pattern —
// see internal/server.registerNetworkLocked). nil is a valid, silent
// default: handleBNCDebug reports "debug unavailable" the same way
// handleBNC already does for a nil admin, matching the class of test/
// harness paths that construct a Session without ever calling this.
func (s *Session) SetDebugRegistry(reg *gobnclog.DebugRegistry) { s.debugRegistry = reg }

// NetworkID returns the keeper.NetworkID this session's uplink is addressed
// by on the shared Driver.
func (s *Session) NetworkID() keeper.NetworkID { return s.netID }

// GracefulQuit asks the driver to flush paced sends and QUIT (bounded by ctx).
func (s *Session) GracefulQuit(ctx context.Context, reason string) {
	if s.driver == nil {
		return
	}
	if reason == "" {
		reason = version.QuitMessage()
	}
	s.driver.WaitFloodDrained(ctx, s.netID)
	var timeout time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d > 0 {
			timeout = d
		}
	}
	_ = s.driver.QuitNetwork(s.netID, reason, timeout)
}

// SetRegisteredForTest marks the session as uplink-registered (tests only).
func (s *Session) SetRegisteredForTest(v bool) {
	s.mu.Lock()
	s.registered = v
	s.mu.Unlock()
}

// SetUpCapsForTest sets the cached uplink capability map (tests only).
func (s *Session) SetUpCapsForTest(m map[string]bool) {
	s.mu.Lock()
	s.upCaps = m
	s.mu.Unlock()
}

// ApplyNetworkConfig stores network settings for the next uplink (re)connect
// and updates the driver's registration config for the next attempt (cfg is
// the resolved brain.NetworkConfig — SASL's HasClientCert in particular
// needs disk/TLS-config resolution Session has no access to, so the caller
// (internal/server, which already resolves this once at network start)
// builds it). Does not drop the current connection or redrive registration
// on it — the new settings take effect on the next Reconnect/redial, same
// as internal/uplink.Uplink.SetNetwork's config-only side; flood pacing
// applies immediately.
func (s *Session) ApplyNetworkConfig(n store.Network, cfg brain.NetworkConfig) {
	s.mu.Lock()
	s.Network = n
	s.mu.Unlock()
	if s.driver == nil {
		return
	}
	s.driver.SetFloodParams(s.netID, n.FloodBurst, n.FloodRate)
	s.driver.UpdateNetworkConfig(s.netID, cfg)
}

// DownlinkCount returns the number of attached clients on this network.
func (s *Session) DownlinkCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.downlinks)
}

// Registered reports whether the uplink has completed welcome (001).
func (s *Session) Registered() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.registered
}

// pushBlob pushes one derived state entry to the keeper's blob store for
// this network, via the shared Driver — best-effort, logged on failure,
// never fatal to the caller's own processing (matching WriteMessage's own
// nil-driver tolerance). Must be called without s.mu held: Driver.PushBlob
// is a blocking wire round trip, and holding a lock this broad across one
// would stall every other access to this Session for its duration — see
// applyState's own doc comment for the established pattern this follows.
func (s *Session) pushBlob(key string, mode keeper.BlobMode, value []byte) {
	if s.driver == nil {
		return
	}
	if err := s.driver.PushBlob(s.netID, key, mode, value); err != nil {
		s.log.Warn("PushBlob", "key", key, "err", err)
	}
}

// currentLineSeq returns the keeper.LineMsg.Seq HandleLine is currently
// dispatching — see currentLineSeq's field doc comment.
func (s *Session) currentLineSeq() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lineSeq
}

// Name returns the network name.
func (s *Session) Name() string { return s.Network.Name }

// Nick returns current nick.
func (s *Session) Nick() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.self == nil {
		return ""
	}
	return s.self.Nick
}

// Self returns the cached User for our uplink nick.
func (s *Session) Self() *User {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.self
}

// SelfPrefix returns nick!user@host from cached self identity.
func (s *Session) SelfPrefix() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.self == nil {
		return ""
	}
	return s.self.Prefix()
}

// HasUpCap reports whether the uplink currently has name negotiated —
// Session's own copy, kept current by HandleRegistered and HandleLine's CAP
// ACK/DEL interpretation, replacing internal/uplink.Uplink.HasCap now that
// there's no Uplink instance to ask.
func (s *Session) HasUpCap(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.upCaps[name]
}

// WriteMessage sends msg to the uplink, flood-paced — replaces
// internal/uplink.Uplink.WriteMessage, routed through the shared Driver
// instead of a per-network raw connection. Uses Wire so a parsed client Raw
// body is preserved; only the tag prefix changes.
func (s *Session) WriteMessage(msg irc.Message) error {
	if s.driver == nil {
		return fmt.Errorf("uplink not ready")
	}
	return s.driver.WriteRaw(s.netID, msg.Wire())
}

// OfferedCaps returns capabilities currently available to downlinks.
func (s *Session) OfferedCaps() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := caps.Offered(s.upCaps)
	if s.saslOffer != "" {
		out = append(out, s.saslOffer)
	}
	return out
}

// User returns a cached user by nick (nil if unknown).
func (s *Session) User(nick string) *User {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.users[s.isupport.CaseMapping.Canonical(nick)]
}
