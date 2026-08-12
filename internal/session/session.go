package session

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/caps"
	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/uplink"
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
	uplink  *uplink.Uplink
	tracker *RequestTracker

	mu        sync.RWMutex
	self      *User
	users     map[string]*User         // folded nick → user
	channels  map[string]*ChannelState // folded name
	// pendingJoinKeys remembers keys from client JOIN until the uplink self-JOIN
	// confirms the channel (so refused joins are not persisted for auto-rejoin).
	pendingJoinKeys map[string]string // folded name → key (may be "")
	downlinks       map[ClientID]Downlink
	isupport  *irc.ISUPPORT
	upCaps    map[string]bool
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

// New creates a session (uplink attached later via SetUplink / Run).
func New(net store.Network, st *store.Store, hist *history.Store, log *slog.Logger) *Session {
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
		tracker:         NewRequestTracker(),
		self:            self,
		users:           users,
		channels:        make(map[string]*ChannelState),
		pendingJoinKeys: make(map[string]string),
		downlinks:       make(map[ClientID]Downlink),
		awaitingUplink:  make(map[ClientID]bool),
		isupport:        irc.NewISUPPORT(),
		upCaps:          make(map[string]bool),
	}
}

// SetUplink associates the uplink.
func (s *Session) SetUplink(u *uplink.Uplink) { s.uplink = u }

// SetAdmin registers the handler for the IRC BNC command.
func (s *Session) SetAdmin(fn AdminFunc) { s.admin = fn }

// Uplink returns the associated uplink (may be nil).
func (s *Session) Uplink() *uplink.Uplink { return s.uplink }

// GracefulQuit asks the uplink to flush paced sends and QUIT (bounded by ctx).
func (s *Session) GracefulQuit(ctx context.Context, reason string) {
	if s.uplink != nil {
		s.uplink.GracefulQuit(ctx, reason)
	}
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

// ApplyNetworkConfig stores network settings for the next uplink (re)connect.
// Does not drop the current uplink connection.
func (s *Session) ApplyNetworkConfig(n store.Network) {
	s.mu.Lock()
	s.Network = n
	s.mu.Unlock()
	if s.uplink != nil {
		s.uplink.SetNetwork(n)
	}
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
