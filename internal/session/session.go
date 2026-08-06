package session

import (
	"log/slog"
	"sync"

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
	downlinks map[ClientID]Downlink
	isupport  *irc.ISUPPORT
	upCaps    map[string]bool
	// saslOffer is the CAP token advertised for passthrough SASL ("" if none).
	saslOffer      string
	saslWaiters    []ClientID
	saslReqPending bool
	saslClient     ClientID // downlink mid AUTHENTICATE exchange
	rpl002         []string // params after nick
	rpl003         []string
	rpl004         []string
}

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
		Network:   net,
		log:       log,
		store:     st,
		hist:      hist,
		tracker:   NewRequestTracker(),
		self:      self,
		users:     users,
		channels:  make(map[string]*ChannelState),
		downlinks: make(map[ClientID]Downlink),
		isupport:  irc.NewISUPPORT(),
		upCaps:    make(map[string]bool),
	}
}

// SetUplink associates the uplink.
func (s *Session) SetUplink(u *uplink.Uplink) { s.uplink = u }

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
