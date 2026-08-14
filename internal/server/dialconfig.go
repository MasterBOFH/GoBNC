package server

import (
	"github.com/MasterBOFH/GoBNC/internal/brain"
	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/registration"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// dialConfigForLocked builds a keeper.DialConfig from a network row — the
// exact TLS/bind resolution internal/uplink.Uplink.dial used to do (client
// cert path resolution, bind_host inheritance from gobnc.json), moved here
// verbatim: the keeper reads TLS material fresh from disk on every dial
// (see keeper.DialConfig's doc comment), so this only ever needs to
// resolve *paths* and the bind address string, never load key material
// itself or cache anything across dials — the same invariant this project
// has followed since Part 3b-i, extended one level up.
//
// Caller must hold at least s.mu's read lock — this deliberately does not
// lock itself: startNetworkLocked (one of its two callers) is called with
// s.mu's *write* lock already held, and sync.RWMutex is not reentrant, so
// a self-locking version deadlocks the moment that caller's own lock is
// still on the stack (found live: internal/server's own control-socket
// test suite hung on exactly this before this comment existed).
func (s *Server) dialConfigForLocked(n store.Network) keeper.DialConfig {
	globalCert, globalKey := s.cfg.TLSClientCert, s.cfg.TLSClientKey
	globalBind := s.cfg.BindHost

	cfg := keeper.DialConfig{
		Host:        n.Host,
		Port:        n.Port,
		TLS:         n.TLS,
		TLSNoVerify: n.TLSNoVerify,
		BindHost:    config.ResolveBindHost(n.BindHost, globalBind),
	}
	if certPath, keyPath, ok := config.ResolveTLSClientCert(n.TLSCert, n.TLSKey, globalCert, globalKey); ok {
		cfg.CertFile = certPath
		cfg.KeyFile = keyPath
	}
	return cfg
}

// networkConfigForLocked builds a brain.NetworkConfig from a network row —
// HasClientCert mirrors internal/uplink's clientCertPathsConfigured: SASL
// EXTERNAL availability depends on whether a client cert is configured at
// all, independent of whether TLS itself happens to be resolved elsewhere;
// internal/registration deliberately can't resolve this itself (it has no
// disk access — see registration.SASLConfig's doc comment), so the caller
// (here) resolves it once, the same way dialConfigForLocked resolves cert
// paths. Caller must hold at least s.mu's read lock — see
// dialConfigForLocked's doc comment for why this doesn't lock itself.
func (s *Server) networkConfigForLocked(n store.Network) brain.NetworkConfig {
	globalCert, globalKey := s.cfg.TLSClientCert, s.cfg.TLSClientKey
	_, _, hasClientCert := config.ResolveTLSClientCert(n.TLSCert, n.TLSKey, globalCert, globalKey)

	return brain.NetworkConfig{
		PrimaryNick:  n.Nick,
		AltNick:      n.AltNick,
		NickRecovery: n.NickRecovery,
		SASL: registration.SASLConfig{
			Wanted:        n.SASL,
			Required:      n.SASLRequired,
			User:          n.SASLUser,
			Pass:          n.SASLPass,
			HasClientCert: hasClientCert,
		},
		Pass:     n.Pass,
		Username: n.Username,
		Realname: n.Realname,
	}
}

// channelJoinsFor converts store.Channel rows to brain.ChannelJoin — the
// small, dependency-free shape Driver.SetChannels takes instead of
// internal/store.Channel directly (see brain.ChannelJoin's doc comment).
func channelJoinsFor(chs []store.Channel) []brain.ChannelJoin {
	out := make([]brain.ChannelJoin, 0, len(chs))
	for _, ch := range chs {
		out = append(out, brain.ChannelJoin{Name: ch.Name, Key: ch.Key})
	}
	return out
}
