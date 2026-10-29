package server

import (
	"context"
	"fmt"
	"sort"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/module"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/version"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// moduleBackend implements module.Backend over the Server (a separate type: Server.Session
// already means the downlink.Manager lookup by name).
type moduleBackend struct{ s *Server }

func (b moduleBackend) BrainVersion() string { return version.DisplayVersion() }

func (b moduleBackend) Ready() bool { return b.s.modulesReady.Load() }

func (b moduleBackend) Networks() []wire.NetworkInfo {
	nets, err := b.s.store.ListNetworks(b.s.storeCtx())
	if err != nil {
		b.s.log.Warn("modules: list networks", "err", err)
		return nil
	}
	out := make([]wire.NetworkInfo, 0, len(nets))
	for _, n := range nets {
		out = append(out, b.s.networkInfo(n))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (b moduleBackend) Store() *store.Store { return b.s.store }

func (b moduleBackend) Config() config.Config {
	b.s.mu.RLock()
	defer b.s.mu.RUnlock()
	return b.s.cfg
}

func (b moduleBackend) ConnectNetwork(id int64) error {
	return b.byName(id, b.s.StartNetworkByName)
}

func (b moduleBackend) DisconnectNetwork(id int64) error {
	return b.byName(id, b.s.StopNetwork)
}

func (b moduleBackend) ReconnectNetwork(id int64) error {
	return b.byName(id, b.s.ReconnectNetwork)
}

// byName runs a by-name network action for network id.
func (b moduleBackend) byName(id int64, action func(name string) error) error {
	nets, err := b.s.store.ListNetworks(b.s.storeCtx())
	if err != nil {
		return err
	}
	for _, n := range nets {
		if n.ID == id {
			return action(n.Name)
		}
	}
	return fmt.Errorf("no network %d", id)
}

func (b moduleBackend) Session(id int64) (*session.Session, bool) {
	sess := b.s.sessionByNetwork(keeper.NetworkID(id))
	return sess, sess != nil
}

// storeCtx is the run context once Run started, else Background (tests drive pieces directly).
func (s *Server) storeCtx() context.Context {
	if s.runCtx != nil {
		return s.runCtx
	}
	return context.Background()
}

// networkInfo is n as modules see it: stopped unless a Session runs it.
func (s *Server) networkInfo(n store.Network) wire.NetworkInfo {
	info := wire.NetworkInfo{ID: n.ID, Name: n.Name, State: wire.NetStopped, Nick: n.Nick}
	if sess := s.sessionByNetwork(keeper.NetworkID(n.ID)); sess != nil {
		info.State = sess.State()
		if nick := sess.Nick(); nick != "" {
			info.Nick = nick
		}
	}
	return info
}

// startModules starts the module socket. Run calls it before registering networks, so a module
// connecting during boot is told ready:false and gets the ready event after LiveReady.
func (s *Server) startModules(ctx context.Context) {
	s.mu.RLock()
	cfg := s.cfg.ResolvedModules()
	s.mu.RUnlock()
	srv := module.NewServer(cfg, moduleBackend{s}, s.log.With("component", "modules"))
	s.modules.Store(srv)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := srv.Serve(ctx); err != nil && ctx.Err() == nil {
			s.log.Error("module socket stopped", "err", err)
		}
	}()
}

// markModulesReady records that LiveReady went out (uplink writes are possible) and tells
// connected modules. Run calls it right after SendLiveReady succeeds.
func (s *Server) markModulesReady() {
	s.modulesReady.Store(true)
	if m := s.modules.Load(); m != nil {
		m.MarkReady()
	}
}

// notifyNetwork sends a network event for n to modules. It takes no Server lock: Session state
// hooks call it from under registerNetworkLocked (a resumed network registers in SeedFromBlob).
func (s *Server) notifyNetwork(n store.Network, sess *session.Session, change string) {
	m := s.modules.Load()
	if m == nil {
		return
	}
	ev := wire.NetworkEvent{Network: n.ID, Name: n.Name, State: wire.NetStopped, Nick: n.Nick, Change: change}
	if sess != nil {
		ev.State = sess.State()
		if nick := sess.Nick(); nick != "" {
			ev.Nick = nick
		}
	}
	m.NetworkChanged(ev)
}

// watchSessionState forwards sess's state changes to modules as network events.
func (s *Server) watchSessionState(sess *session.Session, n store.Network) {
	sess.SetStateHook(func(string) { s.notifyNetwork(n, sess, wire.ChangeState) })
}
