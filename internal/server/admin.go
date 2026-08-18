package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/admin"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/version"
)

// attachAdmin wires the in-band BNC command handler onto a session.
func (s *Server) attachAdmin(sess *session.Session) {
	netName := sess.Network.Name
	sess.SetAdmin(func(args []string) ([]string, error) {
		s.mu.RLock()
		cfg := s.cfg
		s.mu.RUnlock()
		nick, user, real, alt := cfg.NetworkIdentityDefaults()
		return admin.Run(context.Background(), admin.Deps{
			Store:          s.store,
			ListenAddr:     cfg.ListenAddr,
			Nick:           nick,
			Username:       user,
			Realname:       real,
			AltNick:        alt,
			Runtime:        serverRuntime{s: s},
			CurrentNetwork: netName,
		}, admin.Options{AllowInlineSASLPass: true}, args)
	})
}

// StatusSnapshot builds a live status report from config, DB, and running sessions.
func (s *Server) StatusSnapshot(ctx context.Context) (admin.Status, error) {
	s.mu.RLock()
	listen := s.cfg.ListenAddr
	sessions := make(map[string]*session.Session, len(s.sess))
	for name, sess := range s.sess {
		sessions[name] = sess
	}
	s.mu.RUnlock()

	st := admin.Status{
		Running:      true,
		ListenAddr:   listen,
		Version:      version.DisplayVersion(),
		BrainVersion: version.BrainVersion,
	}
	if s.keeperClient != nil {
		st.KeeperVersion = version.NormalizeKeeperVersion(s.keeperClient.KeeperVersion)
		st.KeeperRelease = s.keeperClient.KeeperRelease
		st.KeeperUpgrade = version.CanUpgrade(s.keeperClient.KeeperVersion).String()
	}
	nets, err := s.store.ListNetworks(ctx)
	if err != nil {
		return st, err
	}
	totalClients := 0
	for _, n := range nets {
		ns := admin.NetworkStatus{
			Name: n.Name, Host: n.Host, Port: n.Port, TLS: n.TLS,
			Enabled: n.Enabled, ConfigNick: n.Nick, Nick: n.Nick,
		}
		if sess := sessions[n.Name]; sess != nil {
			ns.Running = true
			ns.Registered = sess.Registered()
			if nick := sess.Nick(); nick != "" {
				ns.Nick = nick
			}
			ns.Clients = sess.DownlinkCount()
			totalClients += ns.Clients
		}
		st.Networks = append(st.Networks, ns)
	}
	st.Clients = totalClients
	return st, nil
}

// serverRuntime applies admin actions in-process (no control socket).
type serverRuntime struct {
	s *Server
}

func (r serverRuntime) StartNetwork(name string) (bool, error) {
	return true, r.s.StartNetworkByName(name)
}

func (r serverRuntime) StopNetwork(name string) (bool, error) {
	err := r.s.StopNetwork(name)
	if err != nil && strings.Contains(err.Error(), "not running") {
		return true, nil
	}
	return true, err
}

func (r serverRuntime) ReloadNetwork(name string) (bool, error) {
	return true, r.s.ReloadNetworkConfig(name)
}

func (r serverRuntime) ReconnectNetwork(name string) error {
	return r.s.ReconnectNetwork(name)
}

func (r serverRuntime) Rehash() error {
	r.s.mu.RLock()
	path := r.s.cfgPath
	r.s.mu.RUnlock()
	if path == "" {
		return errors.New("no config path (SetConfigPath required)")
	}
	return r.s.Rehash(path)
}

func (r serverRuntime) Reload() error {
	return r.s.RequestReload()
}

func (r serverRuntime) Die() error {
	return r.s.RequestDie()
}

func (r serverRuntime) Status() (admin.Status, bool, error) {
	ctx := r.s.runCtx
	if ctx == nil {
		ctx = context.Background()
	}
	st, err := r.s.StatusSnapshot(ctx)
	if err != nil {
		return admin.Status{}, true, err
	}
	return st, true, nil
}

func statusControlReply(s *Server) string {
	ctx := s.runCtx
	if ctx == nil {
		ctx = context.Background()
	}
	st, err := s.StatusSnapshot(ctx)
	if err != nil {
		return "ERR " + err.Error()
	}
	b, err := json.Marshal(st)
	if err != nil {
		return "ERR " + err.Error()
	}
	return "OK " + string(b)
}
