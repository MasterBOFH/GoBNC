package store

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Server-sourced lines are stored under the server's own name (irc.IsServerName: a notice from
// irc.example.net under "irc.example.net"), an ordinary history target. The module API's
// Server buffer (key wire.ServerBufferKey, "*") is every such target of a network together:
// the id-paged reads and activity below take "*" for that set.

// serverTargetCache knows each network's server-name targets once asked (one DISTINCT scan),
// and learns new ones as InsertMessage stores them. A target emptied by retention stays listed;
// it only matches no rows.
type serverTargetCache struct {
	mu   sync.Mutex
	nets map[int64]map[string]bool
}

// ServerTargets lists the server-name targets stored on networkID, sorted.
func (s *Store) ServerTargets(ctx context.Context, networkID int64) ([]string, error) {
	s.serverTargets.mu.Lock()
	known, ok := s.serverTargets.nets[networkID]
	s.serverTargets.mu.Unlock()
	if !ok {
		targets, err := s.DistinctTargets(ctx, networkID)
		if err != nil {
			return nil, err
		}
		known = map[string]bool{}
		for _, t := range targets {
			if irc.IsServerName(t) {
				known[t] = true
			}
		}
		s.serverTargets.mu.Lock()
		if s.serverTargets.nets == nil {
			s.serverTargets.nets = map[int64]map[string]bool{}
		}
		if cur, ok := s.serverTargets.nets[networkID]; ok {
			for t := range cur { // stored meanwhile
				known[t] = true
			}
		}
		s.serverTargets.nets[networkID] = known
		s.serverTargets.mu.Unlock()
	}
	s.serverTargets.mu.Lock()
	out := make([]string, 0, len(known))
	for t := range known {
		out = append(out, t)
	}
	s.serverTargets.mu.Unlock()
	sort.Strings(out)
	return out, nil
}

// noteTarget records a stored target that is a server's name.
func (s *Store) noteTarget(networkID int64, target string) {
	if !irc.IsServerName(target) {
		return
	}
	s.serverTargets.mu.Lock()
	defer s.serverTargets.mu.Unlock()
	if s.serverTargets.nets == nil {
		s.serverTargets.nets = map[int64]map[string]bool{}
	}
	if s.serverTargets.nets[networkID] == nil {
		s.serverTargets.nets[networkID] = map[string]bool{}
	}
	s.serverTargets.nets[networkID][target] = true
}

// InBuffer reports whether a row stored under target belongs to the buffer keyed key: the
// target itself, or for the Server buffer ("*") any server-name target.
func InBuffer(target, key string) bool {
	return target == key || (key == wire.ServerBufferKey && irc.IsServerName(target))
}

// messagesOf is the WHERE clause (and its args) selecting the rows of the buffer keyed target
// on networkID: that target, or for the Server buffer every server-name target.
func (s *Store) messagesOf(ctx context.Context, networkID int64, target string) (string, []any, error) {
	if target != wire.ServerBufferKey {
		return "network_id=? AND target=?", []any{networkID, target}, nil
	}
	targets, err := s.ServerTargets(ctx, networkID)
	if err != nil {
		return "", nil, err
	}
	if len(targets) == 0 {
		return "0", nil, nil
	}
	args := []any{networkID}
	for _, t := range targets {
		args = append(args, t)
	}
	return "network_id=? AND target IN (?" + strings.Repeat(",?", len(targets)-1) + ")", args, nil
}
