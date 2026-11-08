package server

import (
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// The module-only state Sessions keep is the union of the state features granted to configured
// modules: a logger with events alone turns none on (no WHOX polls, no SILENCE query, no
// userhost-in-names); gobnc-retecloud's grants turn on what it lists.
func TestStateFeaturesFromConfiguredModules(t *testing.T) {
	s := &Server{cfg: config.Config{Modules: config.ModulesConfig{List: []config.ModuleConfig{
		{Name: "modlog", Features: []string{wire.FeatureNetworks, wire.FeatureEvents}},
	}}}}
	if f := s.stateFeaturesLocked(); f != 0 {
		t.Fatalf("modlog alone: features %b", f)
	}
	n := store.Network{Name: "n", Nick: "me"}
	if s.networkConfigForLocked(n).UserhostInNames {
		t.Fatal("userhost-in-names wanted with modlog alone")
	}
	s.cfg.Modules.List = append(s.cfg.Modules.List, config.ModuleConfig{Name: "retecloud",
		Features: []string{wire.FeatureEvents, wire.FeatureMemberMeta, wire.FeatureSilence}})
	f := s.stateFeaturesLocked()
	if !f.Has(session.StateMemberMeta|session.StateSilence) || f.Has(session.StateChannelMeta) {
		t.Fatalf("with retecloud: features %b", f)
	}
	if !s.networkConfigForLocked(n).UserhostInNames {
		t.Fatal("userhost-in-names not wanted with member_meta")
	}
}
