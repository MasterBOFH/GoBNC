package session

import (
	"slices"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// A module-only state feature is off unless a configured module is granted it: a module that
// subscribes without member_meta (a logger) starts no WHOX polling, and without silence no
// SILENCE query goes out.
func TestStateFeaturesGateModuleOnlyQueries(t *testing.T) {
	s, _, _, w := newMetaSession(t) // subscribed sink, WHOX and SILENCE advertised
	s.SetStateFeatures(0)
	pol := WHOPoll{Interval: time.Hour, SeedPace: time.Second, Pace: time.Second}
	if p := s.PollWHO(time.Now(), pol); p != "" {
		t.Fatalf("polled %q without member_meta", p)
	}
	s.RefreshSilence()
	if got := w.lines(); len(got) != 0 {
		t.Fatalf("uplink writes without the features: %q", got)
	}

	s.SetStateFeatures(StateMemberMeta)
	if p := s.PollWHO(time.Now(), pol); p == "" {
		t.Fatal("no poll with member_meta")
	}
	w.reset()
	s.RefreshSilence()
	if got := w.lines(); len(got) != 0 {
		t.Fatalf("SILENCE queried with member_meta only: %q", got)
	}
	s.SetStateFeatures(StateSilence)
	s.RefreshSilence()
	if got := w.lines(); !slices.Equal(got, []string{"SILENCE"}) {
		t.Fatalf("uplink writes with silence = %q", got)
	}
}

// userhost-in-names is requested on CAP NEW only with member_meta.
func TestCapNewUserhostInNamesNeedsMemberMeta(t *testing.T) {
	s := newBusSession(t)
	offered := map[string]string{"userhost-in-names": "", "away-notify": ""}
	if req := s.capNewRequest(offered, false); slices.Contains(req, "userhost-in-names") || !slices.Contains(req, "away-notify") {
		t.Fatalf("without member_meta: %q", req)
	}
	s.SetStateFeatures(StateMemberMeta)
	if req := s.capNewRequest(offered, false); !slices.Contains(req, "userhost-in-names") {
		t.Fatalf("with member_meta: %q", req)
	}
}

func TestStateFeaturesOf(t *testing.T) {
	if f := StateFeaturesOf([]string{wire.FeatureEvents, wire.FeatureHistory}); f != 0 {
		t.Fatalf("method features = %b", f)
	}
	f := StateFeaturesOf([]string{wire.FeatureMemberMeta, wire.FeatureSilence, wire.FeatureChannelMeta})
	if !f.Has(StateMemberMeta | StateSilence | StateChannelMeta) {
		t.Fatalf("state features = %b", f)
	}
}
