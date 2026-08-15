package session

import (
	"encoding/json"
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
)

// SeedFromBlob initializes Session's state directly from a resumed
// network's blob snapshot (keeper.NetworkStatus.Blob, delivered for free
// at attach time) and marks the session registered — the resume-time
// replacement for what completeRegistration used to learn by watching a
// replayed registration burst. Gap-only delivery (see
// docs/keeper-design.md) means that burst is never replayed on a resumed
// attach, so without this, a resumed Session would simply never become
// Registered at all: nothing would ever arrive to make it so.
//
// Call once, before any live line for this network reaches HandleLine —
// internal/server's registerNetworkLocked is the one caller, for a
// network found in resumedAtBoot. Safe to call with an empty snapshot
// (a network that was Connected at attach but never derived any blob
// state yet, e.g. mid-registration when the brain that was attached
// before this one crashed) — it still marks the session registered from
// whatever's there, which may be little or nothing; a real fix for that
// narrower case needs registration-phase resume, not covered here (the
// keeper only reports NotConnected/blob-cleared for a connection that
// never reached a state worth resuming, and dialNetworkLocked's own
// resumedAtBoot handling only fires for a network the keeper reports
// Connected in the first place).
//
// Known gap, not fixed here: nick-recovery state and s.uplinkServer (the
// source prefix from the uplink's own 001, used cosmetically for
// synthetic message sources) have no blob key and are not restored —
// matches brain.Driver.RegisterResumedNetwork's own documented
// nick-recovery gap for the same underlying reason (no snapshot to
// resume them from).
func (s *Session) SeedFromBlob(entries []keeper.BlobEntry) {
	s.mu.Lock()
	// isupport first: channel case-mapping below depends on it being
	// current. Snapshot's own first-push order already happens to put
	// isupport first in the normal case (005 always precedes JOIN traffic
	// in a real session) — this two-pass structure is defensive against
	// that ordering rather than relying on it.
	for _, e := range entries {
		if e.Key != "isupport" {
			continue
		}
		for _, v := range e.Values {
			var params []string
			if json.Unmarshal(v, &params) == nil {
				s.isupport.Parse005(params)
			}
		}
	}
	for _, e := range entries {
		switch {
		case e.Key == "isupport":
			// handled above
		case e.Key == "caps":
			if len(e.Values) == 0 {
				continue
			}
			var names []string
			if json.Unmarshal(e.Values[0], &names) == nil {
				for _, n := range names {
					s.upCaps[n] = true
				}
			}
		case e.Key == "self-nick":
			if len(e.Values) > 0 && s.self != nil {
				s.ensureSelfLocked(string(e.Values[0]))
			}
		case e.Key == "cloak":
			if len(e.Values) > 0 && s.self != nil {
				s.self.Host = string(e.Values[0])
			}
		case e.Key == "account":
			if len(e.Values) > 0 && s.self != nil {
				s.self.Account = string(e.Values[0])
			}
		case strings.HasPrefix(e.Key, "channel:"):
			s.seedChannelLocked(strings.TrimPrefix(e.Key, "channel:"), e.Values)
		}
	}
	s.gotWelcome = true
	s.mu.Unlock()

	s.completeRegistration()
}

// seedChannelLocked installs one resumed channel — mirrors
// stateJOINLocked's own ChannelState construction (self included in
// Members, Modes non-nil) so a resumed channel isn't observably different
// in shape from one learned via a live self-JOIN, even though its member
// roster beyond self is empty until live traffic (JOIN/NAMES/WHO) repopulates
// it — the blob only carries what persistChannel itself tracks (name and
// key), not a full roster snapshot; see this method's own doc comment on
// SeedFromBlob for why that's the accepted scope, not an oversight.
func (s *Session) seedChannelLocked(name string, values [][]byte) {
	var key string
	if len(values) > 0 {
		key = string(values[0])
	}
	folded := s.isupport.CaseMapping.Canonical(name)
	ch := &ChannelState{Name: name, Key: key, Modes: irc.NewChannelModes(), Members: map[string]struct{}{}}
	if s.self != nil {
		ch.Members[s.isupport.CaseMapping.Canonical(s.self.Nick)] = struct{}{}
	}
	s.channels[folded] = ch
}
