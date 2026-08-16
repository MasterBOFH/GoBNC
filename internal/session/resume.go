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
//
// Does NOT write anything to the uplink itself, including the live NAMES
// refresh a resumed channel needs (see RefreshResumedChannelNames) or the
// MODE nick poll for usermodes (see RefreshSelfUModes) — this runs from
// internal/server's registerNetworkLocked, which the whole brain process's
// boot sequence deliberately calls *before* keeperClient.SendLiveReady
// goes out (every network must be registered first — see Run's own
// comment on why). The keeper's serveLive doesn't read anything at all, a
// WriteRequest included, until LiveReady is the first frame it sees; a
// write fired from here would race that and get read as a malformed
// LiveReady, killing the whole live attach. The channel names this seeded
// are stashed on Session instead, for the caller to flush once it's
// actually safe to write.
func (s *Session) SeedFromBlob(entries []keeper.BlobEntry) {
	var resumedChannels []string

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
				acct := string(e.Values[0])
				s.self.Account = acct
				// The account blob is only ever written from 900/901
				// (applyAccountFromSASL). rplLoggedInLocked requires
				// loggedIn as well as Account, so a resumed Session
				// that restored Account alone would silently omit
				// RPL_LOGGEDIN from every subsequent attach burst.
				s.loggedIn = acct != ""
			}
		case strings.HasPrefix(e.Key, "channel:"):
			name := strings.TrimPrefix(e.Key, "channel:")
			s.seedChannelLocked(name, e.Values)
			resumedChannels = append(resumedChannels, name)
		}
	}
	s.gotWelcome = true
	s.pendingNamesRefresh = resumedChannels
	// Known only when a "cloak" blob entry existed (see the case above) —
	// that key is only ever pushed from CHGHOST/396 (state.go's applyState),
	// never from the ordinary self-JOIN echo a live session normally learns
	// it from for free, since a resumed network never sends that JOIN (see
	// RefreshSelfUserHost's own doc comment). Most resumed sessions land
	// here with self.Host still empty, which is exactly the case
	// User.Prefix() must not turn into an RFC-invalid "nick!user" (no host)
	// prefix on the very next JOIN this session replays to a client.
	s.pendingUserHostRefresh = s.self != nil && s.self.Host == ""
	// Usermodes have no blob key (a live session learns them from the
	// uplink's own MODE nick during registration, which gap-only resume
	// never replays). Always poll after SeedFromBlob; RefreshSelfUModes
	// is the write, deferred to after LiveReady like NAMES/USERHOST.
	s.pendingUModeRefresh = true
	s.mu.Unlock()

	s.completeRegistration()
}

// RefreshResumedChannelNames asks the uplink for a real NAMES list on every
// channel the most recent SeedFromBlob call seeded, then clears the
// pending list (safe to call more than once; a second call is a no-op).
//
// The blob only ever carried a channel's name and key (see
// seedChannelLocked's doc comment), never a member roster — a resumed
// channel starts with only self in Members. Unlike an ordinary self-JOIN,
// a resumed attach never sends JOIN to the uplink (it's already joined;
// see brain.Driver.RegisterResumedNetwork), so there is no implicit
// JOIN-triggered 353/366 to repopulate it either.
//
// Call once the brain has actually sent LiveReady — internal/server's
// dialNetworkLocked, in its resumedAtBoot branch, is the one caller,
// chosen specifically because it's the earliest point in the boot
// sequence that runs after SendLiveReady (see SeedFromBlob's own doc
// comment for why a write can't happen any earlier).
//
// One NAMES per channel, not a single comma-joined command, since real
// networks cap NAMES targets tightly (Libera advertises ISUPPORT
// TARGMAX=NAMES:1). The reply comes back through the ordinary unsolicited
// path (no downlink requested it, so RequestTracker.RouteMessage has
// nothing to route it to specifically) and both updates ch.Members via
// state353Locked and broadcasts to every attached downlink like any other
// server-sent NAMES reply — the same thing a client's own manual /NAMES
// would produce, just fired automatically instead of waiting for someone
// to notice the roster is thin.
func (s *Session) RefreshResumedChannelNames() {
	s.mu.Lock()
	names := s.pendingNamesRefresh
	s.pendingNamesRefresh = nil
	s.mu.Unlock()
	for _, name := range names {
		_ = s.WriteMessage(irc.Message{Command: "NAMES", Params: []string{name}})
	}
}

// RefreshSelfUserHost asks the uplink for our own ident/host via USERHOST
// when the most recent SeedFromBlob call left self.Host unknown, then
// clears the pending flag (safe to call more than once; a second call is a
// no-op). Same safe-to-write-only-after-LiveReady timing as
// RefreshResumedChannelNames — same caller, same reasoning, see that
// method's doc comment.
//
// A live (non-resumed) registration normally learns self's host for free,
// the moment the uplink echoes our own self-JOIN with a full
// nick!user@host prefix (touchUserFromPrefixLocked, applyState's first
// line, fires for every message with a source) — no explicit query
// needed. A resumed network never sends that JOIN at all
// (brain.Driver.RegisterResumedNetwork installs registration.PhaseComplete
// directly, so registration.Step never re-drives it — see
// docs/keeper-design.md), and the blob's "cloak" key is only ever pushed
// from a later CHGHOST/396, not from that first self-JOIN's own prefix —
// so a resumed session commonly has no way to learn its own host at all
// without asking. Without this, self.Host stays "" and User.Prefix()'s own
// ServerName fallback papers over it in every synthesized message, forever
// — worse than transient, since nothing else would ever prompt a real
// answer. The reply (RPL_USERHOST, 302) is parsed by state302Locked,
// reached through the ordinary HandleLine path like any other line.
func (s *Session) RefreshSelfUserHost() {
	s.mu.Lock()
	need := s.pendingUserHostRefresh
	s.pendingUserHostRefresh = false
	nick := ""
	if s.self != nil {
		nick = s.self.Nick
	}
	s.mu.Unlock()
	if !need || nick == "" {
		return
	}
	_ = s.WriteMessage(irc.Message{Command: "USERHOST", Params: []string{nick}})
}

// RefreshSelfUModes asks the uplink for our own usermodes via MODE nick
// when the most recent SeedFromBlob left them unknown, then clears the
// pending flag (safe to call more than once; a second call is a no-op).
// Same safe-to-write-only-after-LiveReady timing as
// RefreshResumedChannelNames — same caller, same reasoning, see that
// method's doc comment.
//
// A live (non-resumed) registration normally learns umodes for free, from
// the uplink's own `:nick MODE nick +modes` during or just after welcome
// (stateMODELocked). A resumed network never sees that burst (gap-only
// delivery; see docs/keeper-design.md), and the blob has no umode key, so
// without this a resumed Session's UModes stay empty forever: Attach would
// omit the own-MODE line connecting clients need, and nothing else would
// ever prompt a real answer. The reply is RPL_UMODEIS (221), parsed by
// state221Locked; an unsolicited 221 (this poll, no downlink queried
// MODE nick) is rewritten to the same `:prefix MODE nick +modes` line
// Attach bursts, via broadcastSelfUMode, so a client that attached before
// the answer landed still learns the modes — matching NAMES' live 353/366
// catch-up.
func (s *Session) RefreshSelfUModes() {
	s.mu.Lock()
	need := s.pendingUModeRefresh
	s.pendingUModeRefresh = false
	nick := ""
	if s.self != nil {
		nick = s.self.Nick
	}
	s.mu.Unlock()
	if !need || nick == "" {
		return
	}
	_ = s.WriteMessage(irc.Message{Command: "MODE", Params: []string{nick}})
}

// seedChannelLocked installs one resumed channel — mirrors
// stateJOINLocked's own ChannelState construction (self included in
// Members, Modes non-nil) so a resumed channel isn't observably different
// in shape from one learned via a live self-JOIN, even though its member
// roster beyond self is empty until live traffic (RefreshResumedChannelNames'
// NAMES query, answered via state366Locked) repopulates it — the blob only
// carries what persistChannel itself tracks (name and key), not a full
// roster snapshot; see this method's own doc comment on SeedFromBlob for
// why that's the accepted scope, not an oversight. RosterKnown starts
// false (its zero value) for exactly this reason — spelled out here
// anyway since leaving it implicit next to a struct literal that sets
// every other field explicitly would read as an oversight, not a choice.
func (s *Session) seedChannelLocked(name string, values [][]byte) {
	var key string
	if len(values) > 0 {
		key = string(values[0])
	}
	folded := s.isupport.CaseMapping.Canonical(name)
	ch := &ChannelState{Name: name, Key: key, Modes: irc.NewChannelModes(), Members: map[string]struct{}{}, RosterKnown: false}
	if s.self != nil {
		ch.Members[s.isupport.CaseMapping.Canonical(s.self.Nick)] = struct{}{}
	}
	s.channels[folded] = ch
}
