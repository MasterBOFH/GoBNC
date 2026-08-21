package session

import (
	"strings"
	"sync"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/version"
)

// CTCPMode is how a Session handles one class of incoming CTCP request.
type CTCPMode string

const (
	// CTCPModeRelay forwards the request live to attached downlinks,
	// unchanged — the pre-CTCP-feature default behavior. Never stored for
	// chathistory/legacy replay (see CTCPConfig's doc comment).
	CTCPModeRelay CTCPMode = "relay"
	// CTCPModeEdge answers the request directly from the bouncer over the
	// uplink instead of relaying it to any downlink. Only meaningful for
	// PING/VERSION.
	CTCPModeEdge CTCPMode = "edge"
	// CTCPModeDisable drops the request entirely: no relay, no edge reply.
	CTCPModeDisable CTCPMode = "disable"
)

// CTCPConfig holds the server-wide relay/edge/disable settings for
// incoming CTCP PING/VERSION/other requests, shared by pointer across
// every Session on the process (mirroring how *history.Store and
// *brain.Driver are resolved once and handed to every Session, rather
// than copied in). All methods are nil-receiver-safe: a nil *CTCPConfig
// behaves as if every mode were CTCPModeRelay, so a Session that never
// had SetCTCPConfig called keeps pre-CTCP-feature passthrough behavior.
type CTCPConfig struct {
	mu                   sync.Mutex
	ping, version, other CTCPMode
}

// NewCTCPConfig builds a CTCPConfig with the given initial modes.
func NewCTCPConfig(ping, version, other CTCPMode) *CTCPConfig {
	return &CTCPConfig{ping: ping, version: version, other: other}
}

// Set updates all three modes in place (called on rehash).
func (c *CTCPConfig) Set(ping, version, other CTCPMode) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.ping, c.version, c.other = ping, version, other
	c.mu.Unlock()
}

// Ping returns the configured CTCP PING mode (CTCPModeRelay if c is nil).
func (c *CTCPConfig) Ping() CTCPMode {
	if c == nil {
		return CTCPModeRelay
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ping
}

// Version returns the configured CTCP VERSION mode (CTCPModeRelay if c is nil).
func (c *CTCPConfig) Version() CTCPMode {
	if c == nil {
		return CTCPModeRelay
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.version
}

// Other returns the configured mode for every CTCP verb besides
// PING/VERSION/ACTION (CTCPModeRelay if c is nil).
func (c *CTCPConfig) Other() CTCPMode {
	if c == nil {
		return CTCPModeRelay
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.other
}

const ctcpDelim = '\x01'

// parseCTCP extracts the verb and params from a single \x01-framed CTCP
// message body (e.g. "\x01PING 12345\x01" -> "PING", "12345", true). Does
// not attempt CTCP low-level (\020) dequoting or multiple concatenated
// CTCP messages in one line — out of scope; real-world PING/VERSION
// requests never need either.
func parseCTCP(text string) (verb, params string, ok bool) {
	if len(text) < 2 || text[0] != ctcpDelim || text[len(text)-1] != ctcpDelim {
		return "", "", false
	}
	inner := text[1 : len(text)-1]
	if inner == "" {
		return "", "", false
	}
	if sp := strings.IndexByte(inner, ' '); sp >= 0 {
		return inner[:sp], inner[sp+1:], true
	}
	return inner, "", true
}

// handleUplinkCTCP processes a CTCP-framed PRIVMSG arriving from the
// uplink per the configured mode. Returns true if the line has been fully
// handled (edge-replied, disabled, or live-relayed) and must not fall
// through to HandleMessage's normal history-storage/tracker/broadcast
// path — a CTCP request is never stored for chathistory/legacy replay,
// regardless of mode, since it's transient protocol chatter rather than
// conversation content.
//
// Applies uniformly to CTCP directed at our own nick or at a channel
// we're in — HandleMessage never sees a PRIVMSG that isn't one of those
// two cases, so no separate target check is needed. CTCP-framed NOTICE
// (a reply, not a request) is left alone entirely and always relays
// normally.
func (s *Session) handleUplinkCTCP(msg irc.Message) bool {
	if msg.Command != "PRIVMSG" {
		return false
	}
	verb, params, ok := parseCTCP(msg.Trailing())
	if !ok || strings.EqualFold(verb, "ACTION") {
		return false
	}
	upper := strings.ToUpper(verb)
	s.mu.RLock()
	ctcp := s.ctcp
	s.mu.RUnlock()
	var mode CTCPMode
	switch upper {
	case "PING":
		mode = ctcp.Ping()
	case "VERSION":
		mode = ctcp.Version()
	default:
		mode = ctcp.Other()
	}
	switch mode {
	case CTCPModeDisable:
		return true
	case CTCPModeEdge:
		s.replyCTCPEdge(msg, upper, params)
		return true
	default: // CTCPModeRelay
		s.applyState(msg)
		s.relayCTCPLive(msg)
		return true
	}
}

// replyCTCPEdge answers a CTCP PING/VERSION request directly from the
// bouncer, writing straight to the uplink (bypassing every downlink
// client) — "CTCP stops at the bouncer".
func (s *Session) replyCTCPEdge(msg irc.Message, verb, params string) {
	nick := msg.Nick()
	if nick == "" {
		return
	}
	var reply string
	switch verb {
	case "PING":
		reply = "PING"
		if params != "" {
			reply += " " + params
		}
	case "VERSION":
		reply = "VERSION GoBNC " + version.Version
	default:
		return
	}
	_ = s.WriteMessage(irc.Message{
		Command: "NOTICE",
		Params:  []string{nick, string(ctcpDelim) + reply + string(ctcpDelim)},
	})
}

// relayCTCPLive forwards msg live to every currently attached downlink,
// unchanged from HandleMessage's normal broadcast except it skips the
// legacy-playback catch-up bookkeeping (advanceLegacyPlaybackIfDelivered),
// which is meaningless for a line that is never stored.
func (s *Session) relayCTCPLive(msg irc.Message) {
	s.mu.RLock()
	downlinks := make([]Downlink, 0, len(s.downlinks))
	for _, d := range s.downlinks {
		downlinks = append(downlinks, d)
	}
	s.mu.RUnlock()
	for _, d := range downlinks {
		out := s.rewriteFor(d, msg)
		if out.Command == "" {
			continue
		}
		_ = d.Send(out)
	}
}
