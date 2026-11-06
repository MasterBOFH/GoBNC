package retecloud

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/modules/retecloud/highlight"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// The synced highlight settings of a network (WEBSOCKET_PROTOCOL.md, "Highlight rules" and
// "Buffer highlight switch"; the lite protocol's GoBNC notes): the rules and the set of buffers
// whose highlight switch is off, each kept in the module's key-value store with the time of its
// last write (its version, the config item's updated_at). A key never written is absent: it is
// not sent in config_update, so a client can seed it.

const (
	kvHighlightOff   = "highlight_off/"
	kvHighlightRules = "highlight_rules/"

	configKeyHighlightOff   = "buffer_highlight_off"
	configKeyHighlightRules = "highlight_rules"

	maxHighlightOff = 500
)

type storedHighlightOff struct {
	Buffers   []string `json:"buffers"`
	UpdatedAt string   `json:"updated_at"`
}

type storedHighlightRules struct {
	Rules     json.RawMessage `json:"rules"`
	UpdatedAt string          `json:"updated_at"`
}

// highlightConfig is a network's loaded settings. Loop-owned.
type highlightConfig struct {
	off          map[string]bool // folded buffer names
	offList      []string        // sorted
	offVersion   string          // "" = never written
	rules        []highlight.Rule
	rulesRaw     json.RawMessage
	rulesVersion string // "" = never written
}

func hlOffKey(network int64) string   { return kvHighlightOff + strconv.FormatInt(network, 10) }
func hlRulesKey(network int64) string { return kvHighlightRules + strconv.FormatInt(network, 10) }

// versionFormat is the version's spelling: UTC, milliseconds (as retecloud-api's updated_at).
const versionFormat = "2006-01-02T15:04:05.000Z"

// sameVersion compares an if_version with the stored version as instants at microsecond
// precision, so any RFC 3339 spelling of the same time matches; "" matches only a key never
// written.
func sameVersion(ifVersion, stored string) bool {
	if ifVersion == "" || stored == "" {
		return ifVersion == stored
	}
	a, errA := time.Parse(time.RFC3339Nano, ifVersion)
	b, errB := time.Parse(time.RFC3339Nano, stored)
	if errA != nil || errB != nil {
		return false
	}
	return a.Truncate(time.Microsecond).Equal(b.Truncate(time.Microsecond))
}

// nextVersion is now's version, made later than prev so every write has a new one.
func nextVersion(prev string, now time.Time) string {
	now = now.UTC().Truncate(time.Millisecond)
	if p, err := time.Parse(versionFormat, prev); err == nil && !now.After(p) {
		now = p.Add(time.Millisecond)
	}
	return now.Format(versionFormat)
}

func (m *Module) storageGet(key string) (string, bool, error) {
	var res wire.StorageGetResult
	if err := m.call(wire.MethodStorageGet, wire.StorageKeyParams{Key: key}, &res); err != nil {
		return "", false, err
	}
	return res.Value, res.Found, nil
}

func (m *Module) storageSet(key, value string) error {
	return m.call(wire.MethodStorageSet, wire.StorageSetParams{Key: key, Value: value}, nil)
}

// readHighlightOff reads a network's stored set (nil, "" when never written).
func (m *Module) readHighlightOff(network int64) (storedHighlightOff, bool, error) {
	var s storedHighlightOff
	v, found, err := m.storageGet(hlOffKey(network))
	if err != nil || !found {
		return s, false, err
	}
	if err := json.Unmarshal([]byte(v), &s); err != nil {
		return s, false, err
	}
	return s, true, nil
}

func (m *Module) readHighlightRules(network int64) (storedHighlightRules, bool, error) {
	var s storedHighlightRules
	v, found, err := m.storageGet(hlRulesKey(network))
	if err != nil || !found {
		return s, false, err
	}
	if err := json.Unmarshal([]byte(v), &s); err != nil {
		return s, false, err
	}
	return s, true, nil
}

// loadHighlightConfig (re)reads a network's settings from the store.
func (m *Module) loadHighlightConfig(n *network) {
	hc := highlightConfig{off: map[string]bool{}}
	if s, ok, err := m.readHighlightOff(n.id); err != nil {
		m.log.Warn("highlight_off: load", "network", n.id, "err", err)
	} else if ok {
		hc.setOff(s)
	}
	if s, ok, err := m.readHighlightRules(n.id); err != nil {
		m.log.Warn("highlight_rules: load", "network", n.id, "err", err)
	} else if ok {
		if err := hc.setRules(s); err != nil {
			m.log.Warn("highlight_rules: stored rules unreadable", "network", n.id, "err", err)
		}
	}
	n.hl = hc
}

func (hc *highlightConfig) setOff(s storedHighlightOff) {
	hc.off = make(map[string]bool, len(s.Buffers))
	for _, b := range s.Buffers {
		hc.off[b] = true
	}
	hc.offList = append([]string{}, s.Buffers...)
	sort.Strings(hc.offList)
	hc.offVersion = s.UpdatedAt
}

func (hc *highlightConfig) setRules(s storedHighlightRules) error {
	rules, err := highlight.ParseRules(s.Rules)
	if err != nil {
		return err
	}
	hc.rules, hc.rulesRaw, hc.rulesVersion = rules, s.Rules, s.UpdatedAt
	return nil
}

// rulesActive reports enabled rules: the evaluation differs from the default nick mention.
func (hc *highlightConfig) rulesActive() bool {
	for _, r := range hc.rules {
		if r.IsEnabled {
			return true
		}
	}
	return false
}

// configItems are the config_update items of a network's written settings.
func (n *network) configItems() []map[string]interface{} {
	var out []map[string]interface{}
	if n.hl.offVersion != "" {
		list := n.hl.offList
		if list == nil {
			list = []string{}
		}
		v, _ := json.Marshal(list)
		out = append(out, map[string]interface{}{"key": configKeyHighlightOff, "value": string(v), "updated_at": n.hl.offVersion})
	}
	if n.hl.rulesVersion != "" {
		out = append(out, map[string]interface{}{"key": configKeyHighlightRules, "value": string(n.hl.rulesRaw),
			"updated_at": n.hl.rulesVersion})
	}
	return out
}

// syncConfigUpdate is the config_update a sync_session sends before sync_start: the network's
// name and its written highlight settings.
func (n *network) syncConfigUpdate(now time.Time) ServerMessage {
	name, _ := json.Marshal(n.name)
	items := []map[string]interface{}{{"key": "name", "value": string(name), "updated_at": now.UTC()}}
	items = append(items, n.configItems()...)
	return ServerMessage{Type: "config_update", SessionID: sessionID(n.id), Data: map[string]interface{}{"updates": items}}
}

// broadcastConfigItem tells every connection following n that one setting changed.
func (m *Module) broadcastConfigItem(n *network, key string) {
	for _, it := range n.configItems() {
		if it["key"] == key {
			m.broadcastFollowers(n.id, ServerMessage{Type: "config_update", SessionID: sessionID(n.id),
				Data: map[string]interface{}{"updates": []map[string]interface{}{it}}})
		}
	}
}

// codedError is an error frame with a code (and optional data).
func codedError(kind, rid, sid, text, code string, data interface{}) ServerMessage {
	f := errorFrame(kind, rid, text)
	f.SessionID, f.Code, f.Data = sid, code, data
	return f
}

// SetBufferHighlightRequest is set_buffer_highlight's data.
type SetBufferHighlightRequest struct {
	Buffer    string `json:"buffer"`
	Highlight *bool  `json:"highlight"`
}

// handleSetBufferHighlight switches one buffer's highlights on or off for every device: one
// add or remove on the stored set (read, changed and written on the loop, so two devices
// switching different buffers both take effect). A change is broadcast to n's followers.
func (m *Module) handleSetBufferHighlight(c *wsConn, msg *ClientMessage) {
	const kind = "set_buffer_highlight"
	fail := func(text, code string) { c.send(codedError(kind, msg.RequestID, msg.SessionID, text, code, nil)) }
	if msg.SessionID == "" {
		fail("session_id is required", "invalid_request")
		return
	}
	var req SetBufferHighlightRequest
	if len(msg.Data) > 0 {
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			fail("Invalid request data", "invalid_request")
			return
		}
	}
	if strings.TrimSpace(req.Buffer) == "" {
		fail("buffer is required", "invalid_request")
		return
	}
	if req.Highlight == nil {
		fail("highlight is required", "invalid_request")
		return
	}
	n := m.networkFor(msg.SessionID)
	if n == nil {
		fail("No access to session", "no_access")
		return
	}
	key := n.key(req.Buffer)
	stored, _, err := m.readHighlightOff(n.id)
	if err != nil {
		fail("Database error", "unavailable")
		return
	}
	set := map[string]bool{}
	for _, b := range stored.Buffers {
		set[b] = true
	}
	off := !*req.Highlight
	changed := set[key] != off
	if changed {
		if off && len(set) >= maxHighlightOff {
			fail("Too many buffers with highlights off", "limit")
			return
		}
		if off {
			set[key] = true
		} else {
			delete(set, key)
		}
		next := storedHighlightOff{Buffers: make([]string, 0, len(set)), UpdatedAt: nextVersion(stored.UpdatedAt, m.now())}
		for b := range set {
			next.Buffers = append(next.Buffers, b)
		}
		sort.Strings(next.Buffers)
		value, _ := json.Marshal(next)
		if err := m.storageSet(hlOffKey(n.id), string(value)); err != nil {
			fail("Database error", "unavailable")
			return
		}
		n.hl.setOff(next)
	}
	c.send(ServerMessage{Type: kind + "_success", RequestID: msg.RequestID, SessionID: msg.SessionID,
		Data: map[string]interface{}{"buffer": key, "highlight": *req.Highlight}})
	if changed {
		m.broadcastConfigItem(n, configKeyHighlightOff)
	}
}

// SetHighlightRulesRequest is set_highlight_rules' data.
type SetHighlightRulesRequest struct {
	Rules     json.RawMessage `json:"rules"`
	IfVersion *string         `json:"if_version"`
}

// handleSetHighlightRules replaces a network's rules when if_version is still the stored version
// ("" when never written), and broadcasts them to n's followers.
func (m *Module) handleSetHighlightRules(c *wsConn, msg *ClientMessage) {
	const kind = "set_highlight_rules"
	fail := func(text, code string, data interface{}) {
		c.send(codedError(kind, msg.RequestID, msg.SessionID, text, code, data))
	}
	if msg.SessionID == "" {
		fail("session_id is required", "invalid_request", nil)
		return
	}
	var req SetHighlightRulesRequest
	if len(msg.Data) > 0 {
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			fail("Invalid request data", "invalid_request", nil)
			return
		}
	}
	if len(req.Rules) == 0 || bytes.Equal(bytes.TrimSpace(req.Rules), []byte("null")) {
		fail("rules is required", "invalid_request", nil)
		return
	}
	if req.IfVersion == nil {
		fail("if_version is required", "invalid_request", nil)
		return
	}
	rules, err := highlight.ParseRules(req.Rules)
	if err != nil {
		fail("Invalid rule", "invalid_request", nil)
		return
	}
	if len(rules) > highlight.MaxRules || len(req.Rules) > highlight.MaxBytes { // the array as sent
		fail("Too many highlight rules", "limit", nil)
		return
	}
	// Stored compactly with exactly the rule fields (isEnabled, ruleType, matchType.value).
	normal, err := json.Marshal(rules)
	if err != nil {
		fail("Invalid rule", "invalid_request", nil)
		return
	}
	n := m.networkFor(msg.SessionID)
	if n == nil {
		fail("No access to session", "no_access", nil)
		return
	}
	stored, _, err := m.readHighlightRules(n.id)
	if err != nil {
		fail("Database error", "unavailable", nil)
		return
	}
	if !sameVersion(*req.IfVersion, stored.UpdatedAt) {
		fail("Highlight rules changed", "version_conflict", map[string]interface{}{"version": stored.UpdatedAt})
		return
	}
	next := storedHighlightRules{Rules: json.RawMessage(normal), UpdatedAt: nextVersion(stored.UpdatedAt, m.now())}
	value, _ := json.Marshal(next)
	if err := m.storageSet(hlRulesKey(n.id), string(value)); err != nil {
		fail("Database error", "unavailable", nil)
		return
	}
	_ = n.hl.setRules(next)
	c.send(ServerMessage{Type: kind + "_success", RequestID: msg.RequestID, SessionID: msg.SessionID,
		Data: map[string]interface{}{"version": next.UpdatedAt}})
	m.broadcastConfigItem(n, configKeyHighlightRules)
}

// isHighlight evaluates one stored PRIVMSG / NOTICE (source, raw line) in a buffer: the synced
// switch and rules, with the sender's membership as the network knows it now. A server-sourced
// line is never a highlight.
func (n *network) isHighlight(source, raw, bufName, bufKey string) bool {
	if session.ServerSource(source) {
		return false
	}
	msg, err := irc.Parse(raw)
	if err != nil {
		return false
	}
	text := msg.Param(1)
	if t, ok := ctcpAction(text); ok {
		text = t
	}
	tag, _ := msg.Tag("account")
	l := highlight.Line{Off: n.hl.off[bufKey], DM: bufName != wire.ServerBuffer && !n.isChannel(bufName),
		Sender: msg.Nick(), Text: text, Buffer: bufName}
	if n.hl.rulesActive() {
		l.Account, l.Channels = n.senderContext(l.Sender, tag)
	}
	return highlight.Eval(n.hl.rules, l, n.nick, n.cm)
}

// ctcpAction returns a CTCP ACTION's text.
func ctcpAction(text string) (string, bool) {
	if !strings.HasPrefix(text, "\x01ACTION") {
		return "", false
	}
	t := strings.TrimPrefix(text, "\x01ACTION")
	t = strings.TrimSuffix(t, "\x01")
	return strings.TrimPrefix(t, " "), true
}

// adjustActivity applies the synced settings to the brain's counts (history.activity counts
// highlights with the default evaluation): a buffer whose switch is off has none, and with
// enabled rules a channel's (or Server's) unread lines are counted again under them. closedAt
// gives the device's closed_at_id per buffer key (nil: read from the brain when needed).
func (m *Module) adjustActivity(n *network, deviceID string, act *wire.HistoryActivityResult, closedAt map[string]int64) {
	for i := range act.Buffers {
		a := &act.Buffers[i]
		if n.hl.off[a.Key] {
			a.Highlights = 0
			continue
		}
		if a.Unread == 0 || !n.hl.rulesActive() || (a.Buffer != wire.ServerBuffer && !n.isChannel(a.Buffer)) {
			continue
		}
		if closedAt == nil {
			closedAt = map[string]int64{}
			var bufs wire.DeviceBuffersResult
			if err := m.call(wire.MethodDevicesBuffers, wire.DeviceNetworkParams{ClientID: deviceID, Network: n.id}, &bufs); err == nil {
				for _, b := range bufs.Buffers {
					closedAt[b.Key] = b.ClosedAtID
				}
			}
		}
		if hl, err := m.recountHighlights(n, a, closedAt[a.Key]); err == nil {
			a.Highlights = hl
		}
	}
}

// recountHighlights counts the highlights among a buffer's unread lines (after its read cursor
// and the device's close), with the same filters as history.activity.
func (m *Module) recountHighlights(n *network, a *wire.BufferActivity, closedAt int64) (int, error) {
	var mark wire.ReadMarkerGetResult
	if err := m.call(wire.MethodReadMarkersGet, wire.ReadMarkerGetParams{Network: n.id, Buffer: a.Buffer}, &mark); err != nil {
		return 0, err
	}
	floor := max(mark.ID, closedAt)
	before := a.LastID + 1
	var res wire.HistoryMessagesResult
	if err := m.call(wire.MethodHistoryMessages, wire.HistoryMessagesParams{Network: n.id, Buffer: a.Buffer,
		BeforeID: &before, Limit: getMessagesMaxLimit}, &res); err != nil {
		return 0, err
	}
	hl := 0
	for _, hm := range res.Messages {
		if hm.ID <= floor || (hm.Command != "PRIVMSG" && hm.Command != "NOTICE") {
			continue
		}
		if strings.HasPrefix(hm.Text, "\x01") && !strings.HasPrefix(hm.Text, "\x01ACTION") {
			continue
		}
		if n.isMe(sourceNick(hm.Source)) {
			continue
		}
		if n.isHighlight(hm.Source, hm.Raw, a.Buffer, a.Key) {
			hl++
		}
	}
	return hl, nil
}

func sourceNick(src string) string {
	if i := strings.IndexByte(src, '!'); i >= 0 {
		return src[:i]
	}
	return src
}
