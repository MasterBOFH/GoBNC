package session

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// ClientID identifies a downlink for reply routing.
type ClientID string

// pendingRequest tracks a solicitous command awaiting replies.
type pendingRequest struct {
	Client      ClientID
	ClientLabel string // label from the downlink to echo on replies
	Command     string
	Label       string // uplink label if any
	Target      string // folded nick (WHOIS), WHO mask, or STATS letter
	WHOXToken   string // token we sent upstream
	// WHOXStripToken: we injected 't'; remove querytype from 354 before the client.
	WHOXStripToken bool
	// WHOXClientToken: client's original querytype to restore on 354 (when they sent one).
	WHOXClientToken string
	EndCodes        map[string]bool
	ReplyCodes      map[string]bool // numerics that belong to this exchange (serialized path)
	Created         time.Time
	// Outbound is the uplink line to send. For hold-write kinds (LIST, plain
	// WHO) the second+ request stores it here until the prior end-numeric;
	// Begin never blocks the caller — see holdWrite.
	Outbound irc.Message
	// HoldWrite is true when this request is queued behind an in-flight
	// same-kind stream and must not be written until popSerial.
	HoldWrite bool
	// extra are other downlinks sharing this in-flight exchange. Identical
	// NAMES/MODE/STATS/WHOIS/WHOX requests coalesce onto the first write;
	// replies fan out to Client plus extra. See coalesce.
	extra []*pendingRequest
	// ModeLetters is the normalized list-mode set for a MODE enquiry ("" =
	// MODE #chan / MODE nick; "b" = banlist; "beI" = combined).
	ModeLetters string
	WHOXFlags   string
	WHOXFields  string
	// Remote is the optional server/target argument. Empty is a local enquiry.
	// WHOIS <nick> <nick> uses whoisRemoteNick rather than a hostname.
	Remote string
}

// RouteDest is one downlink that should receive a routed solicitous reply.
type RouteDest struct {
	Client      ClientID
	EchoLabel   string
	StripWHOX   bool
	RestoreWHOX string
}

// stickyRoute delivers one follow-up numeric (e.g. 329 after MODE 324) to the
// same dests after the target enquiry has already ended.
type stickyRoute struct {
	dests []RouteDest
	Code  string
}

// RequestTracker routes solicitous replies to the originating downlink.
type RequestTracker struct {
	mu      sync.Mutex
	labeled map[string]*pendingRequest   // label -> req
	whox    map[string]*pendingRequest   // token -> req
	whois   map[string][]*pendingRequest // folded nick -> waiters (oldest first)
	stats   map[string][]*pendingRequest // stats letter -> waiters (oldest first)
	mode    map[string][]*pendingRequest // modeMapKey(target, letters) -> waiters
	topic   map[string][]*pendingRequest // folded TOPIC channel -> waiters
	names   map[string][]*pendingRequest // folded NAMES channel -> waiters
	serial  map[string][]*pendingRequest // command -> in-flight/queued (LIST, WHO, …)
	ready   []irc.Message                // outbound lines released by popSerial/DropClient
	sticky  map[string]*stickyRoute      // folded target -> one-shot follow-up (MODE 329)
	ircd    string                       // detected IRCd family (irc.IRCd*)
	nextLbl uint64
	nextTok uint64
}

// whoisRemoteNick is ParseWHOISRemote's sentinel for WHOIS <nick> <nick>
// (query the nick's own server). It is not a hostname.
const whoisRemoteNick = "\x00"

// NewRequestTracker creates an empty tracker.
func NewRequestTracker() *RequestTracker {
	return &RequestTracker{
		labeled: make(map[string]*pendingRequest),
		whox:    make(map[string]*pendingRequest),
		whois:   make(map[string][]*pendingRequest),
		stats:   make(map[string][]*pendingRequest),
		mode:    make(map[string][]*pendingRequest),
		topic:   make(map[string][]*pendingRequest),
		names:   make(map[string][]*pendingRequest),
		serial:  make(map[string][]*pendingRequest),
		sticky:  make(map[string]*stickyRoute),
	}
}

// SetIRCd records the detected uplink IRCd family for numeric demux (e.g. MAP).
func (rt *RequestTracker) SetIRCd(ircd string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.ircd = ircd
}

// IRCd returns the detected IRCd family.
func (rt *RequestTracker) IRCd() string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.ircd
}

// endCodes for common solicitous commands.
func endCodesFor(cmd, ircd string) map[string]bool {
	switch cmd {
	case "WHO", "WHOX":
		// 315 is normal end; 403 alone on some ircds (e.g. bahamut) for unknown channels.
		return map[string]bool{"315": true, "403": true, "402": true}
	case "WHOIS":
		// 401 is mid-exchange on ircu (401 then 318); only 318 ends the waiter.
		return map[string]bool{"318": true}
	case "WHOWAS":
		return map[string]bool{"369": true, "431": true}
	case "STATS":
		// 219 = normal end. 402 = no such server on a remote STATS: the
		// server sends it alone, never followed by 219.
		return map[string]bool{"219": true, "402": true}
	case "LIST":
		return map[string]bool{"323": true, "402": true}
	case "NAMES":
		// Most ircds still send 366 for unknown channels; inspircd may send 403 only.
		return map[string]bool{"366": true, "401": true, "403": true}
	case "LINKS":
		return map[string]bool{"365": true, "402": true}
	case "MAP":
		return irc.MAPEndCodes(ircd)
	case "ISON":
		return map[string]bool{"303": true, "461": true}
	case "USERHOST":
		return map[string]bool{"302": true, "461": true}
	case "LUSERS":
		return lusersEndCodes(ircd)
	case "TIME":
		return map[string]bool{"391": true, "402": true}
	case "TRACE":
		// 262 = end; 481 = no privileges; 421 = unknown command on some ircds.
		return map[string]bool{"262": true, "402": true, "481": true, "421": true}
	case "USERS":
		// 394 end-of-list; 395 none; 446 disabled; 266 when aliased to luser stats; 421 unknown.
		return map[string]bool{"394": true, "395": true, "446": true, "266": true, "402": true, "424": true, "421": true}
	case "HELP":
		// RatBox / modern: 704 start, 705 text, 706 end.
		return map[string]bool{"706": true, "524": true} // 524 = ERR_HELPNOTFOUND where used
	case "ADMIN":
		// RFC 2812: 256–259; 259 is last. 402 = no such server.
		return map[string]bool{"259": true, "402": true}
	case "MODE":
		// Channel modes: 324 (329 follows via sticky). Lists: *68/*49/*47/*29.
		// User modes: 221. Errors clear a stuck enquiry.
		// Note: 467/501/502 are MODE *change* errors (not list enquiry); see docker probe.
		return map[string]bool{
			"221": true, "324": true,
			"368": true, "349": true, "347": true, "729": true,
			"401": true, "403": true, "442": true, "461": true, "472": true, "482": true,
		}
	case "TOPIC":
		// 331 = no topic; 332 then 333 (333 ends). Errors clear the enquiry.
		return map[string]bool{"331": true, "333": true, "401": true, "403": true, "442": true, "461": true}
	case "SILENCE":
		// ircu2: 271 RPL_SILELIST, 272 RPL_ENDOFSILELIST (always sent, even if empty).
		return map[string]bool{"272": true}
	default:
		return map[string]bool{}
	}
}

// lusersEndCodes: modern ircds finish with 266; classic ircu stops at 255 (no 265/266).
func lusersEndCodes(ircd string) map[string]bool {
	switch ircd {
	case irc.IRCdIrcu, irc.IRCdSnircd:
		return map[string]bool{"255": true}
	default:
		return map[string]bool{"266": true}
	}
}

// replyCodesFor are numerics attributed to a serialized solicitous exchange.
func replyCodesFor(cmd, ircd string) map[string]bool {
	switch cmd {
	case "WHO", "WHOX":
		return map[string]bool{"352": true, "315": true, "354": true, "402": true, "403": true}
	case "WHOWAS":
		return map[string]bool{
			"314": true, "312": true, "330": true, "338": true,
			"406": true, "369": true, "431": true,
		}
	case "LIST":
		return map[string]bool{"321": true, "322": true, "323": true, "402": true}
	case "NAMES":
		return map[string]bool{"353": true, "366": true, "401": true, "403": true}
	case "LINKS":
		return map[string]bool{"364": true, "365": true, "402": true}
	case "STATS":
		return map[string]bool{
			"211": true, "212": true, "213": true, "214": true, "215": true,
			"216": true, "217": true, "218": true, "219": true,
			"241": true, "242": true, "243": true, "244": true, "245": true,
			"246": true, "247": true, "248": true, "249": true, "250": true,
			"402": true,
		}
	case "MAP":
		return irc.MAPReplyCodes(ircd)
	case "ISON":
		return map[string]bool{"303": true, "461": true}
	case "USERHOST":
		return map[string]bool{"302": true, "461": true}
	case "LUSERS":
		return map[string]bool{
			"250": true, "251": true, "252": true, "253": true, "254": true, "255": true,
			"265": true, "266": true,
		}
	case "TIME":
		return map[string]bool{"391": true, "402": true}
	case "TRACE":
		return map[string]bool{
			"200": true, "201": true, "202": true, "203": true, "204": true,
			"205": true, "206": true, "208": true, "261": true, "262": true,
			"402": true, "481": true, "421": true,
		}
	case "USERS":
		return map[string]bool{
			"265": true, "266": true,
			"392": true, "393": true, "394": true, "395": true,
			"402": true, "424": true, "446": true, "421": true,
		}
	case "HELP":
		return map[string]bool{"704": true, "705": true, "706": true, "524": true}
	case "ADMIN":
		return map[string]bool{"256": true, "257": true, "258": true, "259": true, "402": true}
	case "MODE":
		return map[string]bool{
			"221": true,
			"324": true, "329": true,
			"367": true, "368": true,
			"348": true, "349": true,
			"346": true, "347": true,
			"728": true, "729": true,
			"401": true, "403": true, "442": true, "461": true, "472": true, "482": true,
		}
	case "TOPIC":
		return map[string]bool{"331": true, "332": true, "333": true, "401": true, "403": true, "442": true, "461": true}
	case "SILENCE":
		return map[string]bool{"271": true, "272": true}
	default:
		return endCodesFor(cmd, ircd)
	}
}

// IsSolicitous reports whether cmd needs reply routing.
func IsSolicitous(cmd string) bool {
	switch cmd {
	case "WHO", "WHOIS", "WHOWAS", "STATS", "LIST", "LINKS", "NAMES",
		"USERHOST", "ISON",
		"LUSERS", "TIME", "TRACE", "USERS",
		"MAP", "HELP", "ADMIN":
		return true
	default:
		return false
	}
}

// BeginOpts configures registration of an outbound solicitous command.
type BeginOpts struct {
	Client          ClientID
	Cmd             string
	ClientLabel     string
	PreferLabel     bool
	PreferWHOX      bool
	WhoisTargets    []string    // folded nicks
	WHOMask         string      // WHO mask (for 315 matching)
	StatsLetter     string      // folded STATS query letter ("" if none)
	EnquiryTarget   string      // folded MODE/TOPIC/NAMES target (channel or nick)
	ModeLetters     string      // MODE list letters as the client sent them (normalized in Begin)
	WHOXFlags       string      // WHOX flags before '%'; must match to coalesce
	WHOXFields      string      // WHOX field letters (client form); 't' ignored for coalesce
	WHOXClientToken string      // client's original querytype; restored on 354, not a coalesce key
	WhoisWire       *[]string   // output: nicks that still need an uplink WHOIS
	Remote          string      // optional server/target; empty = local; whoisRemoteNick = WHOIS nick nick
	Outbound        irc.Message // uplink line; queued when writeNow is false
}

// Begin registers an outbound solicitous command.
// writeNow is true when the caller should send Outbound immediately.
// writeNow is false when:
//   - a hold-write kind (LIST, plain WHO, …) already has the same command in
//     flight — the line sits on the request until TakeReady after the end-numeric;
//   - an identical keyed enquiry is already in flight (NAMES same channel
//     and server, MODE same channel+letters, STATS same token and server,
//     WHOIS same nick and local/remote form, WHOX same flags+fields+mask —
//     the querytype token is not part of the match)
//     — the caller is attached as a coalesced recipient of that one uplink
//     exchange. Local and remote forms (STATS c vs STATS c server, WHOIS nick
//     vs WHOIS nick nick) are never coalesced.
//
// Begin never blocks.
// For WHOX, the returned token is numeric 1–999 (ircu/IRCv3 requirement).
func (rt *RequestTracker) Begin(opts BeginOpts) (label, whoxToken string, writeNow bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	cmd := opts.Cmd
	req := &pendingRequest{
		Client:          opts.Client,
		ClientLabel:     opts.ClientLabel,
		Command:         cmd,
		Target:          opts.EnquiryTarget,
		WHOXFlags:       opts.WHOXFlags,
		WHOXFields:      opts.WHOXFields,
		WHOXClientToken: opts.WHOXClientToken,
		Remote:          opts.Remote,
		EndCodes:        endCodesFor(cmd, rt.ircd),
		ReplyCodes:      replyCodesFor(cmd, rt.ircd),
		Created:         time.Now(),
		Outbound:        opts.Outbound,
	}
	if opts.PreferLabel {
		rt.nextLbl++
		label = formatID("L", rt.nextLbl)
		req.Label = label
		rt.labeled[label] = req
		return label, "", true
	}
	if opts.PreferWHOX && (cmd == "WHO" || cmd == "WHOX") {
		req.Target = opts.WHOMask
		if existing := rt.findMatchingWHOX(req); existing != nil {
			existing.coalesce(req)
			return "", "", false
		}
		rt.nextTok++
		if rt.nextTok > 999 {
			rt.nextTok = 1
		}
		whoxToken = strconv.FormatUint(rt.nextTok, 10)
		req.WHOXToken = whoxToken
		rt.whox[whoxToken] = req
		return "", whoxToken, true
	}
	// Unlabeled WHOIS: route by target nick. A nick already in flight with
	// the same local/remote form coalesces; a different form (WHOIS nick vs
	// WHOIS nick nick vs WHOIS server nick) is a separate enquiry — the
	// write is held until the in-flight nick exchange ends.
	if cmd == "WHOIS" && len(opts.WhoisTargets) > 0 {
		var need []string
		for _, nick := range opts.WhoisTargets {
			if nick == "" {
				continue
			}
			w := &pendingRequest{
				Client:      opts.Client,
				ClientLabel: opts.ClientLabel,
				Command:     "WHOIS",
				Target:      nick,
				Remote:      opts.Remote,
				EndCodes:    endCodesFor("WHOIS", rt.ircd),
				Created:     time.Now(),
				Outbound:    whoisOutbound(nick, opts.Remote),
			}
			if q := rt.whois[nick]; len(q) > 0 {
				if existing := findRemoteMatch(q, opts.Remote); existing != nil {
					existing.coalesce(w)
					continue
				}
				w.HoldWrite = true
				rt.whois[nick] = append(q, w)
				continue
			}
			rt.whois[nick] = []*pendingRequest{w}
			need = append(need, nick)
		}
		if opts.WhoisWire != nil {
			*opts.WhoisWire = need
		}
		return "", "", len(need) > 0
	}
	// STATS: demux by query token. Same token+server coalesces; same token
	// with a different server is held (219 does not name the target server).
	if cmd == "STATS" && opts.StatsLetter != "" {
		letter := opts.StatsLetter
		req.Target = letter
		if q := rt.stats[letter]; len(q) > 0 {
			if existing := findRemoteMatch(q, opts.Remote); existing != nil {
				existing.coalesce(req)
				return "", "", false
			}
			req.HoldWrite = true
			rt.stats[letter] = append(q, req)
			return "", "", false
		}
		rt.stats[letter] = []*pendingRequest{req}
		return "", "", true
	}
	// TOPIC: demux by channel (no coalesce — not in the identical-enquiry set).
	if cmd == "TOPIC" && opts.EnquiryTarget != "" {
		rt.topic[opts.EnquiryTarget] = append(rt.topic[opts.EnquiryTarget], req)
		return "", "", true
	}
	// MODE: demux by channel/nick + list letters. Same (target, letters)
	// coalesces; MODE #c b vs MODE #c e both write.
	if cmd == "MODE" && opts.EnquiryTarget != "" {
		letters := normalizeModeListLetters(opts.ModeLetters)
		req.ModeLetters = letters
		key := modeMapKey(opts.EnquiryTarget, letters)
		if q := rt.mode[key]; len(q) > 0 {
			q[0].coalesce(req)
			return "", "", false
		}
		rt.mode[key] = []*pendingRequest{req}
		return "", "", true
	}
	// NAMES: 353/366 carry the channel. Duplicate NAMES for the same
	// channel and server shares the in-flight write; NAMES #c vs
	// NAMES #c server are different enquiries (held, not coalesced).
	if cmd == "NAMES" && opts.EnquiryTarget != "" {
		if q := rt.names[opts.EnquiryTarget]; len(q) > 0 {
			if existing := findRemoteMatch(q, opts.Remote); existing != nil {
				existing.coalesce(req)
				return "", "", false
			}
			req.HoldWrite = true
			rt.names[opts.EnquiryTarget] = append(q, req)
			return "", "", false
		}
		rt.names[opts.EnquiryTarget] = []*pendingRequest{req}
		return "", "", true
	}
	// Remaining commands: per-kind queue. LIST and plain WHO (and other
	// streams whose replies cannot be split) hold the write; ISON/USERHOST
	// send immediately and FIFO the unique end-numeric. Commands with an
	// optional server (TIME, ADMIN, …) hold a different local/remote form
	// so those replies are not mixed.
	q := rt.serial[cmd]
	req.HoldWrite = len(q) > 0 && (holdWrite(cmd) || (takesServerTarget(cmd) && serialHasOtherRemote(q, opts.Remote)))
	rt.serial[cmd] = append(q, req)
	return "", "", !req.HoldWrite
}

// holdWrite reports whether a second in-flight command of this kind must
// wait to be *written* until the first's end-numeric. True when replies
// are an unkeyed stream (LIST 322s, WHO 352s). False when each reply
// uniquely identifies the exchange (ISON 303, USERHOST 302, TIME 391).
func holdWrite(cmd string) bool {
	switch cmd {
	case "LIST", "WHO", "WHOX", "NAMES",
		"LINKS", "MAP", "LUSERS", "TRACE", "USERS",
		"HELP", "ADMIN", "SILENCE", "WHOWAS":
		return true
	default:
		return false
	}
}

// SetWHOXClientFix records how to rewrite 354 replies for this uplink token:
// injectT → strip the querytype field; otherwise restore clientToken when non-empty.
func (rt *RequestTracker) SetWHOXClientFix(uplinkToken string, injectT bool, clientToken string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if req, ok := rt.whox[uplinkToken]; ok {
		req.WHOXStripToken = injectT
		if !injectT {
			req.WHOXClientToken = clientToken
		}
		for _, e := range req.extra {
			e.fixWHOXRewrite(req.WHOXStripToken)
		}
	}
}

// ActiveClient returns the client owning the head of any serialized
// (hold-write or FIFO) command queue, if any.
func (rt *RequestTracker) ActiveClient() (ClientID, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, kind := range serialKindOrder {
		if q := rt.serial[kind]; len(q) > 0 {
			return q[0].Client, true
		}
	}
	return "", false
}

// TakeReady returns uplink lines that became writable because a prior
// hold-write exchange ended (or its client dropped). Caller must write
// them; Begin itself never waits.
func (rt *RequestTracker) TakeReady() []irc.Message {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	out := rt.ready
	rt.ready = nil
	return out
}

// RouteAll returns the downlinks that should receive msg.
// An empty result means broadcast (or drop — HandleMessage fans out to every downlink).
// Replies for a coalesced enquiry are returned as one dest per waiter.
func (rt *RequestTracker) RouteAll(msg irc.Message, cm irc.CaseMapping) []RouteDest {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.routeLocked(msg, cm)
}

// RouteMessage is RouteAll for the single-waiter case tests still use.
// only is true when dests is non-empty; client/echo/WHOX rewrite are dests[0].
func (rt *RequestTracker) RouteMessage(msg irc.Message, cm irc.CaseMapping) (client ClientID, only bool, echoLabel string, stripWHOXToken bool, restoreWHOXToken string) {
	dests := rt.RouteAll(msg, cm)
	if len(dests) == 0 {
		return "", false, "", false, ""
	}
	d := dests[0]
	return d.Client, true, d.EchoLabel, d.StripWHOX, d.RestoreWHOX
}

func (rt *RequestTracker) routeLocked(msg irc.Message, cm irc.CaseMapping) []RouteDest {
	if lbl, ok := msg.Tag("label"); ok && lbl != "" {
		if req, ok := rt.labeled[lbl]; ok {
			if req.EndCodes[msg.Command] {
				// MODE 324 is often followed by 329; keep a per-target sticky.
				if req.Command == "MODE" && msg.Command == "324" && req.ReplyCodes["329"] {
					target := req.Target
					if target == "" && len(msg.Params) > 1 {
						target = cm.Canonical(msg.Params[1])
					}
					if target != "" {
						rt.sticky[target] = &stickyRoute{dests: destsOf(req), Code: "329"}
					}
				}
				delete(rt.labeled, lbl)
			}
			return destsOf(req)
		}
	}

	// One-shot follow-up (RPL_CREATIONTIME after channel MODE enquiry).
	if msg.Command == "329" && len(msg.Params) > 1 {
		target := cm.Canonical(msg.Params[1])
		if s := rt.sticky[target]; s != nil && s.Code == "329" {
			delete(rt.sticky, target)
			return s.dests
		}
	}

	if dests := rt.routeModeLocked(msg, cm); dests != nil {
		return dests
	}

	// TOPIC enquiry numerics: demux by channel.
	if target := topicEnquiryNumericTarget(msg, cm); target != "" {
		if dests := rt.routeTargetQueue(rt.topic, target, msg.Command, false); dests != nil {
			return dests
		}
	}

	// NAMES: 353/366 carry the channel — concurrent across channels; same
	// channel is one coalesced exchange.
	if target := namesNumericTarget(msg, cm); target != "" {
		if dests := rt.routeTargetQueue(rt.names, target, msg.Command, false); dests != nil {
			return dests
		}
	}

	// WHOIS numerics: route by target nick (params[1]).
	if irc.IsWHOISReply(msg.Command, rt.ircd) && len(msg.Params) > 1 {
		nick := cm.Canonical(msg.Params[1])
		if waiters := rt.whois[nick]; len(waiters) > 0 {
			req := waiters[0]
			dests := destsOf(req)
			if req.EndCodes[msg.Command] {
				rt.popWaiters(rt.whois, nick)
			}
			return dests
		}
	}

	// STATS 402 ERR_NOSUCHSERVER: the whole reply to a remote STATS the
	// server can't route (no 219 follows). params[1] is the server mask the
	// client sent, so match it against the in-flight (head) waiter's Remote.
	// Only heads can be in flight — same-letter/different-remote requests
	// behind them are HoldWrite and haven't been written yet.
	if msg.Command == "402" && len(msg.Params) > 1 {
		remote := foldServer(msg.Params[1])
		for letter, waiters := range rt.stats {
			if len(waiters) == 0 || waiters[0].Remote == "" || waiters[0].Remote != remote {
				continue
			}
			dests := destsOf(waiters[0])
			rt.popWaiters(rt.stats, letter)
			return dests
		}
	}
	// STATS: 219 carries the query letter in params[1].
	if msg.Command == "219" && len(msg.Params) > 1 {
		letter := foldStatsLetter(msg.Params[1])
		if waiters := rt.stats[letter]; len(waiters) > 0 {
			req := waiters[0]
			dests := destsOf(req)
			rt.popWaiters(rt.stats, letter)
			return dests
		}
	}
	// Intermediate STATS numerics: only when a single letter is pending.
	if isSTATSNumeric(msg.Command) && msg.Command != "219" {
		if _, req, ok := rt.singleStatsPending(); ok {
			return destsOf(req)
		}
	}

	// WHOX: token is params[1] when 't' was requested (fixed field order).
	// A real (non-"0") token that doesn't match any pending request is not
	// a server quirk to guess around — it means the original requester is
	// already gone (e.g. disconnected before the reply arrived). Only
	// ircds known to default/mangle the token (ircu/snircd, which send
	// literal "0") get the leniency below; a genuinely echoed-but-unknown
	// token is dropped unconditionally rather than misrouted or broadcast.
	whoxQuirkyIRCd := rt.ircd == irc.IRCdIrcu || rt.ircd == irc.IRCdSnircd
	if msg.Command == "354" && len(msg.Params) > 1 {
		tok := msg.Params[1]
		if req, ok := rt.whox[tok]; ok {
			return destsOf(req)
		}
		if tok != "0" {
			return nil
		}
	}
	if msg.Command == "315" {
		mask := ""
		if len(msg.Params) > 1 {
			mask = cm.Canonical(msg.Params[1])
		}
		if req, tok, ok := rt.whoxByMask(mask); ok {
			delete(rt.whox, tok)
			return destsOf(req)
		}
		// Fallback: oldest pending WHOX, ircu/snircd only — those ircds can
		// normalize the mask on RPL_ENDOFWHO so the exact match above misses.
		if whoxQuirkyIRCd {
			var best *pendingRequest
			var bestTok string
			for tok, req := range rt.whox {
				if req.Command != "WHO" && req.Command != "WHOX" {
					continue
				}
				if best == nil || req.Created.Before(best.Created) {
					best = req
					bestTok = tok
				}
			}
			if best != nil {
				delete(rt.whox, bestTok)
				return destsOf(best)
			}
		}
	}

	// Fallback: single pending WHOX request (token mismatch / server
	// defaulted querytype to "0") — ircu/snircd only.
	if whoxQuirkyIRCd && (msg.Command == "354" || msg.Command == "315") && len(rt.whox) == 1 {
		for tok, req := range rt.whox {
			if msg.Command == "315" {
				delete(rt.whox, tok)
			}
			return destsOf(req)
		}
	}

	if kind, req, ok := rt.serialHeadFor(msg.Command); ok {
		dests := destsOf(req)
		if req.EndCodes[msg.Command] {
			rt.popSerial(kind)
		}
		return dests
	}
	return nil
}

// routeTargetQueue delivers a numeric to the in-flight waiter for target
// (plus coalesced extras). sticky324 is unused here — MODE has its own path.
func (rt *RequestTracker) routeTargetQueue(queues map[string][]*pendingRequest, target, cmd string, sticky324 bool) []RouteDest {
	waiters := queues[target]
	if len(waiters) == 0 {
		return nil
	}
	req := waiters[0]
	if len(req.ReplyCodes) > 0 && !req.ReplyCodes[cmd] {
		return nil
	}
	dests := destsOf(req)
	if req.EndCodes[cmd] {
		if sticky324 && req.Command == "MODE" && cmd == "324" && req.ReplyCodes["329"] {
			rt.sticky[target] = &stickyRoute{dests: dests, Code: "329"}
		}
		rt.popWaiters(queues, target)
	}
	return dests
}

func (rt *RequestTracker) routeModeLocked(msg irc.Message, cm irc.CaseMapping) []RouteDest {
	target := modeEnquiryNumericTarget(msg, cm)
	if target == "" {
		return nil
	}
	if isModeEnquiryError(msg.Command) {
		return rt.popAllModeTarget(target, msg.Command)
	}
	letter, ok := modeNumericLetter(msg.Command)
	if !ok {
		return nil
	}
	key, waiters := rt.findModeWaiters(target, letter)
	if len(waiters) == 0 {
		return nil
	}
	req := waiters[0]
	if len(req.ReplyCodes) > 0 && !req.ReplyCodes[msg.Command] {
		return nil
	}
	dests := destsOf(req)
	if req.EndCodes[msg.Command] {
		if req.Command == "MODE" && msg.Command == "324" && req.ReplyCodes["329"] {
			rt.sticky[target] = &stickyRoute{dests: dests, Code: "329"}
		}
		rt.mode[key] = waiters[1:]
		if len(rt.mode[key]) == 0 {
			delete(rt.mode, key)
		}
	}
	return dests
}

func (rt *RequestTracker) findModeWaiters(target, letter string) (key string, waiters []*pendingRequest) {
	exact := modeMapKey(target, letter)
	if q := rt.mode[exact]; len(q) > 0 {
		return exact, q
	}
	if letter != "" {
		var bestKey string
		var bestCreated time.Time
		found := false
		for k, q := range rt.mode {
			if len(q) == 0 || modeKeyTarget(k) != target {
				continue
			}
			if !strings.Contains(modeKeyLetters(k), letter) {
				continue
			}
			if !found || q[0].Created.Before(bestCreated) {
				bestKey = k
				bestCreated = q[0].Created
				found = true
			}
		}
		if found {
			return bestKey, rt.mode[bestKey]
		}
	}
	// Fallback: oldest waiter on this target (Begin without ModeLetters).
	var bestKey string
	var bestCreated time.Time
	found := false
	for k, q := range rt.mode {
		if len(q) == 0 || modeKeyTarget(k) != target {
			continue
		}
		if !found || q[0].Created.Before(bestCreated) {
			bestKey = k
			bestCreated = q[0].Created
			found = true
		}
	}
	if found {
		return bestKey, rt.mode[bestKey]
	}
	return "", nil
}

// popAllModeTarget fans an error numeric to every MODE waiter for target and
// clears them (the channel/nick is gone; every enquiry for it is finished).
func (rt *RequestTracker) popAllModeTarget(target, cmd string) []RouteDest {
	var dests []RouteDest
	var keys []string
	for k, q := range rt.mode {
		if modeKeyTarget(k) != target || len(q) == 0 {
			continue
		}
		if len(q[0].ReplyCodes) > 0 && !q[0].ReplyCodes[cmd] {
			continue
		}
		dests = append(dests, destsOf(q[0])...)
		keys = append(keys, k)
	}
	for _, k := range keys {
		delete(rt.mode, k)
	}
	return dests
}

// modeEnquiryNumericTarget extracts the folded channel/nick from a MODE enquiry reply.
func modeEnquiryNumericTarget(msg irc.Message, cm irc.CaseMapping) string {
	switch msg.Command {
	case "221": // RPL_UMODEIS <nick> <modes>
		if len(msg.Params) > 0 {
			return cm.Canonical(msg.Params[0])
		}
	case "324", "329", "367", "368", "348", "349", "346", "347", "728", "729",
		"403", "442", "472", "482":
		if len(msg.Params) > 1 {
			return cm.Canonical(msg.Params[1])
		}
	case "401", "461":
		if len(msg.Params) > 1 {
			return cm.Canonical(msg.Params[1])
		}
	}
	return ""
}

// topicEnquiryNumericTarget extracts the folded channel from a TOPIC enquiry reply.
func topicEnquiryNumericTarget(msg irc.Message, cm irc.CaseMapping) string {
	switch msg.Command {
	case "331", "332", "333", "401", "403", "442", "461":
		if len(msg.Params) > 1 {
			return cm.Canonical(msg.Params[1])
		}
	}
	return ""
}

// namesNumericTarget extracts the folded channel from a NAMES reply.
func namesNumericTarget(msg irc.Message, cm irc.CaseMapping) string {
	switch msg.Command {
	case "353": // <nick> <type> <channel> :<names>
		if len(msg.Params) > 2 {
			return cm.Canonical(msg.Params[2])
		}
	case "366", "401", "403":
		if len(msg.Params) > 1 {
			return cm.Canonical(msg.Params[1])
		}
	}
	return ""
}

func (rt *RequestTracker) whoxByMask(mask string) (*pendingRequest, string, bool) {
	if mask == "" {
		return nil, "", false
	}
	var best *pendingRequest
	var bestTok string
	for tok, req := range rt.whox {
		if req.Target == "" || req.Target != mask {
			continue
		}
		if best == nil || req.Created.Before(best.Created) {
			best = req
			bestTok = tok
		}
	}
	if best == nil {
		return nil, "", false
	}
	return best, bestTok, true
}

func (rt *RequestTracker) singleStatsPending() (letter string, req *pendingRequest, ok bool) {
	for let, waiters := range rt.stats {
		if len(waiters) == 0 {
			continue
		}
		if ok {
			return "", nil, false // more than one letter
		}
		letter, req, ok = let, waiters[0], true
	}
	return letter, req, ok
}

var serialKindOrder = []string{
	"LIST", "WHO", "WHOX", "NAMES", "ISON", "USERHOST", "LUSERS",
	"LINKS", "MAP", "WHOWAS", "TIME", "TRACE", "USERS", "HELP", "ADMIN", "SILENCE",
}

func (rt *RequestTracker) serialHeadFor(numeric string) (kind string, req *pendingRequest, ok bool) {
	for _, kind := range serialKindOrder {
		q := rt.serial[kind]
		if len(q) == 0 {
			continue
		}
		head := q[0]
		if head.ReplyCodes[numeric] || (len(head.ReplyCodes) == 0 && isLikelyReply(numeric)) {
			return kind, head, true
		}
	}
	return "", nil, false
}

func (rt *RequestTracker) popSerial(kind string) {
	q := rt.serial[kind]
	if len(q) == 0 {
		return
	}
	rt.serial[kind] = q[1:]
	if len(rt.serial[kind]) == 0 {
		delete(rt.serial, kind)
		return
	}
	next := rt.serial[kind][0]
	if next.HoldWrite {
		next.HoldWrite = false
		if next.Outbound.Command != "" || next.Outbound.Raw != "" {
			rt.ready = append(rt.ready, next.Outbound)
		}
	}
}

func (rt *RequestTracker) enqueueHeldWrite(req *pendingRequest) {
	if req == nil || !req.HoldWrite {
		return
	}
	req.HoldWrite = false
	if req.Outbound.Command != "" || req.Outbound.Raw != "" {
		rt.ready = append(rt.ready, req.Outbound)
	}
}

func (rt *RequestTracker) popWaiters(queues map[string][]*pendingRequest, key string) {
	q := queues[key]
	if len(q) == 0 {
		return
	}
	queues[key] = q[1:]
	if len(queues[key]) == 0 {
		delete(queues, key)
		return
	}
	rt.enqueueHeldWrite(queues[key][0])
}

func findRemoteMatch(q []*pendingRequest, remote string) *pendingRequest {
	for _, req := range q {
		if req.Remote == remote {
			return req
		}
	}
	return nil
}

func serialHasOtherRemote(q []*pendingRequest, remote string) bool {
	for _, req := range q {
		if req.Remote != remote {
			return true
		}
	}
	return false
}

func takesServerTarget(cmd string) bool {
	switch cmd {
	case "TIME", "ADMIN", "USERS", "TRACE", "MAP", "LIST", "LINKS", "LUSERS", "WHOWAS":
		return true
	default:
		return false
	}
}

func whoisOutbound(nick, remote string) irc.Message {
	switch remote {
	case "":
		return irc.Message{Command: "WHOIS", Params: []string{nick}}
	case whoisRemoteNick:
		return irc.Message{Command: "WHOIS", Params: []string{nick, nick}}
	default:
		return irc.Message{Command: "WHOIS", Params: []string{remote, nick}}
	}
}

func foldServer(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// ParseWHOISRemote returns the WHOIS server/target identity.
// Empty = local WHOIS <nick>. whoisRemoteNick = WHOIS <nick> <nick>.
// Otherwise the folded explicit server from WHOIS <server> <nick>.
func ParseWHOISRemote(params []string, cm irc.CaseMapping) string {
	if len(params) < 2 || params[1] == "" {
		return ""
	}
	if cm.Equal(params[0], params[1]) {
		return whoisRemoteNick
	}
	return foldServer(params[0])
}

// EnquiryRemote extracts the optional server/target argument for a solicitous command.
func EnquiryRemote(cmd string, params []string, cm irc.CaseMapping) string {
	switch cmd {
	case "WHOIS":
		return ParseWHOISRemote(params, cm)
	case "STATS":
		if len(params) > 1 {
			return foldServer(params[1])
		}
	case "NAMES", "LIST":
		if len(params) > 1 {
			return foldServer(params[1])
		}
	case "TIME", "ADMIN", "USERS", "TRACE", "MAP":
		if len(params) > 0 {
			return foldServer(params[0])
		}
	case "LUSERS":
		if len(params) > 1 {
			return foldServer(params[1])
		}
	case "LINKS":
		if len(params) > 0 {
			return foldServer(params[0])
		}
	case "WHOWAS":
		if len(params) > 2 {
			return foldServer(params[2])
		}
	}
	return ""
}

func (rt *RequestTracker) dropQueueClient(queues map[string][]*pendingRequest, client ClientID) {
	for target, waiters := range queues {
		var kept []*pendingRequest
		droppedHead := false
		for i, req := range waiters {
			if req.dropClient(client) {
				if i == 0 {
					droppedHead = true
				}
				continue
			}
			kept = append(kept, req)
		}
		if len(kept) == 0 {
			delete(queues, target)
			continue
		}
		queues[target] = kept
		if droppedHead {
			rt.enqueueHeldWrite(kept[0])
		}
	}
}

func destsOf(req *pendingRequest) []RouteDest {
	if req == nil {
		return nil
	}
	out := []RouteDest{{
		Client:      req.Client,
		EchoLabel:   req.ClientLabel,
		StripWHOX:   req.WHOXStripToken,
		RestoreWHOX: req.WHOXClientToken,
	}}
	for _, e := range req.extra {
		out = append(out, RouteDest{
			Client:      e.Client,
			EchoLabel:   e.ClientLabel,
			StripWHOX:   e.WHOXStripToken,
			RestoreWHOX: e.WHOXClientToken,
		})
	}
	return out
}

func (req *pendingRequest) coalesce(other *pendingRequest) {
	if other == nil || other.Client == req.Client {
		return
	}
	for _, e := range req.extra {
		if e.Client == other.Client {
			return
		}
	}
	other.fixWHOXRewrite(req.WHOXStripToken)
	req.extra = append(req.extra, other)
}

// fixWHOXRewrite keeps this waiter's own querytype token. Strip the injected
// 't' field only when this client did not send a token.
func (req *pendingRequest) fixWHOXRewrite(headInjectedT bool) {
	if req.WHOXClientToken != "" {
		req.WHOXStripToken = false
		return
	}
	req.WHOXStripToken = headInjectedT
}

// dropClient removes client from this exchange. True means the request is empty.
func (req *pendingRequest) dropClient(client ClientID) bool {
	if req.Client == client {
		if len(req.extra) == 0 {
			return true
		}
		head := req.extra[0]
		req.Client = head.Client
		req.ClientLabel = head.ClientLabel
		req.WHOXStripToken = head.WHOXStripToken
		req.WHOXClientToken = head.WHOXClientToken
		req.extra = req.extra[1:]
		return false
	}
	var kept []*pendingRequest
	for _, e := range req.extra {
		if e.Client != client {
			kept = append(kept, e)
		}
	}
	req.extra = kept
	return false
}

func (rt *RequestTracker) findMatchingWHOX(req *pendingRequest) *pendingRequest {
	for _, existing := range rt.whox {
		if existing.Target == req.Target &&
			existing.WHOXFlags == req.WHOXFlags &&
			whoxFieldsKey(existing.WHOXFields) == whoxFieldsKey(req.WHOXFields) {
			return existing
		}
	}
	return nil
}

// whoxFieldsKey drops querytype 't' so two WHOX that differ only by token
// (o%nuhs vs o%nuhst,9) still match. Other field letters must be identical.
func whoxFieldsKey(fields string) string {
	var b strings.Builder
	for i := 0; i < len(fields); i++ {
		c := fields[i]
		if c == 't' || c == 'T' {
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

func modeMapKey(target, letters string) string {
	return target + "\x00" + letters
}

func modeKeyTarget(key string) string {
	if i := strings.IndexByte(key, 0); i >= 0 {
		return key[:i]
	}
	return key
}

func modeKeyLetters(key string) string {
	if i := strings.IndexByte(key, 0); i >= 0 {
		return key[i+1:]
	}
	return ""
}

func normalizeModeListLetters(modes string) string {
	var seen [256]bool
	for i := 0; i < len(modes); i++ {
		c := modes[i]
		if c == '+' || c == '-' {
			continue
		}
		seen[c] = true
	}
	var b []byte
	for _, c := range []byte("beIq") {
		if seen[c] {
			b = append(b, c)
			seen[c] = false
		}
	}
	for c := 0; c < 256; c++ {
		if seen[c] {
			b = append(b, byte(c))
		}
	}
	return string(b)
}

func modeNumericLetter(cmd string) (letter string, ok bool) {
	switch cmd {
	case "221", "324", "329":
		return "", true
	case "367", "368":
		return "b", true
	case "348", "349":
		return "e", true
	case "346", "347":
		return "I", true
	case "728", "729":
		return "q", true
	default:
		return "", false
	}
}

func isModeEnquiryError(cmd string) bool {
	switch cmd {
	case "401", "403", "442", "461", "472", "482":
		return true
	default:
		return false
	}
}

// DropClient removes pending requests for a disconnected client.
func (rt *RequestTracker) DropClient(client ClientID) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for k, req := range rt.labeled {
		if req.dropClient(client) {
			delete(rt.labeled, k)
		}
	}
	for k, req := range rt.whox {
		if req.dropClient(client) {
			delete(rt.whox, k)
		}
	}
	rt.dropQueueClient(rt.whois, client)
	rt.dropQueueClient(rt.stats, client)
	rt.dropQueueClient(rt.mode, client)
	rt.dropQueueClient(rt.topic, client)
	rt.dropQueueClient(rt.names, client)
	for target, s := range rt.sticky {
		var kept []RouteDest
		for _, d := range s.dests {
			if d.Client != client {
				kept = append(kept, d)
			}
		}
		if len(kept) == 0 {
			delete(rt.sticky, target)
		} else {
			s.dests = kept
		}
	}
	for kind, q := range rt.serial {
		var kept []*pendingRequest
		droppedHead := len(q) > 0 && q[0].Client == client
		for _, req := range q {
			if req.Client == client {
				continue
			}
			kept = append(kept, req)
		}
		if len(kept) == 0 {
			delete(rt.serial, kind)
			continue
		}
		rt.serial[kind] = kept
		if droppedHead {
			rt.enqueueHeldWrite(kept[0])
		}
	}
}

// isSTATSNumeric reports numerics that commonly belong to a STATS exchange.
func isSTATSNumeric(cmd string) bool {
	switch cmd {
	case "211", "212", "213", "214", "215", "216", "217", "218", "219",
		"241", "242", "243", "244", "245", "246", "247", "248", "249", "250":
		return true
	default:
		return false
	}
}

// ParseWHOISTargets extracts target nick(s) from WHOIS params.
// WHOIS <nick[,nick...]> | WHOIS <nick> <nick> | WHOIS <server> <nick[,nick...]>
func ParseWHOISTargets(params []string) []string {
	if len(params) == 0 {
		return nil
	}
	nickParam := params[0]
	if len(params) >= 2 {
		if strings.EqualFold(params[0], params[1]) {
			nickParam = params[0]
		} else {
			nickParam = params[1]
		}
	}
	var out []string
	for _, n := range strings.Split(nickParam, ",") {
		n = strings.TrimSpace(n)
		if n != "" {
			out = append(out, n)
		}
	}
	return out
}

// ParseStatsLetter returns the folded STATS query token, or "" if absent.
func ParseStatsLetter(params []string) string {
	if len(params) == 0 || params[0] == "" {
		return ""
	}
	return foldStatsLetter(params[0])
}

// foldStatsLetter folds a STATS query token for use as a map key. Most ircds
// use a single-letter type (m, u, l, …), but some (e.g. ircu's "iauth") use
// multi-character tokens, so the whole token is folded rather than just the
// first character.
func foldStatsLetter(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func isLikelyReply(cmd string) bool {
	if len(cmd) == 3 {
		for i := 0; i < 3; i++ {
			if cmd[i] < '0' || cmd[i] > '9' {
				return false
			}
		}
		return true
	}
	return false
}

func formatID(prefix string, n uint64) string {
	const digits = "0123456789abcdefghijklmnopqrstuvwxyz"
	if n == 0 {
		return prefix + "0"
	}
	var b [32]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = digits[n%36]
		n /= 36
	}
	return prefix + string(b[i:])
}
