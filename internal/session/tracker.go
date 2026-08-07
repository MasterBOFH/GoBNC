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
	// gate is closed when this request may be written upstream (hold-until-end).
	// nil means already clear to send.
	gate chan struct{}
}

// stickyRoute delivers one follow-up numeric (e.g. 329 after MODE 324) to the
// same client after the target enquiry has already ended.
type stickyRoute struct {
	Client ClientID
	Echo   string
	Code   string
}

// RequestTracker routes solicitous replies to the originating downlink.
type RequestTracker struct {
	mu      sync.Mutex
	labeled map[string]*pendingRequest   // label -> req
	whox    map[string]*pendingRequest   // token -> req
	whois   map[string][]*pendingRequest // folded nick -> waiters (oldest first)
	stats   map[string][]*pendingRequest // stats letter -> waiters (oldest first)
	mode    map[string][]*pendingRequest // folded MODE target (chan/nick) -> waiters
	topic   map[string][]*pendingRequest // folded TOPIC channel -> waiters
	queue   []*pendingRequest            // serialized (no demux key)
	active  *pendingRequest              // head of serialized exchange
	sticky  map[string]*stickyRoute      // folded target -> one-shot follow-up (MODE 329)
	ircd    string                       // detected IRCd family (irc.IRCd*)
	nextLbl uint64
	nextTok uint64
}

// NewRequestTracker creates an empty tracker.
func NewRequestTracker() *RequestTracker {
	return &RequestTracker{
		labeled: make(map[string]*pendingRequest),
		whox:    make(map[string]*pendingRequest),
		whois:   make(map[string][]*pendingRequest),
		stats:   make(map[string][]*pendingRequest),
		mode:    make(map[string][]*pendingRequest),
		topic:   make(map[string][]*pendingRequest),
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
		return map[string]bool{"315": true}
	case "WHOIS":
		return map[string]bool{"318": true, "401": true}
	case "STATS":
		return map[string]bool{"219": true}
	case "LIST":
		return map[string]bool{"323": true}
	case "NAMES":
		return map[string]bool{"366": true}
	case "LINKS":
		return map[string]bool{"365": true}
	case "MAP":
		return irc.MAPEndCodes(ircd)
	case "HELP":
		// RatBox / modern: 704 start, 705 text, 706 end.
		return map[string]bool{"706": true, "524": true} // 524 = ERR_HELPNOTFOUND where used
	case "ADMIN":
		// RFC 2812: 256–259; 259 is last. 402 = no such server.
		return map[string]bool{"259": true, "402": true}
	case "MODE":
		// Channel modes: 324 (329 follows via sticky). Lists: *68/*49/*47/*29.
		// User modes: 221. Errors clear a stuck enquiry.
		return map[string]bool{
			"221": true, "324": true,
			"368": true, "349": true, "347": true, "729": true,
			"401": true, "403": true, "442": true, "461": true, "472": true, "482": true,
		}
	case "TOPIC":
		// 331 = no topic; 332 then 333 (333 ends). Errors clear the enquiry.
		return map[string]bool{"331": true, "333": true, "403": true, "442": true, "461": true}
	default:
		return map[string]bool{}
	}
}

// replyCodesFor are numerics attributed to a serialized solicitous exchange.
func replyCodesFor(cmd, ircd string) map[string]bool {
	switch cmd {
	case "WHO", "WHOX":
		return map[string]bool{"352": true, "315": true, "354": true}
	case "LIST":
		return map[string]bool{"321": true, "322": true, "323": true}
	case "NAMES":
		return map[string]bool{"353": true, "366": true}
	case "LINKS":
		return map[string]bool{"364": true, "365": true}
	case "STATS":
		return map[string]bool{
			"211": true, "212": true, "213": true, "214": true, "215": true,
			"216": true, "217": true, "218": true, "219": true,
			"241": true, "242": true, "243": true, "244": true, "245": true,
			"246": true, "247": true, "248": true, "249": true, "250": true,
		}
	case "MAP":
		return irc.MAPReplyCodes(ircd)
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
		return map[string]bool{"331": true, "332": true, "333": true, "403": true, "442": true, "461": true}
	default:
		return endCodesFor(cmd, ircd)
	}
}

// IsSolicitous reports whether cmd needs reply routing.
func IsSolicitous(cmd string) bool {
	switch cmd {
	case "WHO", "WHOIS", "WHOWAS", "STATS", "LIST", "LINKS", "NAMES",
		"USERHOST", "ISON", "MONITOR", "WATCH",
		"MAP", "HELP", "ADMIN":
		return true
	default:
		return false
	}
}

// BeginOpts configures registration of an outbound solicitous command.
type BeginOpts struct {
	Client        ClientID
	Cmd           string
	ClientLabel   string
	PreferLabel   bool
	PreferWHOX    bool
	WhoisTargets  []string // folded nicks
	WHOMask       string   // WHO mask (for 315 matching)
	StatsLetter   string   // folded STATS query letter ("" if none)
	EnquiryTarget string   // folded MODE/TOPIC target (channel or nick)
}

// Begin registers an outbound solicitous command.
// wait is non-nil when the caller must block until it is closed before writing upstream.
// For WHOX, the returned token is numeric 1–999 (ircu/IRCv3 requirement).
func (rt *RequestTracker) Begin(opts BeginOpts) (label, whoxToken string, wait <-chan struct{}) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	cmd := opts.Cmd
	req := &pendingRequest{
		Client:      opts.Client,
		ClientLabel: opts.ClientLabel,
		Command:     cmd,
		Target:      opts.EnquiryTarget,
		EndCodes:    endCodesFor(cmd, rt.ircd),
		ReplyCodes:  replyCodesFor(cmd, rt.ircd),
		Created:     time.Now(),
	}
	if opts.PreferLabel {
		rt.nextLbl++
		label = formatID("L", rt.nextLbl)
		req.Label = label
		rt.labeled[label] = req
		return label, "", nil
	}
	if opts.PreferWHOX && (cmd == "WHO" || cmd == "WHOX") {
		rt.nextTok++
		if rt.nextTok > 999 {
			rt.nextTok = 1
		}
		whoxToken = strconv.FormatUint(rt.nextTok, 10)
		req.WHOXToken = whoxToken
		req.Target = opts.WHOMask
		rt.whox[whoxToken] = req
		return "", whoxToken, nil
	}
	// Unlabeled WHOIS: route by target nick (handles remote/interleaved replies).
	if cmd == "WHOIS" && len(opts.WhoisTargets) > 0 {
		for _, nick := range opts.WhoisTargets {
			if nick == "" {
				continue
			}
			w := &pendingRequest{
				Client:      opts.Client,
				ClientLabel: opts.ClientLabel,
				Command:     "WHOIS",
				Target:      nick,
				EndCodes:    endCodesFor("WHOIS", rt.ircd),
				Created:     time.Now(),
			}
			rt.whois[nick] = append(rt.whois[nick], w)
		}
		return "", "", nil
	}
	// STATS: demux by query letter (y vs c may run concurrently).
	if cmd == "STATS" && opts.StatsLetter != "" {
		letter := opts.StatsLetter
		req.Target = letter
		waiters := rt.stats[letter]
		if len(waiters) == 0 {
			rt.stats[letter] = []*pendingRequest{req}
			return "", "", nil
		}
		// Same letter already in flight — hold until prior 219.
		req.gate = make(chan struct{})
		rt.stats[letter] = append(waiters, req)
		return "", "", req.gate
	}
	// MODE/TOPIC enquiries: demux by channel/nick (concurrent across targets).
	if opts.EnquiryTarget != "" && (cmd == "MODE" || cmd == "TOPIC") {
		queues := rt.mode
		if cmd == "TOPIC" {
			queues = rt.topic
		}
		waiters := queues[opts.EnquiryTarget]
		if len(waiters) == 0 {
			queues[opts.EnquiryTarget] = []*pendingRequest{req}
			return "", "", nil
		}
		req.gate = make(chan struct{})
		queues[opts.EnquiryTarget] = append(waiters, req)
		return "", "", req.gate
	}
	// Serialize everything else (plain WHO, LIST, NAMES, letter-less STATS, …).
	rt.queue = append(rt.queue, req)
	if rt.active == nil {
		rt.active = req
		return "", "", nil
	}
	req.gate = make(chan struct{})
	return "", "", req.gate
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
	}
}

// ActiveClient returns the client owning the serialized active request, if any.
func (rt *RequestTracker) ActiveClient() (ClientID, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.active == nil {
		return "", false
	}
	return rt.active.Client, true
}

// RouteMessage decides which client should receive msg (empty = broadcast).
// cm folds WHOIS target nicks. echoLabel is for labeled-response clients.
// For 354: stripWHOXToken removes an injected querytype; restoreWHOXToken replaces it with the client's.
func (rt *RequestTracker) RouteMessage(msg irc.Message, cm irc.CaseMapping) (client ClientID, only bool, echoLabel string, stripWHOXToken bool, restoreWHOXToken string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	whoxFix := func(req *pendingRequest) (bool, string) {
		if msg.Command != "354" {
			return false, ""
		}
		if req.WHOXStripToken {
			return true, ""
		}
		return false, req.WHOXClientToken
	}

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
						rt.sticky[target] = &stickyRoute{Client: req.Client, Echo: req.ClientLabel, Code: "329"}
					}
				}
				delete(rt.labeled, lbl)
			}
			return req.Client, true, req.ClientLabel, false, ""
		}
	}

	// One-shot follow-up (RPL_CREATIONTIME after channel MODE enquiry).
	if msg.Command == "329" && len(msg.Params) > 1 {
		target := cm.Canonical(msg.Params[1])
		if s := rt.sticky[target]; s != nil && s.Code == "329" {
			delete(rt.sticky, target)
			return s.Client, true, s.Echo, false, ""
		}
	}

	// MODE enquiry numerics: demux by channel/nick in params.
	if target := modeEnquiryNumericTarget(msg, cm); target != "" {
		if c, only, echo, ok := rt.routeTargetQueue(rt.mode, target, msg.Command, true); ok {
			return c, only, echo, false, ""
		}
	}

	// TOPIC enquiry numerics: demux by channel.
	if target := topicEnquiryNumericTarget(msg, cm); target != "" {
		if c, only, echo, ok := rt.routeTargetQueue(rt.topic, target, msg.Command, false); ok {
			return c, only, echo, false, ""
		}
	}

	// WHOIS numerics: route by target nick (params[1]), oldest waiter first.
	if irc.IsWHOISReply(msg.Command, rt.ircd) && len(msg.Params) > 1 {
		nick := cm.Canonical(msg.Params[1])
		if waiters := rt.whois[nick]; len(waiters) > 0 {
			req := waiters[0]
			if req.EndCodes[msg.Command] {
				rt.whois[nick] = waiters[1:]
				if len(rt.whois[nick]) == 0 {
					delete(rt.whois, nick)
				}
			}
			return req.Client, true, req.ClientLabel, false, ""
		}
	}

	// STATS: 219 carries the query letter in params[1].
	if msg.Command == "219" && len(msg.Params) > 1 {
		letter := foldStatsLetter(msg.Params[1])
		if waiters := rt.stats[letter]; len(waiters) > 0 {
			req := waiters[0]
			rt.stats[letter] = waiters[1:]
			if len(rt.stats[letter]) == 0 {
				delete(rt.stats, letter)
			} else {
				rt.releaseGate(rt.stats[letter][0])
			}
			return req.Client, true, req.ClientLabel, false, ""
		}
	}
	// Intermediate STATS numerics: only when a single letter is pending.
	if isSTATSNumeric(msg.Command) && msg.Command != "219" {
		if letter, req, ok := rt.singleStatsPending(); ok {
			_ = letter
			return req.Client, true, req.ClientLabel, false, ""
		}
	}

	// WHOX: token is params[1] when 't' was requested (fixed field order).
	if msg.Command == "354" && len(msg.Params) > 1 {
		tok := msg.Params[1]
		if req, ok := rt.whox[tok]; ok {
			strip, restore := whoxFix(req)
			return req.Client, true, req.ClientLabel, strip, restore
		}
	}
	if msg.Command == "315" {
		mask := ""
		if len(msg.Params) > 1 {
			mask = cm.Canonical(msg.Params[1])
		}
		if req, tok, ok := rt.whoxByMask(mask); ok {
			delete(rt.whox, tok)
			return req.Client, true, req.ClientLabel, false, ""
		}
		// Fallback: oldest pending WHOX.
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
			return best.Client, true, best.ClientLabel, false, ""
		}
	}

	// Fallback: single pending WHOX request (token mismatch / server defaulted querytype).
	if (msg.Command == "354" || msg.Command == "315") && len(rt.whox) == 1 {
		for tok, req := range rt.whox {
			strip, restore := whoxFix(req)
			if msg.Command == "315" {
				delete(rt.whox, tok)
			}
			return req.Client, true, req.ClientLabel, strip, restore
		}
	}

	if rt.active != nil {
		cmd := msg.Command
		if rt.active.ReplyCodes[cmd] || (len(rt.active.ReplyCodes) == 0 && isLikelyReply(cmd)) {
			client := rt.active.Client
			echo := rt.active.ClientLabel
			if rt.active.EndCodes[cmd] {
				rt.finishActive()
			}
			return client, true, echo, false, ""
		}
	}
	return "", false, "", false, ""
}

// routeTargetQueue delivers a numeric to the oldest waiter for target.
// sticky324 enables per-target 329 sticky when ending a MODE enquiry on 324.
func (rt *RequestTracker) routeTargetQueue(queues map[string][]*pendingRequest, target, cmd string, sticky324 bool) (ClientID, bool, string, bool) {
	waiters := queues[target]
	if len(waiters) == 0 {
		return "", false, "", false
	}
	req := waiters[0]
	if len(req.ReplyCodes) > 0 && !req.ReplyCodes[cmd] {
		return "", false, "", false
	}
	if req.EndCodes[cmd] {
		if sticky324 && req.Command == "MODE" && cmd == "324" && req.ReplyCodes["329"] {
			rt.sticky[target] = &stickyRoute{Client: req.Client, Echo: req.ClientLabel, Code: "329"}
		}
		queues[target] = waiters[1:]
		if len(queues[target]) == 0 {
			delete(queues, target)
		} else {
			rt.releaseGate(queues[target][0])
		}
	}
	return req.Client, true, req.ClientLabel, true
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
	case "331", "332", "333", "403", "442", "461":
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

func (rt *RequestTracker) releaseGate(req *pendingRequest) {
	if req == nil || req.gate == nil {
		return
	}
	close(req.gate)
	req.gate = nil
}

func (rt *RequestTracker) finishActive() {
	if len(rt.queue) > 0 && rt.queue[0] == rt.active {
		rt.queue = rt.queue[1:]
	}
	rt.active = nil
	if len(rt.queue) > 0 {
		rt.active = rt.queue[0]
		rt.releaseGate(rt.active)
	}
}

// DropClient removes pending requests for a disconnected client.
func (rt *RequestTracker) DropClient(client ClientID) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for k, req := range rt.labeled {
		if req.Client == client {
			delete(rt.labeled, k)
		}
	}
	for k, req := range rt.whox {
		if req.Client == client {
			delete(rt.whox, k)
		}
	}
	for nick, waiters := range rt.whois {
		var kept []*pendingRequest
		for _, req := range waiters {
			if req.Client != client {
				kept = append(kept, req)
			}
		}
		if len(kept) == 0 {
			delete(rt.whois, nick)
		} else {
			rt.whois[nick] = kept
		}
	}
	for letter, waiters := range rt.stats {
		var kept []*pendingRequest
		droppedHead := len(waiters) > 0 && waiters[0].Client == client
		for _, req := range waiters {
			if req.Client == client {
				rt.releaseGate(req)
				continue
			}
			kept = append(kept, req)
		}
		if len(kept) == 0 {
			delete(rt.stats, letter)
		} else {
			rt.stats[letter] = kept
			if droppedHead {
				rt.releaseGate(kept[0])
			}
		}
	}
	for _, queues := range []map[string][]*pendingRequest{rt.mode, rt.topic} {
		for target, waiters := range queues {
			var kept []*pendingRequest
			droppedHead := len(waiters) > 0 && waiters[0].Client == client
			for _, req := range waiters {
				if req.Client == client {
					rt.releaseGate(req)
					continue
				}
				kept = append(kept, req)
			}
			if len(kept) == 0 {
				delete(queues, target)
			} else {
				queues[target] = kept
				if droppedHead {
					rt.releaseGate(kept[0])
				}
			}
		}
	}
	for target, s := range rt.sticky {
		if s.Client == client {
			delete(rt.sticky, target)
		}
	}
	var nq []*pendingRequest
	for _, req := range rt.queue {
		if req.Client == client {
			rt.releaseGate(req)
			continue
		}
		nq = append(nq, req)
	}
	rt.queue = nq
	if rt.active != nil && rt.active.Client == client {
		rt.active = nil
		if len(rt.queue) > 0 {
			rt.active = rt.queue[0]
			rt.releaseGate(rt.active)
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

// ParseStatsLetter returns the folded STATS query letter, or "" if absent.
func ParseStatsLetter(params []string) string {
	if len(params) == 0 || params[0] == "" {
		return ""
	}
	return foldStatsLetter(params[0])
}

func foldStatsLetter(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Only the first character is the query type on most ircds.
	return strings.ToLower(s[:1])
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
