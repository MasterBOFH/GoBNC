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
	Target      string // folded nick for WHOIS nick-routing
	WHOXToken   string // token we sent upstream
	// WHOXStripToken: we injected 't'; remove querytype from 354 before the client.
	WHOXStripToken bool
	// WHOXClientToken: client's original querytype to restore on 354 (when they sent one).
	WHOXClientToken string
	EndCodes        map[string]bool
	Created         time.Time
}

// RequestTracker routes solicitous replies to the originating downlink.
type RequestTracker struct {
	mu       sync.Mutex
	labeled  map[string]*pendingRequest   // label -> req
	whox     map[string]*pendingRequest   // token -> req
	whois    map[string][]*pendingRequest // folded nick -> waiters (oldest first)
	queue    []*pendingRequest            // serialized (no label/token/whois)
	active   *pendingRequest              // head of serialized exchange
	nextLbl  uint64
	nextTok  uint64
}

// NewRequestTracker creates an empty tracker.
func NewRequestTracker() *RequestTracker {
	return &RequestTracker{
		labeled: make(map[string]*pendingRequest),
		whox:    make(map[string]*pendingRequest),
		whois:   make(map[string][]*pendingRequest),
	}
}

// endCodes for common solicitous commands.
func endCodesFor(cmd string) map[string]bool {
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
	default:
		return map[string]bool{}
	}
}

// IsSolicitous reports whether cmd needs reply routing.
func IsSolicitous(cmd string) bool {
	switch cmd {
	case "WHO", "WHOIS", "WHOWAS", "STATS", "LIST", "LINKS", "NAMES", "USERHOST", "ISON", "MONITOR", "WATCH":
		return true
	default:
		return false
	}
}

// Begin registers an outbound solicitous command.
// clientLabel is an optional label from the downlink to echo on replies.
// preferLabel / preferWHOX control uplink strategies.
// whoisTargets are folded nicks for unlabeled WHOIS nick-routing (ignored otherwise).
// For WHOX, the returned token is numeric 1–999 (ircu/IRCv3 requirement).
func (rt *RequestTracker) Begin(client ClientID, cmd, clientLabel string, preferLabel, preferWHOX bool, whoisTargets []string) (label, whoxToken string, wait bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	req := &pendingRequest{
		Client:      client,
		ClientLabel: clientLabel,
		Command:     cmd,
		EndCodes:    endCodesFor(cmd),
		Created:     time.Now(),
	}
	if preferLabel {
		rt.nextLbl++
		label = formatID("L", rt.nextLbl)
		req.Label = label
		rt.labeled[label] = req
		return label, "", false
	}
	if preferWHOX && (cmd == "WHO" || cmd == "WHOX") {
		rt.nextTok++
		if rt.nextTok > 999 {
			rt.nextTok = 1
		}
		whoxToken = strconv.FormatUint(rt.nextTok, 10)
		req.WHOXToken = whoxToken
		rt.whox[whoxToken] = req
		return "", whoxToken, false
	}
	// Unlabeled WHOIS: route by target nick (handles remote/interleaved replies).
	if cmd == "WHOIS" && len(whoisTargets) > 0 {
		for _, nick := range whoisTargets {
			if nick == "" {
				continue
			}
			w := &pendingRequest{
				Client:      client,
				ClientLabel: clientLabel,
				Command:     "WHOIS",
				Target:      nick,
				EndCodes:    endCodesFor("WHOIS"),
				Created:     time.Now(),
			}
			rt.whois[nick] = append(rt.whois[nick], w)
		}
		return "", "", false
	}
	// Serialize everything else
	rt.queue = append(rt.queue, req)
	if rt.active == nil {
		rt.active = req
		return "", "", false
	}
	return "", "", true // caller should wait / queue at higher level
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
				delete(rt.labeled, lbl)
			}
			return req.Client, true, req.ClientLabel, false, ""
		}
	}

	// WHOIS numerics: route by target nick (params[1]), oldest waiter first.
	if isWHOISNumeric(msg.Command) && len(msg.Params) > 1 {
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

	// WHOX: token is params[1] when 't' was requested (fixed field order).
	if msg.Command == "354" && len(msg.Params) > 1 {
		tok := msg.Params[1]
		if req, ok := rt.whox[tok]; ok {
			strip, restore := whoxFix(req)
			return req.Client, true, req.ClientLabel, strip, restore
		}
	}
	if msg.Command == "315" {
		// End-of-WHO without a token field — finish the oldest pending WHOX.
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
		if isLikelyReply(cmd) {
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

func (rt *RequestTracker) finishActive() {
	if len(rt.queue) > 0 && rt.queue[0] == rt.active {
		rt.queue = rt.queue[1:]
	}
	rt.active = nil
	if len(rt.queue) > 0 {
		rt.active = rt.queue[0]
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
	var nq []*pendingRequest
	for _, req := range rt.queue {
		if req.Client != client {
			nq = append(nq, req)
		}
	}
	rt.queue = nq
	if rt.active != nil && rt.active.Client == client {
		rt.active = nil
		if len(rt.queue) > 0 {
			rt.active = rt.queue[0]
		}
	}
}

// isWHOISNumeric reports numerics that carry the WHOIS target nick in params[1].
func isWHOISNumeric(cmd string) bool {
	switch cmd {
	case "301", // RPL_AWAY (also unsolicited; only routed when a WHOIS is pending)
		"276", "307", "311", "312", "313", "317", "318", "319", "320",
		"325", "330", "335", "336", "337", "338", "378", "379",
		"671", "672", "673", "674",
		"401": // ERR_NOSUCHNICK
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
