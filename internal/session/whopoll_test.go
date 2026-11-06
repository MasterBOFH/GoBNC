package session

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// A2992: member metadata for gobnc-retecloud's lite member lists. GoBNC learns hosts and flags
// from userhost-in-names and from WHO/WHOX replies that pass through for its IRC clients, and
// polls WHOX itself (silently) only for a channel nobody polled within the interval.

// writeRec captures what the Session writes to the uplink.
type writeRec struct {
	mu   sync.Mutex
	msgs []irc.Message
}

func (w *writeRec) write(m irc.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.msgs = append(w.msgs, m)
	return nil
}

func (w *writeRec) lines() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.msgs))
	for _, m := range w.msgs { // command and params, without the encoder's trailing ':'
		out = append(out, strings.TrimSpace(m.Command+" "+strings.Join(m.Params, " ")))
	}
	return out
}

func (w *writeRec) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.msgs = nil
}

// newMetaSession is a registered "me" on a WHOX network, in #c and #d with alice and bob, one
// IRC client attached and one module subscribed; writes to the uplink are recorded.
func newMetaSession(t *testing.T) (*Session, *fakeDL, *recSink, *writeRec) {
	t.Helper()
	s := newBusSession(t) // registered, in #c
	w := &writeRec{}
	s.writeHook = w.write
	feed(s, ":srv 005 me WHOX SILENCE=15 PREFIX=(ov)@+ :are supported by this server",
		":srv 353 me = #c :@me alice bob", ":srv 366 me #c :End of /NAMES list.",
		":me!u@h JOIN #d", ":srv 353 me = #d :me alice", ":srv 366 me #d :End of /NAMES list.")
	d := &fakeDL{id: "d1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	d.clearSent()
	sink := &recSink{}
	s.Subscribe(1, sink, nil, nil)
	return s, d, sink, w
}

func (s *Session) userForTest(nick string) User {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if u := s.users[s.isupport.CaseMapping.Canonical(nick)]; u != nil {
		return *u
	}
	return User{}
}

func memberOf(snap wire.Snapshot, channel, nick string) (wire.Member, bool) {
	for _, ch := range snap.Channels {
		if ch.Name != channel {
			continue
		}
		for _, m := range ch.Members {
			if m.Nick == nick {
				return m, true
			}
		}
	}
	return wire.Member{}, false
}

func TestNAMESWithUserhostInNamesLearnsHosts(t *testing.T) {
	s := newBusSession(t)
	feed(s, ":srv 005 me PREFIX=(ov)@+ :are supported by this server",
		":srv 353 me = #c :@me!u@h @+alice!al@a.example bob!b@b.example",
		":srv 366 me #c :End of /NAMES list.")
	snap := s.SubscribeWithSnapshot(9, &recSink{}, nil, nil)
	alice, ok := memberOf(snap, "#c", "alice")
	if !ok || alice.User != "al" || alice.Host != "a.example" || alice.Prefixes != "@+" {
		t.Fatalf("alice = %+v (found %v)", alice, ok)
	}
	if bob, _ := memberOf(snap, "#c", "bob"); bob.User != "b" || bob.Host != "b.example" {
		t.Fatalf("bob = %+v", bob)
	}
	for _, ch := range snap.Channels {
		for _, m := range ch.Members {
			if strings.ContainsAny(m.Nick, "!@") {
				t.Fatalf("member nick kept the userhost: %q", m.Nick)
			}
		}
	}
}

// Clients are never offered userhost-in-names, so a NAMES reply reaches them in the plain form.
func TestNAMESUserhostStrippedForClientsWithoutTheCap(t *testing.T) {
	s := newBusSession(t)
	d := &fakeDL{id: "d1", caps: map[string]bool{}}
	msg, err := irc.Parse(":srv 353 me = #c :@me!u@h @+alice!al@a.example bob!b@b.example")
	if err != nil {
		t.Fatal(err)
	}
	out := s.rewriteFor(d, msg)
	if got := out.Wire(); got != ":srv 353 me = #c :@me @+alice bob" {
		t.Fatalf("plain client got %q", got)
	}
	d.EnableCap("userhost-in-names")
	if got := s.rewriteFor(d, msg).Trailing(); got != "@me!u@h @+alice!al@a.example bob!b@b.example" {
		t.Fatalf("client with the cap got %q", got)
	}
}

func TestUserhostInNamesIsRequested(t *testing.T) {
	found := false
	for _, c := range DesiredCaps {
		found = found || c == "userhost-in-names"
	}
	if !found {
		t.Fatal("userhost-in-names is not requested upstream")
	}
}

// A client's own WHOX passes through: it gets its replies as before, and GoBNC learns from them.
func TestClientWHOXReplyTeachesMetadata(t *testing.T) {
	s, d, _, _ := newMetaSession(t)
	_, tok, _ := s.tracker.Begin(BeginOpts{Client: d.ID(), Cmd: "WHO", PreferWHOX: true, WHOMask: "#c", WHOXFields: "cuhnfa"})
	s.tracker.SetWHOXClientFix(tok, true, "")
	feed(s, ":srv 354 me "+tok+" #c al a.example alice G*B alice_acct",
		":srv 354 me "+tok+" #c b b.example bob H 0",
		":srv 315 me #c :End of /WHO list.")
	alice := s.userForTest("alice")
	if alice.User != "al" || alice.Host != "a.example" || !alice.Away || !alice.Oper || !alice.Bot || alice.Account != "alice_acct" {
		t.Fatalf("alice = %+v", alice)
	}
	if bob := s.userForTest("bob"); bob.Away || bob.Oper || bob.Account != "" || bob.Host != "b.example" {
		t.Fatalf("bob = %+v", bob)
	}
	if n := len(d.snapshot()); n != 3 {
		t.Fatalf("the requesting client got %d lines, want its 2 replies and the end", n)
	}
	snap := s.SubscribeWithSnapshot(9, &recSink{}, nil, nil)
	if m, _ := memberOf(snap, "#c", "alice"); !m.Oper || !m.Bot || !m.Away || m.Account != "alice_acct" {
		t.Fatalf("snapshot member = %+v", m)
	}
}

// WHO flags are the server's whole answer: a later reply without '*' clears oper.
func TestWHOFlagsAreAuthoritative(t *testing.T) {
	u := &User{Oper: true, Bot: true, Away: true}
	u.ApplyWHOFlags("H")
	if u.Oper || u.Bot || u.Away {
		t.Fatalf("after H: %+v", u)
	}
}

func TestOwnWHOXPollIsSilentAndTeaches(t *testing.T) {
	s, d, sink, w := newMetaSession(t)
	now := time.Now()
	polled := s.PollWHO(now, WHOPoll{Interval: 45 * time.Minute})
	if polled != "#c" {
		t.Fatalf("polled %q, want #c (first by key)", polled)
	}
	got := w.lines()
	if len(got) != 1 || !strings.HasPrefix(got[0], "WHO #c %tcuhnfa,") {
		t.Fatalf("uplink writes = %q", got)
	}
	tok := strings.TrimPrefix(got[0], "WHO #c %tcuhnfa,")
	before := len(sink.lines())
	feed(s, ":srv 354 me "+tok+" #c al a.example alice H@ acct",
		":srv 315 me #c :End of /WHO list.")
	if sent := d.snapshot(); len(sent) != 0 {
		t.Fatalf("an IRC client got the poll's replies: %+v", sent)
	}
	if n := len(sink.lines()); n != before {
		t.Fatalf("modules got %d line events from the poll", n-before)
	}
	if a := s.userForTest("alice"); a.Account != "acct" || a.Host != "a.example" {
		t.Fatalf("alice = %+v", a)
	}
	// One channel per tick; the next tick takes the other channel, then nothing is due.
	if p := s.PollWHO(now.Add(time.Second), WHOPoll{Interval: 45 * time.Minute}); p != "#d" {
		t.Fatalf("second tick polled %q, want #d", p)
	}
	if p := s.PollWHO(now.Add(2*time.Second), WHOPoll{Interval: 45 * time.Minute}); p != "" {
		t.Fatalf("third tick polled %q, want nothing due", p)
	}
	if p := s.PollWHO(now.Add(46*time.Minute), WHOPoll{Interval: 45 * time.Minute}); p != "#c" {
		t.Fatalf("after the interval polled %q, want #c again", p)
	}
}

// A client is polling #c, so GoBNC does not: it polls only the channel nobody polled.
func TestClientPollingSuppressesOwnPoll(t *testing.T) {
	s, d, _, w := newMetaSession(t)
	_, tok, _ := s.tracker.Begin(BeginOpts{Client: d.ID(), Cmd: "WHO", PreferWHOX: true, WHOMask: "#c", WHOXFields: "cuhnf"})
	s.tracker.SetWHOXClientFix(tok, true, "")
	feed(s, ":srv 354 me "+tok+" #c al a.example alice H", ":srv 315 me #c :End of /WHO list.")
	// An untracked 352 (no request GoBNC knows asked for it) updates the user but no longer counts
	// as polling; #d is not polled again here because GoBNC already polled it itself.
	if p := s.PollWHO(time.Now(), WHOPoll{Interval: 45 * time.Minute}); p != "#d" {
		t.Fatalf("polled %q, want #d (#c was just polled by a client)", p)
	}
	w.reset()
	feed(s, ":srv 352 me #d al a.example srv alice H :0 Alice")
	if p := s.PollWHO(time.Now().Add(time.Minute), WHOPoll{Interval: 45 * time.Minute}); p != "" {
		t.Fatalf("polled %q, want nothing", p)
	}
	if got := w.lines(); len(got) != 0 {
		t.Fatalf("uplink writes = %q", got)
	}
}

func TestNoOwnPollWithoutWHOXModulesOrInterval(t *testing.T) {
	s, _, _, w := newMetaSession(t)
	if p := s.PollWHO(time.Now(), WHOPoll{}); p != "" {
		t.Fatalf("interval 0 polled %q", p)
	}
	s.Unsubscribe(1)
	if p := s.PollWHO(time.Now(), WHOPoll{Interval: time.Minute}); p != "" {
		t.Fatalf("no module subscribed, polled %q", p)
	}
	s.Subscribe(1, &recSink{}, nil, nil)
	s.mu.Lock()
	s.isupport.WHOX = false
	s.mu.Unlock()
	if p := s.PollWHO(time.Now(), WHOPoll{Interval: time.Minute}); p != "" {
		t.Fatalf("no WHOX, polled %q", p)
	}
	if got := w.lines(); len(got) != 0 {
		t.Fatalf("uplink writes = %q", got)
	}
}
