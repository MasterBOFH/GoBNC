//go:build ircd

// Live integration test: a real brain attached to docker/ircd's ergo,
// with the configured nick occupied by a squatter so registration settles
// on a different assigned nick. A real TLS downlink then connects to the
// bouncer with yet another NICK (not the assigned one, not the configured
// one). The attach burst must address 001–005 and the own-MODE at the
// assigned nick, with MODE sourced from the real nick!user@host — and the
// same must hold after a brain reload.
//
//	Run: (cd docker/ircd && docker compose up -d ergo) && \
//	  go test -tags ircd ./internal/server/... -run LiveDownlinkWelcome -v
package server

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/auth"
	"github.com/MasterBOFH/GoBNC/internal/downlink"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
)

const (
	ergoServerName = "ergo.test"
	bouncerPass    = "s3cret"
)

// TestLiveDownlinkWelcomeNumericsAfterConnectAndReload is the real-ircd
// counterpart of TestResumeReplaysWelcomeNumerics +
// TestResumeAttachUsesAssignedNick: those drive a fake ircd that 001s
// whatever we tell it to; this occupies the configured nick on a genuine
// ergo so the assigned nick is a registration-time fact, not a fixture.
// The downlink is a real TLS client whose NICK is deliberately not the
// assigned uplink nick — 001 and the own-MODE must still target (and, for
// MODE, be sourced from) the assigned nick, not the one the client sent
// the bouncer.
func TestLiveDownlinkWelcomeNumericsAfterConnectAndReload(t *testing.T) {
	if os.Getenv("GOBNC_IRCD") == "0" {
		t.Skip("GOBNC_IRCD=0")
	}
	addr := fmt.Sprintf("%s:%d", ergoLiveAddr, ergoLivePort)
	if c, err := net.DialTimeout("tcp", addr, 3*time.Second); err != nil {
		t.Skipf("ergo not reachable at %s: %v (docker compose -f docker/ircd/docker-compose.yml up -d ergo)", addr, err)
	} else {
		_ = c.Close()
	}

	configured := fmt.Sprintf("gbw%d", time.Now().UnixNano()%1000000)
	alt := configured + "_"
	requested := "dlnick" // what the downlink sends; must not equal assigned or configured
	occupyNickOnErgo(t, addr, configured)

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")
	rk := startResumeTestKeeper(t)
	fx := testutil.NewTLSFixture(t)

	s1 := newResumeTestServer(t, dbPath)
	hash, err := auth.HashPassword(bouncerPass)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Store().SetPasswordHash(s1.runCtx, hash); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Store().UpsertNetwork(s1.runCtx, store.Network{
		Name: "net1", Host: ergoLiveAddr, Port: ergoLivePort,
		Nick: configured, AltNick: alt, Username: "gobnc",
		NickRecovery: true, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	n, err := s1.Store().NetworkByName(s1.runCtx, "net1")
	if err != nil {
		t.Fatal(err)
	}
	rk.attach(t, s1, []store.Network{n})

	sess1, err := s1.Session("net1")
	if err != nil {
		t.Fatal(err)
	}
	waitForRegisteredLive(t, "ergo (fresh, nick collision)", sess1.Registered)

	assigned := sess1.Nick()
	if assigned == "" || assigned == configured {
		t.Fatalf("sess1 nick=%q want a nick other than configured %q (collision during registration)", assigned, configured)
	}
	if assigned == requested {
		t.Fatalf("assigned nick %q collided with the downlink's requested nick", assigned)
	}
	t.Logf("registered as %q (configured %q was occupied; downlink will NICK %q)", assigned, configured, requested)

	waitForUModeI(t, "first brain", sess1)
	ensureSelfUserHost(t, sess1)

	listen1 := startTestDownlink(t, s1, fx)
	burst1 := dialBouncerBurst(t, listen1, fx.ClientTLS, "net1", requested)
	assertWelcomeBurst(t, "first brain", burst1, assigned, requested, configured, ergoServerName)
	assertOwnMODE(t, "first brain", burst1, assigned, requested, configured)

	_ = s1.keeperClient.Close()

	s2 := newResumeTestServer(t, dbPath)
	rk.attach(t, s2, []store.Network{n})

	sess2, err := s2.Session("net1")
	if err != nil {
		t.Fatal(err)
	}
	waitForRegisteredLive(t, "ergo (resumed)", sess2.Registered)
	if got := sess2.Nick(); got != assigned {
		t.Fatalf("resumed session nick=%q want assigned %q (not configured %q)", got, assigned, configured)
	}

	waitForUModeI(t, "resumed brain", sess2)
	ensureSelfUserHost(t, sess2)

	listen2 := startTestDownlink(t, s2, fx)
	burst2 := dialBouncerBurst(t, listen2, fx.ClientTLS, "net1", requested)
	assertWelcomeBurst(t, "resumed brain", burst2, assigned, requested, configured, ergoServerName)
	assertOwnMODE(t, "resumed brain", burst2, assigned, requested, configured)
}

func startTestDownlink(t *testing.T, s *Server, fx *testutil.TLSFixture) string {
	t.Helper()
	s.cfg.AllowPasswordAuth = true
	s.cfg.AllowCertAuth = false
	ln, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
	if err != nil {
		t.Fatalf("downlink listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	dl := downlink.NewListener(s.cfg, s.store, s, fx.ServerTLS, s.log)
	s.dl = dl
	go func() { _ = dl.Serve(s.runCtx, ln) }()
	return ln.Addr().String()
}

// dialBouncerBurst connects a real TLS IRC client to the bouncer, sending
// NICK requested (deliberately not the uplink's assigned nick), and returns
// the attach burst through own-MODE (which follows 376).
func dialBouncerBurst(t *testing.T, addr string, clientTLS *tls.Config, network, requested string) []irc.Message {
	t.Helper()
	c, err := tls.Dial("tcp", addr, clientTLS)
	if err != nil {
		t.Fatalf("dial bouncer: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	write := func(s string) {
		_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := c.Write([]byte(s + "\r\n")); err != nil {
			t.Fatalf("write %q: %v", s, err)
		}
	}
	write("PASS " + network + "/" + bouncerPass)
	write("NICK " + requested)
	write("USER client 0 * :client")

	r := bufio.NewReader(c)
	var msgs []irc.Message
	saw376, sawMODE := false, false
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		raw, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read burst (376=%v MODE=%v): %v\n%+v", saw376, sawMODE, err, msgs)
		}
		line := strings.TrimRight(raw, "\r\n")
		if line == "" {
			continue
		}
		msg, err := irc.Parse(line)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		if msg.Command == "PING" {
			write("PONG :" + msg.Trailing())
			continue
		}
		msgs = append(msgs, msg)
		if msg.Command == "376" {
			saw376 = true
		}
		if msg.Command == "MODE" {
			sawMODE = true
		}
		if saw376 && sawMODE {
			return msgs
		}
	}
	t.Fatalf("timeout waiting for 376+MODE (376=%v MODE=%v): %+v", saw376, sawMODE, msgs)
	return msgs
}

func waitForUModeI(t *testing.T, label string, sess *session.Session) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if self := sess.Self(); self != nil && strings.Contains(self.UModeString(), "i") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	got := ""
	if self := sess.Self(); self != nil {
		got = self.UModeString()
	}
	t.Fatalf("%s: never learned umode +i (got %q)", label, got)
}

// ensureSelfUserHost makes sure Prefix() is a real nick!user@host, not the
// ServerName placeholder. A live registration with no JOIN never sees a
// self-echo prefix; USERHOST is the same query resume uses for that gap.
func ensureSelfUserHost(t *testing.T, sess *session.Session) {
	t.Helper()
	if hasRealUserHost(sess.SelfPrefix()) {
		return
	}
	nick := sess.Nick()
	if err := sess.WriteMessage(irc.Message{Command: "USERHOST", Params: []string{nick}}); err != nil {
		t.Fatalf("USERHOST: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if hasRealUserHost(sess.SelfPrefix()) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("never learned nick!user@host (prefix=%q)", sess.SelfPrefix())
}

func hasRealUserHost(prefix string) bool {
	bang := strings.IndexByte(prefix, '!')
	at := strings.LastIndexByte(prefix, '@')
	if bang < 1 || at <= bang+1 || at == len(prefix)-1 {
		return false
	}
	host := prefix[at+1:]
	return host != session.ServerName
}

func occupyNickOnErgo(t *testing.T, addr, nick string) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 8*time.Second)
	if err != nil {
		t.Fatalf("squatter dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	write := func(s string) {
		_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, _ = c.Write([]byte(s + "\r\n"))
	}
	write("NICK " + nick)
	write("USER squatter 0 * :squatter")

	r := bufio.NewReader(c)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		raw, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("squatter register: %v", err)
		}
		msg, err := irc.Parse(strings.TrimRight(raw, "\r\n"))
		if err != nil {
			continue
		}
		switch msg.Command {
		case "PING":
			write("PONG :" + msg.Trailing())
		case "001":
			if msg.Param(0) != nick {
				t.Fatalf("squatter 001 target=%q want %q", msg.Param(0), nick)
			}
			stop := make(chan struct{})
			t.Cleanup(func() { close(stop) })
			go squatterKeepAlive(c, r, write, stop)
			return
		case "433", "432":
			t.Fatalf("squatter could not occupy %q: %s", nick, msg.Command)
		case "ERROR":
			t.Fatalf("squatter ERROR: %s", msg.Trailing())
		}
	}
	t.Fatal("squatter never got 001")
}

func squatterKeepAlive(c net.Conn, r *bufio.Reader, write func(string), stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		raw, err := r.ReadString('\n')
		if err != nil {
			continue
		}
		msg, err := irc.Parse(strings.TrimRight(raw, "\r\n"))
		if err != nil {
			continue
		}
		if msg.Command == "PING" {
			write("PONG :" + msg.Trailing())
		}
	}
}

func waitForRegisteredLive(t *testing.T, label string, registered func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if registered() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s: never registered", label)
}

// assertWelcomeBurst checks a post-registration Attach burst: 001–005 are
// all present, each sourced from the uplink server name (not the gobnc
// fallback), each targeted at the assigned nick (not the configured one
// and not the NICK the downlink sent the bouncer), and 005 carries a token
// that can only have come from the real ircd (NETWORK=ErgoTest).
func assertWelcomeBurst(t *testing.T, label string, msgs []irc.Message, assigned, requested, configured, wantServer string) {
	t.Helper()
	seen := map[string]irc.Message{}
	for _, m := range msgs {
		if m.Command == "NICK" {
			t.Fatalf("%s: attach must not send NICK (001 is the nick source of truth): %+v", label, msgs)
		}
		switch m.Command {
		case "001", "002", "003", "004", "005":
			if m.Source != wantServer {
				t.Fatalf("%s: %s source=%q want %q: %+v", label, m.Command, m.Source, wantServer, m)
			}
			if m.Param(0) != assigned {
				t.Fatalf("%s: %s target=%q want assigned %q (not requested %q or configured %q): %+v",
					label, m.Command, m.Param(0), assigned, requested, configured, m)
			}
			if _, ok := seen[m.Command]; !ok {
				seen[m.Command] = m
			}
		}
	}
	for _, cmd := range []string{"001", "002", "003", "004", "005"} {
		if _, ok := seen[cmd]; !ok {
			t.Fatalf("%s: attach missing %s: %+v", label, cmd, msgs)
		}
	}
	if p := seen["002"].Param(1); !strings.Contains(p, wantServer) {
		t.Fatalf("%s: 002 %q does not mention uplink server %q", label, p, wantServer)
	}
	if p := seen["003"].Param(1); p == "" {
		t.Fatalf("%s: 003 trailing empty: %+v", label, seen["003"])
	}
	if p := seen["004"].Param(1); p != wantServer {
		t.Fatalf("%s: 004 server=%q want %q: %+v", label, p, wantServer, seen["004"])
	}
	var hasNetwork bool
	for _, m := range msgs {
		if m.Command != "005" {
			continue
		}
		for _, p := range m.Params {
			if p == "NETWORK=ErgoTest" || strings.HasPrefix(p, "NETWORK=ErgoTest") {
				hasNetwork = true
			}
		}
	}
	if !hasNetwork {
		t.Fatalf("%s: 005 missing NETWORK=ErgoTest (empty/synthetic ISUPPORT?): %+v", label, msgs)
	}
}

// assertOwnMODE checks the attach-burst own-MODE: sourced from the
// assigned nick!user@host (not a bare nick, not the downlink's requested
// nick), targeted at the assigned nick, with +i set (ergo sets invisible
// on registration).
func assertOwnMODE(t *testing.T, label string, msgs []irc.Message, assigned, requested, configured string) {
	t.Helper()
	var mode irc.Message
	for _, m := range msgs {
		if m.Command != "MODE" {
			continue
		}
		target := m.Param(0)
		if target == "" || target[0] == '#' || target[0] == '&' {
			continue
		}
		mode = m
		break
	}
	if mode.Command == "" {
		t.Fatalf("%s: attach missing own MODE: %+v", label, msgs)
	}
	if mode.Param(0) != assigned {
		t.Fatalf("%s: MODE target=%q want assigned %q (not requested %q or configured %q): %+v",
			label, mode.Param(0), assigned, requested, configured, mode)
	}
	if !strings.Contains(mode.Param(1), "i") {
		t.Fatalf("%s: MODE %q missing +i: %+v", label, mode.Param(1), mode)
	}
	src := mode.Source
	bang := strings.IndexByte(src, '!')
	at := strings.LastIndexByte(src, '@')
	if bang < 1 || at <= bang+1 || at == len(src)-1 {
		t.Fatalf("%s: MODE source=%q want assigned!user@host, not a bare nick: %+v", label, src, mode)
	}
	srcNick := src[:bang]
	srcUser := src[bang+1 : at]
	srcHost := src[at+1:]
	if srcNick != assigned {
		t.Fatalf("%s: MODE prefix nick=%q want assigned %q (not requested %q or configured %q): %+v",
			label, srcNick, assigned, requested, configured, mode)
	}
	if srcNick == requested || srcNick == configured {
		t.Fatalf("%s: MODE prefix nick %q must not be the downlink or configured nick: %+v", label, srcNick, mode)
	}
	if srcUser == "" || srcHost == "" || srcHost == session.ServerName {
		t.Fatalf("%s: MODE source=%q want a real user@host, not the gobnc placeholder: %+v", label, src, mode)
	}
}
