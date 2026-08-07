package uplink

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
	"github.com/xdg-go/scram"
)

type regHandler struct {
	mu  sync.Mutex
	reg bool
}

func (h *regHandler) OnRegistered(u *Uplink) {
	h.mu.Lock()
	h.reg = true
	h.mu.Unlock()
}
func (h *regHandler) OnMessage(u *Uplink, msg irc.Message)                 {}
func (h *regHandler) OnDisconnect(u *Uplink, err error)                    {}
func (h *regHandler) OnRegistrationLine(u *Uplink, msg irc.Message)        {}
func (h *regHandler) OnCapsChanged(u *Uplink, added, removed []string)     {}
func (h *regHandler) OnSASLOffer(u *Uplink, available bool)                {}
func (h *regHandler) OnCapNAK(u *Uplink, names []string)                   {}

func TestUplinkHappyPath(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	h := &regHandler{}
	u := New(Config{
		Network: store.Network{
			Name: "test", Host: "pipe", Port: 1, Nick: "testnick",
			Username: "u", Realname: "r", SASLUser: "u", SASLPass: "p",
		},
		Channels:   []store.Channel{{Name: "#chan"}},
		MinBackoff: time.Hour,
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return client, nil
		},
	}, h)

	scriptDone := make(chan error, 1)
	go func() {
		scriptDone <- testutil.RunScript(ctx, server, []testutil.ScriptStep{
			{ExpectContains: "CAP LS"},
			{ExpectContains: "NICK"},
			{ExpectContains: "USER"},
			{Send: "CAP * LS :sasl=PLAIN,EXTERNAL message-tags server-time batch labeled-response cap-notify"},
			{ExpectContains: "CAP REQ"},
			{Send: "CAP * ACK :sasl message-tags server-time batch labeled-response cap-notify"},
			{Expect: "AUTHENTICATE PLAIN"},
			{Send: "AUTHENTICATE +"},
			{ExpectContains: "AUTHENTICATE "},
			{Send: ":server 903 testnick :SASL authentication successful"},
			{Expect: "CAP END"},
			{Send: ":server 005 testnick CHANMODES=b,k,l,imnpst PREFIX=(ov)@+ CASEMAPPING=rfc1459 WHOX :are supported by this server"},
			{Send: ":server 001 testnick :Welcome"},
			{Send: ":server 002 testnick :Your host is server"},
			{Send: ":server 003 testnick :This server was created once"},
			{Send: ":server 004 testnick server ircd iow nt"},
			{Send: ":testnick MODE testnick :+i"},
			{Send: ":server 376 testnick :End of /MOTD command."},
			{ExpectContains: "JOIN #chan"},
		})
	}()

	runDone := make(chan error, 1)
	go func() { runDone <- u.session(ctx) }()

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timeout script")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if u.Registered() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !u.Registered() {
		t.Fatal("not registered")
	}
	if !u.HasCap("sasl") {
		t.Fatal("missing sasl cap")
	}
	if !u.ISUPPORT().WHOX {
		t.Fatal("expected WHOX")
	}
	_, _, rpl004 := u.Welcome()
	if len(rpl004) < 3 {
		t.Fatalf("expected 004 stored, got %v", rpl004)
	}
	if u.UserModes() != "+i" {
		t.Fatalf("umodes=%q", u.UserModes())
	}
	h.mu.Lock()
	ok := h.reg
	h.mu.Unlock()
	if !ok {
		t.Fatal("handler not notified")
	}
	cancel()
	<-runDone
}

func TestUplinkSASLRequiredFailure(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	u := New(Config{
		Network: store.Network{
			Name: "test", Host: "pipe", Port: 1, Nick: "testnick",
			SASLUser: "u", SASLPass: "p", SASLRequired: true,
		},
		MinBackoff: time.Hour,
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return client, nil
		},
	}, &regHandler{})

	go func() {
		_ = testutil.RunScript(ctx, server, []testutil.ScriptStep{
			{ExpectContains: "CAP LS"},
			{ExpectContains: "NICK"},
			{ExpectContains: "USER"},
			{Send: "CAP * LS :sasl"},
			{ExpectContains: "CAP REQ"},
			{Send: "CAP * ACK :sasl"},
			{Expect: "AUTHENTICATE PLAIN"},
			{Send: "AUTHENTICATE +"},
			{ExpectContains: "AUTHENTICATE "},
			{Send: ":server 904 testnick :SASL authentication failed"},
		})
	}()

	err := u.session(ctx)
	if err == nil {
		t.Fatal("expected SASL failure")
	}
	cancel()
}

func TestPickSASLMech(t *testing.T) {
	fx := testutil.NewTLSFixture(t)
	cases := []struct {
		name    string
		user    string
		pass    string
		tlsConf *tls.Config
		offered []string
		want    string
		ok      bool
	}{
		{name: "legacy empty prefers PLAIN", user: "u", pass: "p", want: "PLAIN", ok: true},
		{name: "prefer SCRAM over PLAIN", user: "u", pass: "p", offered: []string{"PLAIN", "SCRAM-SHA-256"}, want: "SCRAM-SHA-256", ok: true},
		{name: "PLAIN when no SCRAM", user: "u", pass: "p", offered: []string{"EXTERNAL", "PLAIN"}, want: "PLAIN", ok: true},
		{name: "EXTERNAL with cert no password", tlsConf: fx.ClientTLS, offered: []string{"EXTERNAL", "PLAIN"}, want: "EXTERNAL", ok: true},
		{name: "EXTERNAL optional user with cert", user: "acct", tlsConf: fx.ClientTLS, offered: []string{"EXTERNAL"}, want: "EXTERNAL", ok: true},
		{name: "password prefers SCRAM over EXTERNAL", user: "u", pass: "p", tlsConf: fx.ClientTLS, offered: []string{"EXTERNAL", "SCRAM-SHA-256", "PLAIN"}, want: "SCRAM-SHA-256", ok: true},
		{name: "no match", user: "u", pass: "p", offered: []string{"EXTERNAL"}, ok: false},
		{name: "no credentials", offered: []string{"PLAIN", "SCRAM-SHA-256", "EXTERNAL"}, ok: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := New(Config{
				Network: store.Network{SASLUser: tc.user, SASLPass: tc.pass},
				TLSConf: tc.tlsConf,
			}, nil)
			u.saslMechs = tc.offered
			got, ok := u.pickSASLMech()
			if ok != tc.ok || got != tc.want {
				t.Fatalf("got %q,%v want %q,%v", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestUplinkSASLExternal(t *testing.T) {
	fx := testutil.NewTLSFixture(t)
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	u := New(Config{
		Network: store.Network{
			Name: "test", Host: "pipe", Port: 1, Nick: "testnick",
			SASLUser: "acct", // optional authzid
		},
		TLSConf:    fx.ClientTLS,
		MinBackoff: time.Hour,
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return client, nil
		},
	}, &regHandler{})

	scriptDone := make(chan error, 1)
	go func() {
		scriptDone <- testutil.RunScript(ctx, server, []testutil.ScriptStep{
			{ExpectContains: "CAP LS"},
			{ExpectContains: "NICK"},
			{ExpectContains: "USER"},
			{Send: "CAP * LS :sasl=EXTERNAL,PLAIN cap-notify"},
			{ExpectContains: "CAP REQ"},
			{Send: "CAP * ACK :sasl cap-notify"},
			{Expect: "AUTHENTICATE EXTERNAL"},
			{Send: "AUTHENTICATE +"},
			{Expect: "AUTHENTICATE " + mustB64("acct")},
			{Send: ":server 903 testnick :SASL authentication successful"},
			{Expect: "CAP END"},
			{Send: ":server 001 testnick :Welcome"},
			{Send: ":server 376 testnick :End of /MOTD command."},
		})
	}()

	runDone := make(chan error, 1)
	go func() { runDone <- u.session(ctx) }()

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timeout script")
	}
	cancel()
	<-runDone
}

func TestUplinkSASLExternalEmptyAuthzid(t *testing.T) {
	fx := testutil.NewTLSFixture(t)
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	u := New(Config{
		Network: store.Network{
			Name: "test", Host: "pipe", Port: 1, Nick: "testnick",
		},
		TLSConf:    fx.ClientTLS,
		MinBackoff: time.Hour,
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return client, nil
		},
	}, &regHandler{})

	scriptDone := make(chan error, 1)
	go func() {
		scriptDone <- testutil.RunScript(ctx, server, []testutil.ScriptStep{
			{ExpectContains: "CAP LS"},
			{ExpectContains: "NICK"},
			{ExpectContains: "USER"},
			{Send: "CAP * LS :sasl=EXTERNAL"},
			{ExpectContains: "CAP REQ"},
			{Send: "CAP * ACK :sasl"},
			{Expect: "AUTHENTICATE EXTERNAL"},
			{Send: "AUTHENTICATE +"},
			{Expect: "AUTHENTICATE +"},
			{Send: ":server 903 testnick :SASL authentication successful"},
			{Expect: "CAP END"},
			{Send: ":server 001 testnick :Welcome"},
			{Send: ":server 376 testnick :End of /MOTD command."},
		})
	}()

	runDone := make(chan error, 1)
	go func() { runDone <- u.session(ctx) }()

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timeout script")
	}
	cancel()
	<-runDone
}

func TestUplinkSASLSCRAMSHA256(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const user, pass = "alice", "secret"
	credClient, err := scram.SHA256.NewClient(user, pass, "")
	if err != nil {
		t.Fatal(err)
	}
	stored := credClient.GetStoredCredentials(scram.KeyFactors{Salt: "saltSALTsalt", Iters: 4096})
	srv, err := scram.SHA256.NewServer(func(username string) (scram.StoredCredentials, error) {
		if username != user {
			return scram.StoredCredentials{}, fmt.Errorf("unknown user")
		}
		return stored, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sconv := srv.NewConversation()

	u := New(Config{
		Network: store.Network{
			Name: "test", Host: "pipe", Port: 1, Nick: "testnick",
			SASLUser: user, SASLPass: pass,
		},
		MinBackoff: time.Hour,
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return client, nil
		},
	}, &regHandler{})

	scriptDone := make(chan error, 1)
	go func() {
		br := bufio.NewReader(server)
		deadline := time.Now().Add(4 * time.Second)
		read := func() (string, error) {
			_ = server.SetReadDeadline(deadline)
			line, err := br.ReadString('\n')
			return strings.TrimRight(line, "\r\n"), err
		}
		write := func(s string) error {
			_, err := io.WriteString(server, s+"\r\n")
			return err
		}
		var err error
		for _, wantSub := range []string{"CAP LS", "NICK", "USER"} {
			line, e := read()
			if e != nil {
				scriptDone <- e
				return
			}
			if !strings.Contains(line, wantSub) {
				scriptDone <- fmt.Errorf("want %q in %q", wantSub, line)
				return
			}
		}
		if err = write("CAP * LS :sasl=SCRAM-SHA-256,PLAIN"); err != nil {
			scriptDone <- err
			return
		}
		line, err := read()
		if err != nil || !strings.Contains(line, "CAP REQ") {
			scriptDone <- fmt.Errorf("CAP REQ: %q %v", line, err)
			return
		}
		if err = write("CAP * ACK :sasl"); err != nil {
			scriptDone <- err
			return
		}
		line, err = read()
		if err != nil || line != "AUTHENTICATE SCRAM-SHA-256" {
			scriptDone <- fmt.Errorf("mech: %q %v", line, err)
			return
		}
		if err = write("AUTHENTICATE +"); err != nil {
			scriptDone <- err
			return
		}
		line, err = read()
		if err != nil || !strings.HasPrefix(line, "AUTHENTICATE ") {
			scriptDone <- fmt.Errorf("client-first: %q %v", line, err)
			return
		}
		clientFirst, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(line, "AUTHENTICATE "))
		if err != nil {
			scriptDone <- err
			return
		}
		serverFirst, err := sconv.Step(string(clientFirst))
		if err != nil {
			scriptDone <- err
			return
		}
		if err = write("AUTHENTICATE " + base64.StdEncoding.EncodeToString([]byte(serverFirst))); err != nil {
			scriptDone <- err
			return
		}
		line, err = read()
		if err != nil || !strings.HasPrefix(line, "AUTHENTICATE ") {
			scriptDone <- fmt.Errorf("client-final: %q %v", line, err)
			return
		}
		clientFinal, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(line, "AUTHENTICATE "))
		if err != nil {
			scriptDone <- err
			return
		}
		serverFinal, err := sconv.Step(string(clientFinal))
		if err != nil {
			scriptDone <- err
			return
		}
		if err = write("AUTHENTICATE " + base64.StdEncoding.EncodeToString([]byte(serverFinal))); err != nil {
			scriptDone <- err
			return
		}
		if err = write(":server 903 testnick :SASL authentication successful"); err != nil {
			scriptDone <- err
			return
		}
		line, err = read()
		if err != nil || line != "CAP END" {
			scriptDone <- fmt.Errorf("CAP END: %q %v", line, err)
			return
		}
		_ = write(":server 001 testnick :Welcome")
		_ = write(":server 376 testnick :End of /MOTD command.")
		scriptDone <- nil
	}()

	runDone := make(chan error, 1)
	go func() { runDone <- u.session(ctx) }()

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timeout script")
	}
	cancel()
	<-runDone
}

func mustB64(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

