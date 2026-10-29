package module

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/brain"
	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/registration"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// fakeIRCd is a scripted ircd: it registers whoever connects, offers echo-message +
// labeled-response when echo is set (echoing PRIVMSGs back with their label), and records
// every line it receives.
type fakeIRCd struct {
	ln   net.Listener
	echo bool
	mu   sync.Mutex
	got  []string
	conn net.Conn
}

func newFakeIRCd(t *testing.T, echo bool) *fakeIRCd {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	f := &fakeIRCd{ln: ln, echo: echo}
	go f.serve()
	return f
}

func (f *fakeIRCd) addr() (string, int) {
	h, p, _ := net.SplitHostPort(f.ln.Addr().String())
	n, _ := strconv.Atoi(p)
	return h, n
}

func (f *fakeIRCd) serve() {
	c, err := f.ln.Accept()
	if err != nil {
		return
	}
	f.mu.Lock()
	f.conn = c
	f.mu.Unlock()
	defer c.Close()
	send := func(l string) { _, _ = c.Write([]byte(l + "\r\n")) }
	r := bufio.NewReader(c)
	nick := "me"
	welcomed := false
	capDone := false
	gotUser := false
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		f.mu.Lock()
		f.got = append(f.got, line)
		f.mu.Unlock()
		msg, err := irc.Parse(line)
		if err != nil {
			continue
		}
		switch msg.Command {
		case "CAP":
			switch strings.ToUpper(msg.Param(0)) {
			case "LS":
				caps := ""
				if f.echo {
					caps = "echo-message labeled-response message-tags"
				}
				send(":fake.example CAP * LS :" + caps)
			case "REQ":
				send(":fake.example CAP * ACK :" + msg.Trailing())
			case "END":
				capDone = true
			}
		case "NICK":
			nick = msg.Param(0)
		case "USER":
			gotUser = true
		case "PRIVMSG", "NOTICE":
			if f.echo {
				prefix := ""
				if lbl, ok := msg.Tag("label"); ok {
					prefix = "@label=" + lbl + " "
				}
				send(prefix + ":" + nick + "!u@h " + msg.Command + " " + msg.Param(0) + " :" + msg.Trailing())
			}
		}
		if !welcomed && gotUser && (capDone || !f.echo) {
			welcomed = true
			send(":fake.example 001 " + nick + " :Welcome")
			send(":fake.example 376 " + nick + " :End of MOTD")
		}
	}
}

// received returns the lines the ircd got whose command is cmd.
func (f *fakeIRCd) received(cmd string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, l := range f.got {
		if m, err := irc.Parse(l); err == nil && m.Command == cmd {
			out = append(out, l)
		}
	}
	return out
}

// uplinkSession is a Session for network id connected through a real in-process keeper and
// Driver to ircd, registered when it returns.
func uplinkSession(t *testing.T, id int64, ircd *fakeIRCd, db *store.Store) *session.Session {
	t.Helper()
	dir, err := os.MkdirTemp("", "gbk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sockDir := filepath.Join(dir, "s")
	if err := os.Mkdir(sockDir, 0o700); err != nil {
		t.Fatal(err)
	}
	sockPath := filepath.Join(sockDir, "k.sock")
	mgr := keeper.NewManager(1<<20, 4096, nil)
	lctx, lcancel := context.WithCancel(context.Background())
	go func() { _ = keeper.NewListener(mgr, nil).Serve(lctx, sockPath) }()
	var client *keeper.AttachClient
	deadline := time.Now().Add(5 * time.Second)
	for {
		actx, acancel := context.WithTimeout(context.Background(), time.Second)
		client, err = keeper.Attach(actx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
		acancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			lcancel()
			t.Fatalf("keeper.Attach: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := client.SendLiveReady(); err != nil {
		t.Fatal(err)
	}
	driver := brain.NewDriver(client)
	netCfg := store.Network{ID: id, Name: "net", Nick: "me", Username: "u", Realname: "r"}
	var hist *history.Store
	if db != nil {
		hist = history.New(db)
	}
	sess := session.New(netCfg, db, hist, nil, driver)
	netID := sess.NetworkID()
	driver.RegisterNetwork(netID, brain.NetworkConfig{PrimaryNick: "me", Username: "u", Realname: "r"})
	driver.SetChannels(netID, nil)
	rctx, rcancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		rcancel()
		_ = client.Close()
		lcancel()
	})
	go func() { _ = driver.Run(rctx) }()
	go func() {
		for {
			select {
			case <-rctx.Done():
				return
			case line, ok := <-driver.Lines():
				if !ok {
					return
				}
				sess.HandleLine(line.Raw, line.Seq)
			case res, ok := <-driver.Results():
				if !ok {
					return
				}
				if res.State.Phase == registration.PhaseFailed {
					sess.HandleDisconnect(res.State.Err)
				}
			case ev, ok := <-driver.NetworkEvents():
				if !ok {
					return
				}
				if ev.Kind == keeper.EventDisconnected {
					sess.HandleDisconnect(fmt.Errorf("%s", ev.Error))
				}
			case dr, ok := <-driver.DialResults():
				if !ok {
					return
				}
				if dr.OK {
					_ = driver.StartRegistration(dr.Network)
				}
			}
		}
	}()
	host, port := ircd.addr()
	if err := driver.Dial(netID, keeper.DialConfig{Host: host, Port: port, DialTimeout: 5 * time.Second}, 0); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for !sess.Registered() {
		if time.Now().After(deadline) {
			t.Fatal("uplink never registered")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return sess
}
