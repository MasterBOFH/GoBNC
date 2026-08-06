package downlink

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

func TestClientCAPLSAdvertisesPassthroughSASL(t *testing.T) {
	s := session.New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.SetSASLOfferForTest("sasl=PLAIN,EXTERNAL")

	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })

	cl := &Client{
		id:   "c1",
		conn: client,
		r:    bufio.NewReader(client),
		caps: map[string]bool{},
		sess: s,
	}

	line := readCAPReply(t, server, func() {
		if err := handleClientCAP(cl, irc.Message{Command: "CAP", Params: []string{"LS", "302"}}); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(line, "sasl=PLAIN,EXTERNAL") {
		t.Fatalf("CAP LS missing sasl value: %q", line)
	}
	if !cl.cap302 {
		t.Fatal("cap302 not set")
	}

	cl.cap302 = false
	line = readCAPReply(t, server, func() {
		if err := handleClientCAP(cl, irc.Message{Command: "CAP", Params: []string{"LS"}}); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(line, "sasl") || strings.Contains(line, "sasl=") {
		t.Fatalf("want bare sasl without value: %q", line)
	}
}

func TestClientCAPREQSASLWithoutUplinkNAKs(t *testing.T) {
	s := session.New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.SetSASLOfferForTest("sasl=PLAIN")

	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })

	cl := &Client{
		id:   "c1",
		conn: client,
		r:    bufio.NewReader(client),
		caps: map[string]bool{},
		sess: s,
	}

	line := readCAPReply(t, server, func() {
		if err := handleClientCAP(cl, irc.Message{
			Command: "CAP",
			Params:  []string{"REQ", "sasl"},
		}); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(line, "NAK") || !strings.Contains(line, "sasl") {
		t.Fatalf("want CAP NAK sasl: %q", line)
	}
}

func readCAPReply(t *testing.T, server net.Conn, send func()) string {
	t.Helper()
	done := make(chan string, 1)
	go func() {
		_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
		br := bufio.NewReader(server)
		line, err := br.ReadString('\n')
		if err != nil {
			done <- "err:" + err.Error()
			return
		}
		done <- strings.TrimRight(line, "\r\n")
	}()
	send()
	line := <-done
	if strings.HasPrefix(line, "err:") {
		t.Fatal(line)
	}
	return line
}
