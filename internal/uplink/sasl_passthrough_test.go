package uplink

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
)

func TestUplinkNoSASLCredsDoesNotREQ(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	u := New(Config{
		Network: store.Network{
			Name: "test", Host: "pipe", Port: 1, Nick: "testnick",
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
		for _, want := range []string{"CAP LS", "NICK", "USER"} {
			line, err := read()
			if err != nil || !strings.Contains(line, want) {
				scriptDone <- fmt.Errorf("%s: %q %v", want, line, err)
				return
			}
		}
		if err := write("CAP * LS :sasl=PLAIN,EXTERNAL message-tags cap-notify"); err != nil {
			scriptDone <- err
			return
		}
		line, err := read()
		if err != nil {
			scriptDone <- err
			return
		}
		if !strings.Contains(line, "CAP REQ") {
			scriptDone <- fmt.Errorf("want CAP REQ, got %q", line)
			return
		}
		if strings.Contains(line, "sasl") {
			scriptDone <- fmt.Errorf("REQ must not include sasl: %q", line)
			return
		}
		if err := write("CAP * ACK :message-tags cap-notify"); err != nil {
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
		t.Fatal("timeout")
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
	if u.HasCap("sasl") {
		t.Fatal("should not have REQed sasl")
	}
	mechs, ok := u.SASLAvailable()
	if !ok {
		t.Fatal("sasl should still be available for passthrough")
	}
	if len(mechs) != 2 {
		t.Fatalf("mechs=%v", mechs)
	}
	cancel()
	<-runDone
}
