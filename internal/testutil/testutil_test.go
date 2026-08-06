package testutil

import (
	"context"
	"io"
	"testing"
	"time"
)

func TestFakePeerRoundTrip(t *testing.T) {
	fp := NewFakePeer(t)
	go func() {
		_, _ = io.WriteString(fp.Peer, "PING :hi\r\n")
	}()
	if err := fp.Expect("PING :hi", time.Second); err != nil {
		t.Fatal(err)
	}
	done := make(chan string, 1)
	go func() {
		buf := make([]byte, 64)
		n, err := fp.Peer.Read(buf)
		if err != nil {
			done <- err.Error()
			return
		}
		done <- string(buf[:n])
	}()
	if err := fp.Send("PONG :hi"); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got != "PONG :hi\r\n" {
		t.Fatalf("%q", got)
	}
}

func TestTLSFixture(t *testing.T) {
	fx := NewTLSFixture(t)
	if fx.ClientSHA256 == "" || len(fx.ClientSHA256) != 64 {
		t.Fatalf("fp=%q", fx.ClientSHA256)
	}
	if fx.ServerTLS == nil || fx.ClientTLS == nil {
		t.Fatal("missing tls config")
	}
}

func TestRunScript(t *testing.T) {
	fp := NewFakePeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunScript(ctx, fp.Peer, []ScriptStep{
			{Expect: "NICK bob", Send: ":server 001 bob :welcome"},
		})
	}()
	if err := fp.Send("NICK bob"); err != nil {
		t.Fatal(err)
	}
	line, err := fp.ReadLine(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if line != ":server 001 bob :welcome" {
		t.Fatal(line)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}
