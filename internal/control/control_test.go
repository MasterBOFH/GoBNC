package control

import (
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestClientRoundTrip(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "gobnc.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 64)
		n, _ := c.Read(buf)
		if string(buf[:n]) != "PING\n" {
			t.Errorf("got %q", buf[:n])
		}
		_, _ = c.Write([]byte("OK\n"))
	}()
	time.Sleep(20 * time.Millisecond)
	resp, err := Client(sock, "PING")
	if err != nil || resp != "OK" {
		t.Fatalf("resp=%q err=%v", resp, err)
	}
}

func TestTryNotifyMissing(t *testing.T) {
	ok, err := TryNotify(filepath.Join(t.TempDir(), "missing.sock"), "PING")
	if err != nil || ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}
