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

func TestNotifyStreamCollectsStatusThenOK(t *testing.T) {
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
		if string(buf[:n]) != "RELOAD\n" {
			t.Errorf("got %q", buf[:n])
		}
		_, _ = c.Write([]byte("STATUS spawning replacement\n"))
		_, _ = c.Write([]byte("STATUS spawned pid 1234, waiting for handoff\n"))
		_, _ = c.Write([]byte("OK\n"))
	}()
	time.Sleep(20 * time.Millisecond)

	var lines []string
	ok, err := NotifyStream(sock, "RELOAD", 2*time.Second, func(l string) { lines = append(lines, l) })
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	want := []string{"spawning replacement", "spawned pid 1234, waiting for handoff"}
	if len(lines) != len(want) || lines[0] != want[0] || lines[1] != want[1] {
		t.Fatalf("lines=%v want=%v", lines, want)
	}
}

func TestNotifyStreamErr(t *testing.T) {
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
		_, _ = c.Read(buf)
		_, _ = c.Write([]byte("STATUS spawning replacement\n"))
		_, _ = c.Write([]byte("ERR spawn failed: fork/exec: no such file or directory\n"))
	}()
	time.Sleep(20 * time.Millisecond)

	var lines []string
	ok, err := NotifyStream(sock, "RELOAD", 2*time.Second, func(l string) { lines = append(lines, l) })
	if !ok {
		t.Fatal("expected ok=true (connection succeeded; ERR is a real reply, not a dial failure)")
	}
	if err == nil {
		t.Fatal("expected an error for ERR reply")
	}
	if len(lines) != 1 || lines[0] != "spawning replacement" {
		t.Fatalf("lines=%v", lines)
	}
}
