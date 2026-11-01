package main

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/config"
)

func TestModuleIsABuiltinVerb(t *testing.T) {
	if !builtinVerbs["module"] {
		t.Fatal("gobnc module would be sent to the modules as a command")
	}
	if builtinVerbs["devices"] {
		t.Fatal("a module command is treated as built in")
	}
}

func TestCmdModuleCtlSendsModuleCtl(t *testing.T) {
	dir, err := os.MkdirTemp("", "gcm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "c.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan string, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		line, _ := bufio.NewReader(c).ReadString('\n')
		got <- line
		_, _ = c.Write([]byte("OK [\"reloaded retecloud: pid 1 → 2\"]\n"))
	}()
	if err := cmdModuleCtl(config.Config{ControlSocket: sock}, []string{"reload", "retecloud"}); err != nil {
		t.Fatal(err)
	}
	if line := <-got; line != "MODULECTL reload retecloud\n" {
		t.Fatalf("control line = %q", line)
	}
}
