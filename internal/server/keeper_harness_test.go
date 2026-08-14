package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/brain"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
)

// attachTestKeeper wires a real, in-process keeper.Manager + keeper.Listener
// + brain.Driver onto s (setting s.keeperClient/s.driver directly — legal
// here since these tests live in package server) and starts driver.Run plus
// s's own runDemux goroutine — the exact same shape Server.Run's
// bootstrapKeeper sets up for real, just attaching to an in-process keeper
// instead of spawning/attaching to a real separate gobnc-keeper process.
// Needed because these tests construct *Server directly via New and drive
// it through serveControl/StartNetworkByName without ever calling Run —
// startNetworkLocked (and friends) now unconditionally use s.driver, which
// is nil until something does what this does.
func attachTestKeeper(t *testing.T, s *Server) {
	t.Helper()
	mgr := keeper.NewManager(1<<20, 4096, nil)
	// keeper.Listener.Serve requires its socket directory to be mode 0700
	// (see internal/keeper/security.go's ensureSocketDir) — t.TempDir()'s
	// own mode isn't guaranteed to already be that.
	sockDir := filepath.Join(t.TempDir(), "sock")
	if err := os.Mkdir(sockDir, 0700); err != nil {
		t.Fatalf("mkdir sockDir: %v", err)
	}
	sockPath := filepath.Join(sockDir, "keeper.sock")

	listenerCtx, cancelListener := context.WithCancel(context.Background())
	l := keeper.NewListener(mgr, nil)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = l.Serve(listenerCtx, sockPath)
	}()
	<-ready
	for i := 0; i < 200; i++ {
		if _, err := os.Stat(sockPath); err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 5*time.Second)
	client, err := keeper.Attach(attachCtx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	cancelAttach()
	if err != nil {
		cancelListener()
		t.Fatalf("keeper.Attach: %v", err)
	}
	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}

	s.keeperClient = client
	s.driver = brain.NewDriver(client)

	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelRun()
		_ = client.Close()
		cancelListener()
	})
	go func() { _ = s.driver.Run(runCtx) }()
	go s.runDemux(runCtx)
}
