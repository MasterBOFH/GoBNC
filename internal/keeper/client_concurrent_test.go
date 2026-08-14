package keeper

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentSendDoesNotCorruptFrameStream proves AttachClient.Send* is
// safe to call from many goroutines at once — the shape every real brain
// process exercises the moment more than one network is live: each
// network's own nick-recovery loop and flood-pacing drain loop (see
// internal/brain/nickrecovery.go, flood.go) runs on its own goroutine, on
// top of armReconnect's timer callbacks and admin-triggered Dial/Close/
// Reconnect calls, and all of them write through the one AttachClient every
// network in the process shares.
//
// Without serializing writeFrame's two separate Write calls (header, then
// body) — see AttachClient.wmu's doc comment — two goroutines' frames can
// interleave on the wire and desync the whole stream. The keeper's readLoop
// has no way to resync a corrupted frame and tears down the entire live
// session (every network on it, not just the one whose write collided) the
// moment it fails to decode one. That would show up here as Next erroring,
// or as fewer results arriving than requests were sent, well before the
// deadline. Run with -race: even a run that doesn't trip the frame-corrupt
// failure mode should still catch the underlying unsynchronized access to
// net.Conn.Write.
func TestConcurrentSendDoesNotCorruptFrameStream(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
	sockPath := startTestListener(t, mgr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := Attach(ctx, sockPath, HelloMsg{Mode: ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()
	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}

	const workers = 25
	const perWorker = 40
	const want = workers * perWorker * 2 // SendWrite + SendClose per iteration

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// Every worker uses its own network ID so mixing SendWrite and
			// SendClose never races against another worker's Close for the
			// same network — the point of this test is the wire framing,
			// not keeper-side network semantics.
			id := NetworkID(w + 1)
			for i := 0; i < perWorker; i++ {
				if err := client.SendWrite(id, fmt.Sprintf("NOTICE * :worker %d line %d", w, i)); err != nil {
					t.Errorf("worker %d: SendWrite: %v", w, err)
					return
				}
				if err := client.SendClose(id); err != nil {
					t.Errorf("worker %d: SendClose: %v", w, err)
					return
				}
			}
		}(w)
	}

	var got atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for got.Load() < want {
			ev, err := client.Next()
			if err != nil {
				t.Errorf("Next after %d/%d results: %v", got.Load(), want, err)
				return
			}
			if ev.WriteResult == nil && ev.CloseResult == nil {
				t.Errorf("unexpected event %+v", ev)
				return
			}
			got.Add(1)
		}
	}()

	wg.Wait()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for results: got %d/%d", got.Load(), want)
	}
	if n := got.Load(); n != want {
		t.Fatalf("got %d results, want %d", n, want)
	}
}
