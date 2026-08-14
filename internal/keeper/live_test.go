//go:build ircd

// Live integration test against a real ircd (docker/ircd's ergo service).
// Run: (cd docker/ircd && docker compose up -d ergo) && go test -tags ircd ./internal/keeper/... -run Live -v
package keeper

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const ergoAddr = "127.0.0.1"
const ergoPort = 6667

var nickCounter atomic.Uint64

func nickFor(t *testing.T) string {
	return fmt.Sprintf("kpr%d%d", time.Now().Unix()%100000, nickCounter.Add(1))
}

// registerMinimal sends NICK/USER and waits for 001 in the ring buffer.
func registerMinimal(t *testing.T, k *Keeper, nick string) (welcomeSeq uint64) {
	t.Helper()
	if err := k.WriteLine("NICK " + nick); err != nil {
		t.Fatalf("write NICK: %v", err)
	}
	if err := k.WriteLine("USER " + nick + " 0 * :keeper live test"); err != nil {
		t.Fatalf("write USER: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	var after uint64
	for time.Now().Before(deadline) {
		entries, _ := k.Since(after)
		for _, e := range entries {
			after = e.Seq
			// crude: look for " 001 " welcome numeric addressed to our nick
			if strings.Contains(e.Line, " 001 ") {
				return e.Seq
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no 001 welcome from ergo within timeout")
	return 0
}

func TestLiveDialRegisterAgainstErgo(t *testing.T) {
	k := New(8192, 4096, nil, WithReadIdleTimeout(90*time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: ergoAddr, Port: ergoPort}); err != nil {
		t.Fatalf("Dial ergo: %v", err)
	}
	defer k.Close()

	seq := registerMinimal(t, k, nickFor(t))
	t.Logf("registered, welcome at seq=%d, LastSeq=%d", seq, k.LastSeq())

	st, epoch := k.State()
	if st != Connected || epoch != 1 {
		t.Fatalf("state=%v epoch=%d, want Connected/1", st, epoch)
	}
}

// TestLiveIdleHoldsConnection proves the keeper's own read-idle timeout
// doesn't fire during a quiet period shorter than it, and that if the real
// server sends anything (including its own PING) it's seq'd normally.
func TestLiveIdleHoldsConnection(t *testing.T) {
	k := New(8192, 4096, nil, WithReadIdleTimeout(90*time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: ergoAddr, Port: ergoPort}); err != nil {
		t.Fatalf("Dial ergo: %v", err)
	}
	defer k.Close()
	registerMinimal(t, k, nickFor(t))

	time.Sleep(20 * time.Second)

	st, _ := k.State()
	if st != Connected {
		t.Fatalf("state=%v after 20s idle, want still Connected (err=%v)", st, k.LastError())
	}
}

// TestLiveForcedDropAndRedial is the build-order step 1 gate's minimum bar:
// a forced dial -> drop -> dial cycle against a real ircd, not a fake one.
func TestLiveForcedDropAndRedial(t *testing.T) {
	k := New(8192, 4096, nil, WithReadIdleTimeout(90*time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: ergoAddr, Port: ergoPort}); err != nil {
		t.Fatalf("Dial #1: %v", err)
	}
	registerMinimal(t, k, nickFor(t))

	events, unsub := k.Subscribe()
	defer unsub()

	if err := k.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	st, _ := k.State()
	if st != NotConnected {
		t.Fatalf("state=%v after Close, want NotConnected", st)
	}
	select {
	case ev := <-events:
		t.Logf("post-close event: %+v", ev)
	case <-time.After(500 * time.Millisecond):
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	if err := k.Dial(ctx2, DialConfig{Host: ergoAddr, Port: ergoPort}); err != nil {
		t.Fatalf("Dial #2: %v", err)
	}
	defer k.Close()
	registerMinimal(t, k, nickFor(t))

	st, epoch := k.State()
	if st != Connected || epoch != 2 {
		t.Fatalf("state=%v epoch=%d after redial, want Connected/2", st, epoch)
	}
}

// ergoContainer is docker/ircd's ergo service container name per its
// docker-compose.yml (project directory "ircd" -> "ircd-ergo-1").
const ergoContainer = "ircd-ergo-1"

// TestLiveServerKilledUnderneathUs is the asymmetric-drop half of the step 1
// gate: the socket must die because the network died, not because we asked
// it to — Close() was never called on our side. This is what an unattended
// respawn-and-redial loop actually has to detect.
func TestLiveServerKilledUnderneathUs(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available to kill/restart the ergo container")
	}

	k := New(8192, 4096, nil, WithReadIdleTimeout(90*time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: ergoAddr, Port: ergoPort}); err != nil {
		t.Fatalf("Dial #1: %v", err)
	}
	registerMinimal(t, k, nickFor(t))

	events, unsub := k.Subscribe()
	defer unsub()

	if out, err := exec.Command("docker", "kill", ergoContainer).CombinedOutput(); err != nil {
		t.Fatalf("docker kill %s: %v: %s", ergoContainer, err, out)
	}
	t.Cleanup(func() {
		_, _ = exec.Command("docker", "start", ergoContainer).CombinedOutput()
	})

	select {
	case ev := <-events:
		if ev.Kind != EventDisconnected {
			t.Fatalf("got event kind %v, want EventDisconnected", ev.Kind)
		}
		if ev.Err == nil {
			t.Fatalf("server-killed connection reported nil Err — indistinguishable from a deliberate Close")
		}
		t.Logf("detected asymmetric drop: %v", ev.Err)
	case <-time.After(15 * time.Second):
		t.Fatalf("no EventDisconnected within 15s of docker kill %s", ergoContainer)
	}
	st, _ := k.State()
	if st != NotConnected {
		t.Fatalf("state=%v after server was killed, want NotConnected", st)
	}
	if err := k.LastError(); err == nil {
		t.Fatalf("LastError is nil after an asymmetric server-side drop")
	}

	if out, err := exec.Command("docker", "start", ergoContainer).CombinedOutput(); err != nil {
		t.Fatalf("docker start %s: %v: %s", ergoContainer, err, out)
	}

	// The container accepting TCP is not the same as the ircd inside it
	// being ready to register clients — a bare port-open check isn't enough
	// (ergo will accept and then immediately reset while it's still coming
	// up). Retry the whole dial+register as the actual readiness probe.
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		err := k.Dial(ctx2, DialConfig{Host: ergoAddr, Port: ergoPort})
		cancel2()
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if err := registerAttempt(k, nickFor(t)); err != nil {
			lastErr = err
			_ = k.Close()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		lastErr = nil
		break
	}
	if lastErr != nil {
		t.Fatalf("redial+register after restart never succeeded within 30s: %v", lastErr)
	}
	defer k.Close()

	st, epoch := k.State()
	if st != Connected || epoch != 2 {
		t.Fatalf("state=%v epoch=%d after restart+redial, want Connected/2", st, epoch)
	}
}

// registerAttempt is registerMinimal's non-fatal sibling, for use in a retry
// loop where a failed attempt should be retried rather than fail the test.
func registerAttempt(k *Keeper, nick string) error {
	if err := k.WriteLine("NICK " + nick); err != nil {
		return err
	}
	if err := k.WriteLine("USER " + nick + " 0 * :keeper live test"); err != nil {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	var after uint64
	for time.Now().Before(deadline) {
		entries, _ := k.Since(after)
		for _, e := range entries {
			after = e.Seq
			if strings.Contains(e.Line, " 001 ") {
				return nil
			}
		}
		if st, _ := k.State(); st != Connected {
			return fmt.Errorf("connection dropped mid-registration: %v", k.LastError())
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("no 001 welcome within timeout")
}

// TestLiveSoakDialCloseCycles repeats dial->register->close against a real
// ircd and samples goroutine count each cycle. It's a short-form version of
// the reload-soak testing the design brief calls for once a brain exists —
// here there's no brain to restart, so it's exercising the keeper's own
// dial/read-loop/close lifecycle for leaks, which is a precondition for that
// later test being meaningful at all.
func TestLiveSoakDialCloseCycles(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	const cycles = 30
	k := New(8192, 4096, nil, WithReadIdleTimeout(90*time.Second))

	samples := make([]int, 0, cycles)
	for i := 0; i < cycles; i++ {
		// ergo's default connect-ip throttle rejects rapid-fire reconnects
		// from one address — real ircd behavior, not something to route
		// around. Pace under it; the keeper never rapid-fire redials on its
		// own anyway (dialling is brain-driven, one attempt at a time).
		time.Sleep(300 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := k.Dial(ctx, DialConfig{Host: ergoAddr, Port: ergoPort})
		cancel()
		if err != nil {
			t.Fatalf("cycle %d: Dial: %v", i, err)
		}
		if err := registerAttempt(k, nickFor(t)); err != nil {
			t.Fatalf("cycle %d: register: %v", i, err)
		}
		if err := k.Close(); err != nil {
			t.Fatalf("cycle %d: Close: %v", i, err)
		}
		runtime.GC()
		samples = append(samples, runtime.NumGoroutine())
	}

	st, epoch := k.State()
	if st != NotConnected || epoch != cycles {
		t.Fatalf("state=%v epoch=%d after %d cycles, want NotConnected/%d", st, epoch, cycles, cycles)
	}

	// Report the trend rather than asserting a hard bound: a handful of
	// stray goroutines from the Go runtime/test harness is normal, but
	// goroutine count should not be climbing with cycle count.
	warmup := avg(samples[5:15])
	tail := avg(samples[len(samples)-10:])
	t.Logf("goroutines: first=%d warmup_avg=%.1f tail_avg=%.1f last=%d dropped_ring_entries=%d",
		samples[0], warmup, tail, samples[len(samples)-1], k.DroppedCount())
	if tail > warmup+5 {
		t.Errorf("goroutine count trending up: warmup_avg=%.1f tail_avg=%.1f over %d cycles — possible leak", warmup, tail, cycles)
	}
}

func avg(xs []int) float64 {
	if len(xs) == 0 {
		return 0
	}
	sum := 0
	for _, x := range xs {
		sum += x
	}
	return float64(sum) / float64(len(xs))
}
