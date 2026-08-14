package brain

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/flood"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
)

// Flood pacing: ported from internal/uplink/flood.go's shape almost
// directly — same bucket, same queue-then-drain structure — but per
// network (Driver serves every network in the process, not one per
// instance) and writing via AttachClient.SendWrite instead of a raw
// socket. The backpressure signal that used to be "u.conn == nil" (checked
// synchronously before queuing) is now WriteResultMsg.Refused, observed
// asynchronously in Driver.Run and reacted to by clearFloodQueue — see
// WriteResultMsg's doc comment for why that's the right analogue.
type floodState struct {
	bucket *flood.ByteBucket
	cancel context.CancelFunc

	mu    sync.Mutex
	queue []string
	wake  chan struct{}
}

// SetMaxFloodQueue caps the paced outbound queue depth for every tracked
// network (0 = unlimited) — mirrors internal/uplink.Uplink.SetMaxFloodQueue,
// which was likewise one bouncer-wide setting (gobnc.json's
// max_flood_queue) applied uniformly, not a per-network knob.
func (d *Driver) SetMaxFloodQueue(n int) {
	d.floodMu.Lock()
	d.maxFloodQueue = n
	d.floodMu.Unlock()
}

// SetFloodParams configures (or reconfigures) network id's flood-pacing
// bucket — mirrors internal/uplink.Uplink.SetNetwork's flood side effect.
// Burst/rate <=0 disables pacing for id: WriteRaw then writes immediately,
// unpaced, exactly like the old floodEnabled()==false path did.
func (d *Driver) SetFloodParams(id keeper.NetworkID, burst int, rate float64) {
	fs := d.floodStateFor(id)
	fs.bucket.Configure(burst, rate)
	d.kickFlood(fs)
}

func (d *Driver) floodStateFor(id keeper.NetworkID) *floodState {
	d.floodMu.Lock()
	fs, ok := d.flood[id]
	if !ok {
		ctx, cancel := context.WithCancel(context.Background())
		fs = &floodState{
			bucket: flood.NewByteBucket(0, 0),
			cancel: cancel,
			wake:   make(chan struct{}, 1),
		}
		d.flood[id] = fs
		go d.floodDrainLoop(ctx, id, fs)
	}
	d.floodMu.Unlock()
	return fs
}

// WriteRaw is the paced entry point for post-registration outbound
// traffic — mirrors Uplink.WriteRaw: queues behind id's flood bucket when
// pacing is enabled, otherwise writes immediately via the keeper. Driver's
// own internal writes (registration opening lines, auto-join, nick
// recovery) deliberately go straight through AttachClient.SendWrite
// instead, unpaced, matching internal/uplink's own writeImmediate
// carve-outs for PONG and the registration handshake.
func (d *Driver) WriteRaw(id keeper.NetworkID, line string) error {
	fs := d.floodStateFor(id)
	if !fs.bucket.Enabled() {
		return d.client.SendWrite(id, line)
	}
	d.floodMu.Lock()
	max := d.maxFloodQueue
	d.floodMu.Unlock()
	fs.mu.Lock()
	if max > 0 && len(fs.queue) >= max {
		fs.mu.Unlock()
		return fmt.Errorf("brain: flood queue full (%d)", max)
	}
	fs.queue = append(fs.queue, line)
	fs.mu.Unlock()
	d.kickFlood(fs)
	return nil
}

func (d *Driver) kickFlood(fs *floodState) {
	select {
	case fs.wake <- struct{}{}:
	default:
	}
}

func (d *Driver) floodDrainLoop(ctx context.Context, id keeper.NetworkID, fs *floodState) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-fs.wake:
		}
		for {
			fs.mu.Lock()
			if len(fs.queue) == 0 {
				fs.mu.Unlock()
				break
			}
			line := fs.queue[0]
			fs.queue = fs.queue[1:]
			fs.mu.Unlock()

			if err := fs.bucket.Take(ctx, wireBytes(line)); err != nil {
				return
			}
			_ = d.client.SendWrite(id, line) // best-effort; outcome surfaces as a later WriteResult
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}

// clearFloodQueue drops network id's pending paced writes — called when a
// WriteResultMsg reports Refused (no live connection to write to at all;
// see WriteResultMsg's doc comment), matching internal/uplink's own
// "drop remaining queue on write failure" behavior in floodDrainLoop.
func (d *Driver) clearFloodQueue(id keeper.NetworkID) {
	d.floodMu.Lock()
	fs, ok := d.flood[id]
	d.floodMu.Unlock()
	if !ok {
		return
	}
	fs.mu.Lock()
	fs.queue = nil
	fs.mu.Unlock()
}

// WaitFloodDrained blocks until id's paced queue is empty or ctx is done —
// mirrors internal/uplink.Uplink.waitFloodDrained, used by
// session.Session.GracefulQuit to let queued client traffic actually reach
// the wire before sending QUIT, bounded by the same ctx the caller already
// bounds the whole graceful-shutdown sequence with.
func (d *Driver) WaitFloodDrained(ctx context.Context, id keeper.NetworkID) {
	fs := d.floodStateFor(id)
	if !fs.bucket.Enabled() {
		return
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		fs.mu.Lock()
		n := len(fs.queue)
		fs.mu.Unlock()
		if n == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func wireBytes(line string) int {
	n := len(line)
	if !strings.HasSuffix(line, "\r\n") {
		n += 2
	}
	return n
}
