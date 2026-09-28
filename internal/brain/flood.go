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
	lines  *flood.LinePacer
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

// FloodParams is one network's outbound pacing.
type FloodParams struct {
	// Burst (bytes) and Rate (bytes/sec) configure the byte bucket;
	// either <=0 disables it.
	Burst int
	Rate  float64
	// Lines paces per line the way ircds meter clients (see
	// flood.LinePacer) instead, and Burst/Rate are ignored.
	Lines bool
}

// SetFloodParams configures (or reconfigures) network id's flood pacing —
// mirrors internal/uplink.Uplink.SetNetwork's flood side effect. With
// pacing disabled, WriteRaw writes immediately, unpaced, exactly like the
// old floodEnabled()==false path did.
func (d *Driver) SetFloodParams(id keeper.NetworkID, p FloodParams) {
	fs := d.floodStateFor(id)
	if p.Lines {
		fs.bucket.Configure(0, 0)
		fs.lines.Configure(true)
	} else {
		fs.lines.Configure(false)
		fs.bucket.Configure(p.Burst, p.Rate)
	}
	d.kickFlood(fs)
}

// pacingEnabled reports whether either pacer is active for fs.
func (fs *floodState) pacingEnabled() bool {
	return fs.lines.Enabled() || fs.bucket.Enabled()
}

// take waits on whichever pacer is active before a line of n wire bytes.
func (fs *floodState) take(ctx context.Context, n int) error {
	if fs.lines.Enabled() {
		return fs.lines.Take(ctx, n)
	}
	return fs.bucket.Take(ctx, n)
}

func (d *Driver) floodStateFor(id keeper.NetworkID) *floodState {
	d.floodMu.Lock()
	fs, ok := d.flood[id]
	if !ok {
		ctx, cancel := context.WithCancel(context.Background())
		fs = &floodState{
			bucket: flood.NewByteBucket(0, 0),
			lines:  flood.NewLinePacer(),
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
	if !fs.pacingEnabled() {
		return d.sendLine(id, line)
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

			if err := fs.take(ctx, wireBytes(line)); err != nil {
				return
			}
			_ = d.sendLine(id, line) // best-effort; outcome surfaces as a later WriteResult
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
	if !fs.pacingEnabled() {
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
