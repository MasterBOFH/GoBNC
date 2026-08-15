package keeper

import (
	"sync"
	"time"
)

// Entry is one line read from the uplink, tagged with its position in the
// stream and the connection generation it belongs to.
type Entry struct {
	Seq   uint64
	Epoch uint64
	Line  string
	Time  time.Time
}

// ring is a bounded, seq-addressable buffer of recently read lines.
// It is sized against a reconnect burst (e.g. a netsplit's QUIT flood during
// the crash-and-respawn window), not average throughput. When full, the
// oldest entry is evicted to make room.
//
// Two different "overflow" scenarios exist.
//
//  1. A live-attached client falls behind delivery for one network and its
//     per-connection buffer fills (Keeper.SubscribeLines' Overflow signal).
//     Listener does not kill the attach for this: the ring still holds the
//     lines, and fanInNetwork resubscribes and catches up via Since. Kill
//     only if that Since returns ok=false (this ring has already evicted
//     the catch-up window) — a genuine holed stream, the same gap Since
//     exists to report. A traffic burst (WHO/NAMES) overflowing the live
//     channel is not that; killing it used to break the keeper↔brain unix
//     socket with a broken pipe on the next WriteRequest.
//  2. The ring itself evicting (this type's push, below) independent of
//     whether anyone is consuming — e.g. a netsplit burst that outpaces
//     ring capacity with nobody attached to notice. The original
//     single-network design's policy was "kill the process." With multiple
//     networks per keeper process that's wrong: it would take down every
//     other network's uplink for a failure on one. Revised policy, now
//     wired in Keeper.readLoop: a ring overflow on network N, while N has
//     zero live line-subscribers, closes N's uplink and clears N's blob
//     store and resume watermark — loud, logged, one reconnect on one
//     network. since() below already reports ok=false the moment a
//     requested watermark has been evicted; readLoop's self-close makes
//     that failure happen immediately and loudly instead of waiting for a
//     future attach to discover the gap on its own.
type ring struct {
	mu      sync.Mutex
	buf     []Entry
	cap     int
	start   int // index of oldest entry
	len     int
	dropped uint64 // entries evicted before any consumer existed to read them
}

func newRing(capacity int) *ring {
	if capacity <= 0 {
		capacity = 4096
	}
	return &ring{buf: make([]Entry, capacity), cap: capacity}
}

// push appends e, evicting the oldest entry if the ring is full. Returns
// whether this call evicted an entry — the signal Keeper.readLoop acts on
// for the ring-overflow-case-2 policy above.
func (r *ring) push(e Entry) (evicted bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.len == r.cap {
		// Evict oldest.
		r.start = (r.start + 1) % r.cap
		r.len--
		r.dropped++
		evicted = true
	}
	idx := (r.start + r.len) % r.cap
	r.buf[idx] = e
	r.len++
	return evicted
}

// since returns entries with Seq > afterSeq, oldest first. If the requested
// watermark has already fallen out of the buffer, ok is false — the caller
// has no way to recover the gap from this ring and must treat it as data
// loss (fanInNetwork still kills the attach in this case; live-buffer
// overflow without a ring gap does not).
func (r *ring) since(afterSeq uint64) (entries []Entry, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.len == 0 {
		return nil, true
	}
	oldest := r.buf[r.start].Seq
	if afterSeq+1 < oldest && afterSeq != 0 {
		// Requested a watermark older than what's retained (and not "give me
		// everything"). Signal the gap rather than silently truncating.
		return nil, false
	}
	out := make([]Entry, 0, r.len)
	for i := 0; i < r.len; i++ {
		e := r.buf[(r.start+i)%r.cap]
		if e.Seq > afterSeq {
			out = append(out, e)
		}
	}
	return out, true
}

// lastSeq returns the most recently pushed seq, or 0 if the ring is empty.
func (r *ring) lastSeq() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.len == 0 {
		return 0
	}
	return r.buf[(r.start+r.len-1)%r.cap].Seq
}

// droppedCount reports how many entries have been evicted before ever being
// read via since. Exposed for tests and future overflow-policy wiring.
func (r *ring) droppedCount() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dropped
}

// snapshot is a cheap monitoring read: occupancy and recency without
// materializing entries the way since does. Safe to call on a timer.
func (r *ring) snapshot() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := Stats{Occupancy: r.len, Capacity: r.cap, Dropped: r.dropped}
	if r.len > 0 {
		last := r.buf[(r.start+r.len-1)%r.cap]
		st.LastSeq = last.Seq
		st.LastLineTime = last.Time
	}
	return st
}
