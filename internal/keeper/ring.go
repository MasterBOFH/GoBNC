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
// Two different "overflow" scenarios exist and only one is wired so far.
//
//  1. A live-attached client falls behind delivery for one network and its
//     per-connection buffer fills (Keeper.SubscribeLines' Overflow signal).
//     This is wired: Listener kills that connection with an explicit error
//     rather than silently dropping (see listener.go's serveLive/
//     fanInNetwork), and per the multi-network design only that one
//     connection dies — every other network's delivery on it, and every
//     other attached brain's networks entirely, are unaffected since each
//     network has its own Keeper, ring, and subscriber set.
//  2. STEP-3/8 OBLIGATION, still not wired: the ring itself evicting (this
//     type's push, below) independent of whether anyone is consuming —
//     e.g. a netsplit burst that outpaces ring capacity with nobody
//     attached to notice. The original single-network design's policy was
//     "kill the process." With multiple networks per keeper process that's
//     wrong: it would take down every other network's uplink for a failure
//     on one. Revised policy: a ring overflow on network N closes N's
//     uplink (Keeper.Close on that network's Keeper only) and clears N's
//     blob store once one exists — loud, logged, one reconnect on one
//     network. This isn't implemented yet because it has no blob store to
//     clear and wiring only the Close half now would be half a policy.
//     since() below already reports ok=false the moment a requested
//     watermark has been evicted — that detection doesn't need to be
//     rebuilt, only acted on.
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

func (r *ring) push(e Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.len == r.cap {
		// Evict oldest.
		r.start = (r.start + 1) % r.cap
		r.len--
		r.dropped++
	}
	idx := (r.start + r.len) % r.cap
	r.buf[idx] = e
	r.len++
}

// since returns entries with Seq > afterSeq, oldest first. If the requested
// watermark has already fallen out of the buffer, ok is false — the caller
// has no way to recover the gap from this ring and must treat it as data
// loss (this is what the overflow-kills-the-process policy exists to avoid
// once a brain is actually attached; see the doc comment on ring).
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
