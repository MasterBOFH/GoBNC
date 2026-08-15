package keeper

import "sync"

// BlobMode is how a blobStore entry combines with whatever is already
// stored under its key — see docs/keeper-design.md's blob store section.
type BlobMode string

const (
	// BlobModeAppend accumulates values under key, in push order (e.g. isupport).
	BlobModeAppend BlobMode = "append"
	// BlobModeReplace makes the pushed value the sole, latest-wins value under
	// key (e.g. cloak, self-nick, channel:#foo, caps).
	BlobModeReplace BlobMode = "replace"
	// BlobModeDelete removes key entirely (e.g. a channel:#foo entry on PART).
	BlobModeDelete BlobMode = "delete"
)

// BlobEntry is one key's current resolved value, as delivered to an
// attaching client — the result of applying every push against that key so
// far, not a log of the pushes themselves. Values are opaque to the
// keeper: it matches on Key and applies BlobMode, it never inspects Value.
type BlobEntry struct {
	Key    string   `json:"key"`
	Values [][]byte `json:"values"` // one entry for BlobModeReplace, accumulated in order for BlobModeAppend
}

// blobStore holds one network's derived state, keyed by a brain-chosen
// string. It never restarts with the process it belongs to going away —
// blobStore lives exactly as long as the Keeper instance that owns it, and
// is cleared (never partially) the instant that network's connection dies,
// per the keeper-design.md invariant: state derived from a dead epoch is
// stale the instant the epoch ends, not something worth carrying forward
// "just in case."
type blobStore struct {
	mu      sync.Mutex
	entries map[string][][]byte
	order   []string // insertion order, for a deterministic Snapshot
}

func newBlobStore() *blobStore {
	return &blobStore{entries: make(map[string][][]byte)}
}

// Push applies one derived entry under key according to mode. The keeper
// never interprets value — it only matches on key and mode.
func (b *blobStore) Push(key string, mode BlobMode, value []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch mode {
	case BlobModeAppend:
		if _, ok := b.entries[key]; !ok {
			b.order = append(b.order, key)
		}
		b.entries[key] = append(b.entries[key], value)
	case BlobModeReplace:
		if _, ok := b.entries[key]; !ok {
			b.order = append(b.order, key)
		}
		b.entries[key] = [][]byte{value}
	case BlobModeDelete:
		if _, ok := b.entries[key]; ok {
			delete(b.entries, key)
			for i, k := range b.order {
				if k == key {
					b.order = append(b.order[:i], b.order[i+1:]...)
					break
				}
			}
		}
	}
}

// Snapshot returns every currently-held entry, in the order each key was
// first pushed — this is what an attaching client receives in HelloAckMsg.
func (b *blobStore) Snapshot() []BlobEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]BlobEntry, 0, len(b.order))
	for _, key := range b.order {
		vals := b.entries[key]
		cp := make([][]byte, len(vals))
		copy(cp, vals)
		out = append(out, BlobEntry{Key: key, Values: cp})
	}
	return out
}

// Clear removes every entry — called unconditionally on every NotConnected
// transition (see Keeper.readLoop's wiring point), deliberate or not: a
// fresh Dial always starts a blank transcript.
func (b *blobStore) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries = make(map[string][][]byte)
	b.order = nil
}
