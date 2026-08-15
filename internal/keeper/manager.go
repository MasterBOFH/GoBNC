package keeper

import (
	"log/slog"
	"sync"
	"time"
)

// NetworkID identifies one uplink network. It's meant to be the same
// durable key as store.Network.ID elsewhere in the bouncer — declared as an
// alias rather than importing internal/store, so this small, stable
// package doesn't take on a dependency on a larger one just for a type.
type NetworkID = int64

// Manager holds one Keeper per network, in one process.
//
// Shape: one keeper process serves every network, not one keeper process
// per network. There is one brain serving all networks, and a brain
// restart must preserve every uplink simultaneously — with one process
// there's one socket, one handshake, one validate pass covering
// everything, and one thing to supervise. Per-network processes would mean
// the brain attaches to N sockets and validate has to succeed across all of
// them transactionally, for isolation this design doesn't need: each
// Keeper is already fully self-contained (its own ring, seq space, epoch
// counter, socket-death handling), so a failure on one network's uplink
// cannot reach another's state through Manager — there's no shared
// mutable state between Keepers, only the map that looks them up.
type Manager struct {
	maxLine int
	ringCap int
	log     *slog.Logger

	mu      sync.Mutex
	keepers map[NetworkID]*Keeper
}

// NewManager creates an empty Manager. maxLine/ringCapacity are applied to
// every Keeper it creates via EnsureNetwork.
func NewManager(maxLine, ringCapacity int, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	return &Manager{
		maxLine: maxLine,
		ringCap: ringCapacity,
		log:     log,
		keepers: make(map[NetworkID]*Keeper),
	}
}

// EnsureNetwork returns the Keeper for id, creating one (not yet dialed) on
// first use. Idempotent — a network already known to the manager is
// returned as-is.
func (m *Manager) EnsureNetwork(id NetworkID) *Keeper {
	m.mu.Lock()
	defer m.mu.Unlock()
	if k, ok := m.keepers[id]; ok {
		return k
	}
	k := New(m.maxLine, m.ringCap, m.log)
	m.keepers[id] = k
	return k
}

// Network returns the Keeper for id, or nil if the manager has never seen
// it (EnsureNetwork was never called for it).
func (m *Manager) Network(id NetworkID) *Keeper {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.keepers[id]
}

// RemoveNetwork closes id's uplink (if connected) and drops it from the
// manager entirely — for a network deleted from configuration, not merely
// disabled. A disabled network should stay known to the manager (Close its
// Keeper directly, keep the entry) so it still appears in Snapshot/All as
// NotConnected rather than vanishing from the roster.
//
// Uses Retire, not Close: a fan-in goroutine streaming this network to a
// live-attached client has no other signal that this Keeper is gone for
// good rather than mid-reconnect — see Keeper.Retire.
func (m *Manager) RemoveNetwork(id NetworkID) {
	m.mu.Lock()
	k, ok := m.keepers[id]
	if ok {
		delete(m.keepers, id)
	}
	m.mu.Unlock()
	if ok {
		k.Retire()
	}
}

// All returns every network Keeper the manager currently holds, keyed by
// ID. The map is a snapshot copy; the *Keeper values are the live,
// shared instances.
func (m *Manager) All() map[NetworkID]*Keeper {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[NetworkID]*Keeper, len(m.keepers))
	for id, k := range m.keepers {
		out[id] = k
	}
	return out
}

// NetworkStatus is one network's status as reported to an attaching client.
type NetworkStatus struct {
	ID      NetworkID `json:"id"`
	State   State     `json:"state"`
	Epoch   uint64    `json:"epoch"`
	LastSeq uint64    `json:"last_seq"`
	// Blob is this network's current resolved blob-store state, delivered
	// for free at attach time — no extra round trip, same pattern as
	// State/Epoch/LastSeq above. A resuming brain seeds Session's
	// self-nick/ISUPPORT/caps/account/channel state from this directly;
	// it is empty for a network that was never connected, or whose
	// connection has died since (the blob is cleared unconditionally on
	// every NotConnected transition — see Keeper.readLoop).
	Blob []BlobEntry `json:"blob,omitempty"`
}

// Snapshot returns the status of every network currently held, in no
// particular order — this is what an attaching client is told exists.
func (m *Manager) Snapshot() []NetworkStatus {
	return statusOf(m.All())
}

// QuitCloseAll sends QUIT (as "QUIT :"+reason, or bare "QUIT" if reason is
// empty) to every currently held network's uplink and closes it —
// concurrently, each write bounded by perNetworkTimeout, the whole
// operation additionally bounded by overallTimeout so one stuck network
// can't hold up the others or block the caller indefinitely. A network
// with no live connection is silently skipped (Keeper.QuitClose's
// errNotConnected, discarded here — there's nothing to report shutdown
// failures to once the caller is already exiting).
//
// This exists for exactly one caller: a deliberate keeper process
// shutdown. That is the mirror image of the brain-exit rule elsewhere in
// this project (see Driver.QuitNetwork's doc comment) — a brain restart
// must send nothing, because the keeper keeps holding the sockets through
// it, but a keeper shutdown genuinely has no one left to hold them, so
// every server on the other end deserves a real QUIT rather than a
// connection reset that reads as a ping timeout.
func (m *Manager) QuitCloseAll(reason string, perNetworkTimeout, overallTimeout time.Duration) {
	line := "QUIT"
	if reason != "" {
		line += " :" + reason
	}
	all := m.All()
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for _, k := range all {
			wg.Add(1)
			go func(k *Keeper) {
				defer wg.Done()
				_ = k.QuitClose(line, perNetworkTimeout)
			}(k)
		}
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(overallTimeout):
	}
}

// statusOf derives NetworkStatus from an already-fetched All() map, so a
// caller that needs both the map (to subscribe/read from) and the status
// (to report on the wire) can take one consistent snapshot instead of two
// separate locked calls that could observe a network added or removed
// between them.
func statusOf(all map[NetworkID]*Keeper) []NetworkStatus {
	out := make([]NetworkStatus, 0, len(all))
	for id, k := range all {
		st, epoch := k.State()
		out = append(out, NetworkStatus{ID: id, State: st, Epoch: epoch, LastSeq: k.LastSeq(), Blob: k.BlobSnapshot()})
	}
	return out
}
