package retecloud

import (
	"sort"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// badgeTTL bounds how long a device's counted network is reused: a new line or a read move on
// that network ends it at once (badgeChanged); this bounds what they do not see (a Highlight
// switch, a buffer closed).
var badgeTTL = 10 * time.Second

type badgeKey struct {
	device  string
	network int64
}

type badgeEntry struct {
	count int
	at    time.Time
}

// badgeChanged drops every device's count of network: a line arrived or a read cursor moved.
func (m *Module) badgeChanged(network int64) {
	for k := range m.push.badges {
		if k.network == network {
			delete(m.push.badges, k)
		}
	}
}

// deviceBadge is this bouncer's count for a device's app icon (Rete A3353; PUSH_ENCRYPTION.md,
// "Plaintext", badge): the unread highlights and private messages over the networks the device
// is pushed for, counted as the lite bootstrap counts them (history.activity for the buffers
// the device shows, after the read markers and its closes; adjustActivity for the Highlight
// switches and the highlight rules). Every unread line of a query counts. Each network's count
// is kept per device until that network changes or badgeTTL passes, so a push on one network
// recounts only that one. nil when a network could not be counted: the phone keeps the count
// it had.
func (m *Module) deviceBadge(deviceID string, d *pushDevice) *int {
	ids := make([]int64, 0, len(d.networks))
	for id, on := range d.networks {
		if on {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if m.push.badges == nil {
		m.push.badges = map[badgeKey]badgeEntry{}
	}
	now := m.now()
	total := 0
	for _, id := range ids {
		n := m.nets[id]
		if n == nil {
			m.badgeNetworkGone(id, deviceID)
			continue // no longer a network of this bouncer
		}
		k := badgeKey{deviceID, id}
		if e, ok := m.push.badges[k]; ok && now.Sub(e.at) < badgeTTL {
			total += e.count
			continue
		}
		var act wire.HistoryActivityResult
		if err := m.call(wire.MethodHistoryActivity, wire.HistoryActivityParams{Network: id, Device: deviceID}, &act); err != nil {
			m.log.Warn("push: badge", "network", id, "client_id", deviceID, "err", err)
			return nil
		}
		m.adjustActivity(n, deviceID, &act, nil)
		count := 0
		for _, a := range act.Buffers {
			count += a.Highlights
		}
		m.push.badges[k] = badgeEntry{count: count, at: now}
		total += count
	}
	return &total
}

// badgeNetworkGone logs, once per network, a device toggle naming a network the bouncer no
// longer has.
func (m *Module) badgeNetworkGone(network int64, deviceID string) {
	if m.push.badgeGone == nil {
		m.push.badgeGone = map[int64]bool{}
	}
	if m.push.badgeGone[network] {
		return
	}
	m.push.badgeGone[network] = true
	m.log.Info("push: badge skips a network the bouncer no longer has", "network", network, "client_id", deviceID)
}
