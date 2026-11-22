package retecloud

import (
	"sort"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// deviceBadge is this bouncer's count for a device's app icon (Rete A3353; PUSH_ENCRYPTION.md,
// "Plaintext", badge): the unread highlights and private messages over the networks the device
// is pushed for, counted as the lite bootstrap counts them (history.activity for the buffers
// the device shows, after the read markers and its closes; adjustActivity for the Highlight
// switches and the highlight rules). Every unread line of a query counts. nil when a network
// could not be counted: the phone keeps the count it had.
func (m *Module) deviceBadge(deviceID string, d *pushDevice) *int {
	ids := make([]int64, 0, len(d.networks))
	for id, on := range d.networks {
		if on {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	total := 0
	for _, id := range ids {
		n := m.nets[id]
		if n == nil {
			continue // no longer a network of this bouncer
		}
		var act wire.HistoryActivityResult
		if err := m.call(wire.MethodHistoryActivity, wire.HistoryActivityParams{Network: id, Device: deviceID}, &act); err != nil {
			m.log.Warn("push: badge", "network", id, "client_id", deviceID, "err", err)
			return nil
		}
		m.adjustActivity(n, deviceID, &act, nil)
		for _, a := range act.Buffers {
			total += a.Highlights
		}
	}
	return &total
}
