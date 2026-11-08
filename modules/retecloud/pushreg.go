package retecloud

import (
	"crypto/ecdh"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/MasterBOFH/GoBNC/modules/retecloud/push"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Push registrations (WEBSOCKET_PROTOCOL_LITE.md, "push_register / push_unregister (GoBNC)" and
// "GoBNC: storage, following networks, relaying"). A paired phone hands over a relay key minted
// on ReteCloud, the relay's URL, its public key and its profile id; they live in the module's
// key-value store under push/<client_id> and survive restarts and reloads. The module follows
// every network a registered device has toggled on, whether or not a connection is open.

const kvPush = "push/"

// pushRegistration is the stored registration of one device.
type pushRegistration struct {
	RelayURL     string `json:"relay_url"`
	RelayKey     string `json:"relay_key"`
	PublicKey    string `json:"public_key"`
	Profile      string `json:"profile"`
	RegisteredAt string `json:"registered_at"`
	Rejected     bool   `json:"rejected,omitempty"` // the relay refused the key (401 / 410)
}

// pushDevice is a registered device as the loop knows it.
type pushDevice struct {
	reg      pushRegistration
	pub      *ecdh.PublicKey
	networks map[int64]bool // the device's per-network toggles
}

// active reports a device push can reach.
func (d *pushDevice) active() bool { return d != nil && !d.reg.Rejected && d.pub != nil }

// pushState is the module's push bookkeeping. Loop-owned.
type pushState struct {
	devices     map[string]*pushDevice
	readMoves   map[int64]map[string]time.Time // network → origin device ("" = not a lite device) → last move
	clears      map[clearKey]*clearState
	relayPaused map[string]time.Time // relay_url → no requests until then (503)
	coalesce    *push.Coalescer      // one notification per conversation; cool-down and cap per buffer
}

// relayTimeout bounds one relay request.
const relayTimeout = 15 * time.Second

// relayUnavailablePause is how long a 503 stops pushes to that relay.
const relayUnavailablePause = time.Hour

func newPushState() pushState {
	return pushState{devices: map[string]*pushDevice{}, readMoves: map[int64]map[string]time.Time{},
		clears: map[clearKey]*clearState{}, relayPaused: map[string]time.Time{}, coalesce: push.NewCoalescer(nil)}
}

func defaultRelay() *push.Relay {
	return &push.Relay{Client: &http.Client{Timeout: relayTimeout}, Backoff: push.DefaultBackoff}
}

// loadPushDevices reads every registration and its device's toggles (after each brain hello).
func (m *Module) loadPushDevices() {
	var list wire.StorageListResult
	if err := m.call(wire.MethodStorageList, wire.StorageListParams{Prefix: kvPush}, &list); err != nil {
		m.log.Warn("push: list registrations", "err", err)
		return
	}
	devices := map[string]*pushDevice{}
	for _, key := range list.Keys {
		id := strings.TrimPrefix(key, kvPush)
		reg, found, err := m.readPushRegistration(id)
		if err != nil || !found {
			if err != nil {
				m.log.Warn("push: read registration", "client_id", id, "err", err)
				if old := m.push.devices[id]; old != nil {
					devices[id] = old // keep what was loaded before
				}
			}
			continue
		}
		dev := &pushDevice{reg: reg}
		dev.pub, _ = push.ParsePublicKey(reg.PublicKey)
		dev.networks = m.deviceToggles(id)
		devices[id] = dev
	}
	m.push.devices = devices
}

func (m *Module) readPushRegistration(clientID string) (pushRegistration, bool, error) {
	var reg pushRegistration
	v, found, err := m.storageGet(kvPush + clientID)
	if err != nil || !found {
		return reg, false, err
	}
	if err := json.Unmarshal([]byte(v), &reg); err != nil {
		return reg, false, err
	}
	return reg, true, nil
}

func (m *Module) writePushRegistration(clientID string, reg pushRegistration) error {
	v, _ := json.Marshal(reg)
	return m.storageSet(kvPush+clientID, string(v))
}

// deviceToggles reads a device's per-network toggles.
func (m *Module) deviceToggles(clientID string) map[int64]bool {
	out := map[int64]bool{}
	var res wire.DeviceNetworksResult
	if err := m.call(wire.MethodDevicesNetworks, wire.DeviceParams{ClientID: clientID}, &res); err != nil {
		m.log.Warn("push: devices.networks", "client_id", clientID, "err", err)
		return out
	}
	for _, t := range res.Networks {
		if t.Subscribed {
			out[t.Network] = true
		}
	}
	return out
}

// pushFollows reports whether a registered device follows network.
func (m *Module) pushFollows(network int64) bool {
	for _, d := range m.push.devices {
		if d.active() && d.networks[network] {
			return true
		}
	}
	return false
}

// followForPush subscribes to a network a registered device toggled on, when nothing follows
// it on this brain connection yet. A stopped network is followed when the brain restarts it.
func (m *Module) followForPush(id int64) {
	n := m.nets[id]
	if n == nil || n.followed || !m.brainUp {
		return
	}
	if _, err := m.snapshot(id); err != nil && !isNotFound(err) {
		m.log.Warn("push: follow network", "network", id, "err", err)
	}
}

// onDeviceNetwork is a device toggling a network: a registered device's toggle is followed.
func (m *Module) onDeviceNetwork(ev wire.DeviceNetworkEvent) {
	d := m.push.devices[ev.ClientID]
	if d == nil {
		return
	}
	if ev.Subscribed {
		d.networks[ev.Network] = true
		if d.active() {
			m.followForPush(ev.Network)
		}
		return
	}
	delete(d.networks, ev.Network)
}

// PushRegisterRequest is push_register's data.
type PushRegisterRequest struct {
	RelayURL  string `json:"relay_url"`
	RelayKey  string `json:"relay_key"`
	PublicKey string `json:"public_key"`
	Profile   string `json:"profile"`
}

// handlePushRegister stores (replaces) the connection's device registration and follows its
// toggled-on networks. A key the relay refused is answered relay_key_rejected until a new one
// comes.
func (m *Module) handlePushRegister(c *wsConn, msg *ClientMessage) {
	const kind = "push_register"
	fail := func(text, code string) { c.send(codedError(kind, msg.RequestID, "", text, code, nil)) }
	if c.deviceID == "" {
		fail("client_id is required", "invalid_request")
		return
	}
	var req PushRegisterRequest
	if len(msg.Data) > 0 {
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			fail("Invalid request data", "invalid_request")
			return
		}
	}
	if u, err := url.Parse(req.RelayURL); err != nil || u.Scheme != "https" || u.Host == "" {
		fail("relay_url must be https", "invalid_request")
		return
	}
	if req.RelayKey == "" {
		fail("relay_key is required", "invalid_request")
		return
	}
	pub, err := push.ParsePublicKey(req.PublicKey)
	if err != nil {
		fail("public_key must be 32 bytes, base64", "invalid_request")
		return
	}
	if req.Profile == "" {
		fail("profile is required", "invalid_request")
		return
	}
	old, found, err := m.readPushRegistration(c.deviceID)
	if err != nil {
		fail("Storage error", "unavailable")
		return
	}
	if found && old.Rejected && old.RelayKey == req.RelayKey {
		fail("Relay key rejected", "relay_key_rejected")
		return
	}
	reg := pushRegistration{RelayURL: req.RelayURL, RelayKey: req.RelayKey, PublicKey: req.PublicKey,
		Profile: req.Profile, RegisteredAt: m.now().UTC().Format(time.RFC3339)}
	if err := m.writePushRegistration(c.deviceID, reg); err != nil {
		fail("Storage error", "unavailable")
		return
	}
	dev := &pushDevice{reg: reg, pub: pub, networks: m.deviceToggles(c.deviceID)}
	m.push.devices[c.deviceID] = dev
	c.send(ServerMessage{Type: kind + "_success", RequestID: msg.RequestID, Data: map[string]interface{}{}})
	for id := range dev.networks {
		m.followForPush(id)
	}
}

// handlePushUnregister forgets the connection's device registration.
func (m *Module) handlePushUnregister(c *wsConn, msg *ClientMessage) {
	const kind = "push_unregister"
	if c.deviceID == "" {
		c.send(codedError(kind, msg.RequestID, "", "client_id is required", "invalid_request", nil))
		return
	}
	if err := m.call(wire.MethodStorageDelete, wire.StorageKeyParams{Key: kvPush + c.deviceID}, nil); err != nil {
		c.send(codedError(kind, msg.RequestID, "", "Storage error", "unavailable", nil))
		return
	}
	m.forgetPushDevice(c.deviceID)
	c.send(ServerMessage{Type: kind + "_success", RequestID: msg.RequestID, Data: map[string]interface{}{}})
}

func (m *Module) forgetPushDevice(id string) {
	delete(m.push.devices, id)
	m.push.coalesce.Forget(id)
	for k, st := range m.push.clears {
		if k.device == id {
			st.stop()
			delete(m.push.clears, k)
		}
	}
}

// PushPresenceRequest is push_presence's data.
type PushPresenceRequest struct {
	State string `json:"state"`
}

// handlePushPresence records whether the app is on screen on this connection.
func (m *Module) handlePushPresence(c *wsConn, msg *ClientMessage) {
	const kind = "push_presence"
	if c.deviceID == "" {
		c.send(codedError(kind, msg.RequestID, "", "client_id is required", "invalid_request", nil))
		return
	}
	var req PushPresenceRequest
	_ = json.Unmarshal(msg.Data, &req)
	if req.State != "foreground" && req.State != "background" {
		c.send(codedError(kind, msg.RequestID, "", "state must be foreground or background", "invalid_request", nil))
		return
	}
	c.background = req.State == "background"
	c.send(ServerMessage{Type: kind + "_success", RequestID: msg.RequestID, Data: map[string]interface{}{"state": req.State}})
}

// deviceForeground reports an open connection of the device that is in the foreground (from
// authenticate_success until it says background).
func (m *Module) deviceForeground(id string) bool {
	for c := range m.conns {
		if c.authed && c.deviceID == id && !c.background {
			return true
		}
	}
	return false
}
