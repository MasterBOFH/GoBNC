package retecloud

import (
	"encoding/json"
	"strings"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// handleAuthenticate is GoBNC's authenticate: a registered TLS client certificate, else the
// bouncer password (both checked by the brain, auth.check). On success the device is touched
// and authenticate_success lists every network with this device's toggle.
func (m *Module) handleAuthenticate(c *wsConn, msg *ClientMessage) {
	var req AuthenticateRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil || strings.TrimSpace(req.ClientID) == "" {
		c.send(errorFrame("authenticate", msg.RequestID, "Invalid request data"))
		return
	}
	if !m.checkCredentials(c, req.Password) {
		c.send(errorFrame("authenticate", msg.RequestID, "Authentication failed"))
		return
	}
	if err := m.call(wire.MethodDevicesTouch, wire.DeviceTouchParams{ClientID: req.ClientID}, nil); err != nil {
		m.log.Warn("devices.touch", "err", err)
	}
	sessions, err := m.sessionSummaries(req.ClientID)
	if err != nil {
		c.send(errorFrame("authenticate", msg.RequestID, "Failed to list sessions"))
		return
	}
	c.authed = true
	c.deviceID = req.ClientID
	resp := AuthenticateResponse{UserID: 1, ClientID: req.ClientID, AssignedIPs: []struct{}{}, Sessions: sessions}
	start := req.Compression == compressionDeflateStream && m.cfg.CompressionLevel >= 0 && !c.compress
	if start {
		resp.Compression = compressionDeflateStream
		c.compress = true
	}
	c.queue(outFrame{msg: ServerMessage{Type: "authenticate_success", RequestID: msg.RequestID, Data: resp},
		startCompression: start})
}

// checkCredentials tries the connection's client certificate, then the password.
func (m *Module) checkCredentials(c *wsConn, password string) bool {
	try := func(p wire.AuthCheckParams) bool {
		p.RemoteIP = c.remoteIP
		var res wire.AuthCheckResult
		return m.call(wire.MethodAuthCheck, p, &res) == nil && res.OK
	}
	if c.certSHA256 != "" && try(wire.AuthCheckParams{CertSHA256: c.certSHA256}) {
		return true
	}
	return password != "" && try(wire.AuthCheckParams{Password: password})
}

// sessionSummaries lists every network with the device's toggle.
func (m *Module) sessionSummaries(deviceID string) ([]SessionSummary, error) {
	var nets wire.NetworksListResult
	if err := m.call(wire.MethodNetworksList, wire.Empty{}, &nets); err != nil {
		return nil, err
	}
	var toggles wire.DeviceNetworksResult
	if err := m.call(wire.MethodDevicesNetworks, wire.DeviceParams{ClientID: deviceID}, &toggles); err != nil {
		return nil, err
	}
	on := map[int64]bool{}
	for _, t := range toggles.Networks {
		on[t.Network] = t.Subscribed
	}
	out := make([]SessionSummary, 0, len(nets.Networks))
	for _, n := range nets.Networks {
		out = append(out, SessionSummary{SessionID: sessionID(n.ID), Name: n.Name, State: sessionState(n.State),
			Subscribed: on[n.ID]})
	}
	return out, nil
}

// sessionState maps a GoBNC network state to the lite protocol's.
func sessionState(s string) string {
	switch s {
	case wire.NetRegistered:
		return "connected"
	case wire.NetConnecting:
		return "connecting"
	}
	return "disconnected"
}
