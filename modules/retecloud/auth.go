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
		c.send(authError(msg.RequestID, "Invalid request data", authInvalidRequest, 0))
		return
	}
	if code, retryAfter := m.checkCredentials(c, req.Password); code != "" {
		c.send(authError(msg.RequestID, "Authentication failed", code, retryAfter))
		return
	}
	if err := m.call(wire.MethodDevicesTouch, wire.DeviceTouchParams{ClientID: req.ClientID}, nil); err != nil {
		m.log.Warn("devices.touch", "err", err)
	}
	sessions, err := m.sessionSummaries(req.ClientID)
	if err != nil {
		c.send(authError(msg.RequestID, "Failed to list sessions", authUnavailable, 0))
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

// checkCredentials checks the password when one was sent, else the connection's client
// certificate (so a phone with an unregistered certificate installed that logs in with the
// password does not use up its address's failed-attempt allowance). It returns "" when the
// credential was accepted, else the authenticate_error code (and retry_after when rate limited).
func (m *Module) checkCredentials(c *wsConn, password string) (code string, retryAfter int) {
	p := wire.AuthCheckParams{Password: password, RemoteIP: c.remoteIP}
	if password == "" {
		if c.certSHA256 == "" {
			return authInvalidCredentials, 0
		}
		p = wire.AuthCheckParams{CertSHA256: c.certSHA256, RemoteIP: c.remoteIP}
	}
	var res wire.AuthCheckResult
	switch err := m.call(wire.MethodAuthCheck, p, &res); {
	case err != nil:
		m.log.Warn("auth.check", "err", err)
		return authUnavailable, 0
	case res.OK:
		return "", 0
	case res.Reason == wire.AuthRateLimited:
		return authRateLimited, max(1, res.RetryAfter)
	}
	return authInvalidCredentials, 0 // "invalid", or a brain that gives no reason
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
