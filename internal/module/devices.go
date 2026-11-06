package module

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func init() {
	for method, h := range map[string]Handler{
		wire.MethodDevicesTouch:        handleDevicesTouch,
		wire.MethodDevicesNetworks:     handleDevicesNetworks,
		wire.MethodDevicesSetNetwork:   handleDevicesSetNetwork,
		wire.MethodDevicesBuffers:      handleDevicesBuffers,
		wire.MethodDevicesOpen:         handleDevicesOpen,
		wire.MethodDevicesClose:        handleDevicesClose,
		wire.MethodDevicesSeed:         handleDevicesSeed,
		wire.MethodDevicesClearRenames: handleDevicesClearRenames,
	} {
		registerMethod(method, h, wire.FeatureDevices)
	}
}

func needClientID(id string) error {
	if strings.TrimSpace(id) == "" || len(id) > 255 {
		return wire.Errorf(wire.CodeBadParams, "client_id is required (at most 255 bytes)")
	}
	return nil
}

func handleDevicesTouch(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.DeviceTouchParams](params)
	if err != nil {
		return nil, err
	}
	if err := needClientID(p.ClientID); err != nil {
		return nil, err
	}
	created, err := c.srv.backend.Store().TouchDevice(ctx, p.ClientID, p.Name)
	return wire.DeviceTouchResult{Created: created}, err
}

// handleDevicesNetworks lists every network with the device's toggle (false when never set).
func handleDevicesNetworks(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.DeviceParams](params)
	if err != nil {
		return nil, err
	}
	if err := needClientID(p.ClientID); err != nil {
		return nil, err
	}
	subs, err := c.srv.backend.Store().ListDeviceNetworks(ctx, p.ClientID)
	if err != nil {
		return nil, err
	}
	res := wire.DeviceNetworksResult{Networks: []wire.DeviceNetwork{}}
	for _, n := range c.srv.backend.Networks() {
		res.Networks = append(res.Networks, wire.DeviceNetwork{Network: n.ID, Subscribed: subs[n.ID]})
	}
	return res, nil
}

// handleDevicesSetNetwork stores the device's toggle and tells every module granted devices.
func handleDevicesSetNetwork(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.DeviceSetNetworkParams](params)
	if err != nil {
		return nil, err
	}
	if err := needClientID(p.ClientID); err != nil {
		return nil, err
	}
	if !c.srv.networkExists(p.Network) {
		return nil, wire.Errorf(wire.CodeNotFound, "no network %d", p.Network)
	}
	if err := c.srv.backend.Store().SetDeviceNetwork(ctx, p.ClientID, p.Network, p.Subscribed); err != nil {
		return nil, err
	}
	c.srv.Broadcast(wire.FeatureDevices, wire.EventDeviceNetwork,
		wire.DeviceNetworkEvent{ClientID: p.ClientID, Network: p.Network, Subscribed: p.Subscribed})
	return nil, nil
}

func (s *Server) networkExists(id int64) bool {
	for _, n := range s.backend.Networks() {
		if n.ID == id {
			return true
		}
	}
	return false
}

func handleDevicesBuffers(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.DeviceNetworkParams](params)
	if err != nil {
		return nil, err
	}
	if err := needClientID(p.ClientID); err != nil {
		return nil, err
	}
	rows, err := c.srv.backend.Store().ListOpenBuffers(ctx, p.ClientID, p.Network)
	if err != nil {
		return nil, err
	}
	res := wire.DeviceBuffersResult{Buffers: []wire.DeviceBuffer{}}
	for _, b := range rows {
		res.Buffers = append(res.Buffers, wire.DeviceBuffer{Buffer: b.Display, Key: b.Target, Open: b.Open,
			ClosedAtID: b.ClosedAtID, RenamedFrom: b.RenamedFrom})
	}
	return res, nil
}

// handleDevicesClearRenames forgets the renamed_from a sync reported, row by row; a row renamed
// again since (another renamed_from) keeps its own.
func handleDevicesClearRenames(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.DeviceClearRenamesParams](params)
	if err != nil {
		return nil, err
	}
	if err := needClientID(p.ClientID); err != nil {
		return nil, err
	}
	for _, b := range p.Buffers {
		if b.RenamedFrom == "" {
			continue
		}
		if err := c.srv.backend.Store().ClearRenamedFrom(ctx, p.ClientID, p.Network, b.Key, b.RenamedFrom); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func handleDevicesOpen(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.DeviceBufferParams](params)
	if err != nil {
		return nil, err
	}
	if err := needClientID(p.ClientID); err != nil {
		return nil, err
	}
	if p.Buffer == "" {
		return nil, wire.Errorf(wire.CodeBadParams, "buffer is required")
	}
	sess, err := c.srv.session(p.Network)
	if err != nil {
		return nil, err
	}
	return nil, sess.OpenBufferForDevice(p.ClientID, p.Buffer)
}

func handleDevicesClose(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.DeviceBufferParams](params)
	if err != nil {
		return nil, err
	}
	if err := needClientID(p.ClientID); err != nil {
		return nil, err
	}
	if p.Buffer == "" {
		return nil, wire.Errorf(wire.CodeBadParams, "buffer is required")
	}
	sess, err := c.srv.session(p.Network)
	if err != nil {
		return nil, err
	}
	closedAt, err := sess.CloseBufferForDevice(p.ClientID, p.Buffer)
	return wire.DeviceCloseResult{ClosedAtID: closedAt}, err
}

func handleDevicesSeed(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.DeviceNetworkParams](params)
	if err != nil {
		return nil, err
	}
	if err := needClientID(p.ClientID); err != nil {
		return nil, err
	}
	sess, err := c.srv.session(p.Network)
	if err != nil {
		return nil, err
	}
	seeded, err := sess.SeedDevice(p.ClientID)
	if seeded == nil {
		seeded = []string{}
	}
	return wire.DeviceSeedResult{Seeded: seeded}, err
}
