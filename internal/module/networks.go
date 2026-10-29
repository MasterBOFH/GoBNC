package module

import (
	"context"
	"encoding/json"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func init() {
	registerMethod(wire.MethodNetworksList, handleNetworksList, wire.FeatureNetworks)
	registerMethod(wire.MethodNetworksConnect, networkControl(Backend.ConnectNetwork), wire.FeatureNetworks)
	registerMethod(wire.MethodNetworksDisconnect, networkControl(Backend.DisconnectNetwork), wire.FeatureNetworks)
	registerMethod(wire.MethodNetworksReconnect, networkControl(Backend.ReconnectNetwork), wire.FeatureNetworks)
}

// networkControl wraps a start/stop/reconnect action by network id: not_ready before the brain
// is ready, not_found for an unknown network.
func networkControl(action func(Backend, int64) error) Handler {
	return func(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
		p, err := decodeParams[wire.NetworkParams](params)
		if err != nil {
			return nil, err
		}
		if !c.srv.backend.Ready() {
			return nil, wire.Errorf(wire.CodeNotReady, "the brain is not ready")
		}
		if !c.srv.networkExists(p.Network) {
			return nil, wire.Errorf(wire.CodeNotFound, "no network %d", p.Network)
		}
		if err := action(c.srv.backend, p.Network); err != nil {
			return nil, wire.Errorf(wire.CodeBadParams, "%v", err)
		}
		return nil, nil
	}
}

func handleNetworksList(ctx context.Context, c *Conn, _ json.RawMessage) (any, error) {
	nets := c.srv.backend.Networks()
	if nets == nil {
		nets = []wire.NetworkInfo{}
	}
	return wire.NetworksListResult{Networks: nets}, nil
}

// NetworkChanged tells every module granted "networks" about a network added, removed,
// reconfigured or changing state.
func (s *Server) NetworkChanged(ev wire.NetworkEvent) {
	s.Broadcast(wire.FeatureNetworks, wire.EventNetwork, ev)
}
