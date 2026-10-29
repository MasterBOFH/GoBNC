package module

import (
	"context"
	"encoding/json"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func init() {
	registerMethod(wire.MethodNetworksList, handleNetworksList, wire.FeatureNetworks)
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
