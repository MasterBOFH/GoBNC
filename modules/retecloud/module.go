package retecloud

import (
	"context"
	"log/slog"

	"github.com/gorilla/websocket"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Features are the module API features gobnc-retecloud uses (grant them in gobnc.json).
var Features = []string{wire.FeatureNetworks, wire.FeatureEvents, wire.FeatureHistory, wire.FeatureReadMarkers,
	wire.FeatureClients, wire.FeatureSend, wire.FeatureDevices, wire.FeatureAuth, wire.FeatureStorage}

// Brain is what the module needs from its brain connection (*moduleapi.Client).
type Brain interface {
	Call(ctx context.Context, method string, params, result any) error
	Events() <-chan wire.Event
	Run(ctx context.Context, onConnect func(ctx context.Context, c *moduleapi.Client) error) error
}

// Module is gobnc-retecloud's core: WebSocket connections on one side, the brain on the other.
type Module struct {
	cfg   Config
	log   *slog.Logger
	brain Brain
}

// New builds the module over a connected brain client.
func New(cfg Config, brain Brain, log *slog.Logger) *Module {
	if log == nil {
		log = slog.Default()
	}
	return &Module{cfg: cfg, log: log, brain: brain}
}

// Run serves until ctx ends.
func (m *Module) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Accept serves one WebSocket (the Listener's callback).
func (m *Module) Accept(ws *websocket.Conn, certSHA256, remoteIP string) {
	ws.Close()
}
