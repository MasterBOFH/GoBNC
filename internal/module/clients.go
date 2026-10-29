package module

import (
	"context"
	"encoding/json"
	"regexp"

	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func init() {
	registerMethod(wire.MethodClientsAttach, handleClientsAttach, wire.FeatureClients)
	registerMethod(wire.MethodClientsDetach, handleClientsDetach, wire.FeatureClients)
}

var clientNameRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

type clientKey struct {
	network int64
	client  string
}

// handleClientsAttach registers a virtual client of this module on a network. Re-attaching a
// name replaces the earlier client.
func handleClientsAttach(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.ClientParams](params)
	if err != nil {
		return nil, err
	}
	if !clientNameRE.MatchString(p.Client) {
		return nil, wire.Errorf(wire.CodeBadParams, "client must be 1-64 of [A-Za-z0-9._-]")
	}
	sess, err := c.srv.session(p.Network)
	if err != nil {
		return nil, err
	}
	vc := session.NewVirtualClient(c.Module(), p.Client, p.Network, connSink{c}.Deliver)
	sess.AttachVirtual(vc)
	c.mu.Lock()
	first := c.clients == nil
	if first {
		c.clients = map[clientKey]*session.VirtualClient{}
	}
	c.clients[clientKey{p.Network, p.Client}] = vc
	c.mu.Unlock()
	if first {
		c.OnClose(c.detachAll)
	}
	return wire.ClientsAttachResult{ClientID: string(vc.ID())}, nil
}

func handleClientsDetach(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.ClientParams](params)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	vc := c.clients[clientKey{p.Network, p.Client}]
	delete(c.clients, clientKey{p.Network, p.Client})
	c.mu.Unlock()
	if vc == nil {
		return nil, wire.Errorf(wire.CodeNotFound, "no client %q on network %d", p.Client, p.Network)
	}
	if sess, ok := c.srv.backend.Session(p.Network); ok && sess != nil {
		sess.DetachVirtual(vc)
	}
	return nil, nil
}

// virtualClient returns the connection's virtual client on a network.
func (c *Conn) virtualClient(network int64, client string) *session.VirtualClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clients[clientKey{network, client}]
}

// detachAll removes every virtual client the connection attached.
func (c *Conn) detachAll() {
	c.mu.Lock()
	clients := c.clients
	c.clients = nil
	c.mu.Unlock()
	for k, vc := range clients {
		if sess, ok := c.srv.backend.Session(k.network); ok && sess != nil {
			sess.DetachVirtual(vc)
		}
	}
}
