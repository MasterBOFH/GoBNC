package session

import (
	"errors"
	"fmt"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// VirtualClient is a module acting as an IRC client on one network: a Downlink with no socket.
// Its ID routes solicited replies (RequestTracker) and labeled ACKs to the module, which gets
// everything Session sends it as a client_line event.
//
// Paths that skip virtual clients, because the module already has the line as a bus envelope:
// HandleMessage's broadcast loop, fanoutSelfEcho, echoSelfLocally's fan-out, relayCTCPLive,
// broadcastMarkRead and the MARKREAD after our JOIN, and broadcastSelfUMode. Solicited replies,
// ACKs and bouncer notices still reach it. A virtual client survives an uplink disconnect (no
// ERROR, not closed) and is not counted by DownlinkCount.
type VirtualClient struct {
	id      ClientID
	network int64
	client  string
	deliver func(ev any) bool
}

// virtualCapSet holds the capabilities a virtual client always has.
var virtualCapSet = map[string]bool{"message-tags": true, "server-time": true, "echo-message": true,
	"labeled-response": true, "batch": true, "multi-prefix": true, "userhost-in-names": true,
	"account-tag": true, "away-notify": true, "extended-join": true, "chghost": true, "setname": true,
	"invite-notify": true, "cap-notify": true, readMarkerCap: true}

// ErrVirtualClientGone is returned by Send once the module's sink refused a frame.
var ErrVirtualClientGone = errors.New("session: virtual client's module is gone")

// NewVirtualClient makes the client "client" of module on network; deliver queues an event for
// the module without blocking (false = the module is gone).
func NewVirtualClient(module, client string, network int64, deliver func(ev any) bool) *VirtualClient {
	return &VirtualClient{id: ClientID(fmt.Sprintf("mod:%s:%s", module, client)), network: network,
		client: client, deliver: deliver}
}

func (v *VirtualClient) ID() ClientID { return v.id }

func (v *VirtualClient) Caps() map[string]bool {
	out := make(map[string]bool, len(virtualCapSet))
	for c := range virtualCapSet {
		out[c] = true
	}
	return out
}

func (v *VirtualClient) HasCap(name string) bool { return virtualCapSet[name] }

// ClearCap is a no-op: a virtual client's caps are fixed.
func (v *VirtualClient) ClearCap(string) {}

// HasSeenCap is always true so a virtual client is never sent CAP NEW for a cap it "knows".
func (v *VirtualClient) HasSeenCap(string) bool { return true }
func (v *VirtualClient) MarkSeenCap(string)     {}

// Send hands msg to the module as a client_line event.
func (v *VirtualClient) Send(msg irc.Message) error {
	if !v.deliver(wire.ClientLineEvent{Network: v.network, Client: v.client, Raw: msg.Wire()}) {
		return ErrVirtualClientGone
	}
	return nil
}

// Close is a no-op: the module decides when its client goes (DetachVirtual).
func (v *VirtualClient) Close() error { return nil }

func (v *VirtualClient) RemoteAddr() string { return "" }

func isVirtual(d Downlink) bool {
	_, ok := d.(*VirtualClient)
	return ok
}

// AttachVirtual registers vc on this network — without Attach's connect notice and burst; the
// module has the network's state from its snapshot. A client already attached under the same
// ID is detached first, so its pending replies and echoes do not reach vc.
func (s *Session) AttachVirtual(vc *VirtualClient) {
	if s.HasDownlink(vc.ID()) {
		s.Detach(vc.ID())
	}
	s.mu.Lock()
	s.downlinks[vc.ID()] = vc
	s.mu.Unlock()
}

// DetachVirtual removes vc (and its pending solicited requests) if it is still the client
// attached under its ID — not a later client of the same name from another module connection.
func (s *Session) DetachVirtual(vc *VirtualClient) {
	s.mu.RLock()
	cur, ok := s.downlinks[vc.ID()]
	s.mu.RUnlock()
	if ok && cur == Downlink(vc) {
		s.Detach(vc.ID())
	}
}

// HasDownlink reports whether a client (IRC or virtual) with this id is attached.
func (s *Session) HasDownlink(id ClientID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.downlinks[id]
	return ok
}
