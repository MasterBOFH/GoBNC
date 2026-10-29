package module

import (
	"context"
	"encoding/json"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func init() {
	registerMethod(wire.MethodSendLine, handleSendLine, wire.FeatureSend, wire.FeatureClients)
}

// handleSendLine sends an IRC line as one of the module's virtual clients, exactly as if an IRC
// client had typed it (Session.HandleClientMessage). label, when given, tags the line so its
// echo (line event) and any labeled reply carry it.
func handleSendLine(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.SendLineParams](params)
	if err != nil {
		return nil, err
	}
	sess, err := c.srv.session(p.Network)
	if err != nil {
		return nil, err
	}
	vc := c.virtualClient(p.Network, p.Client)
	if vc == nil {
		return nil, wire.Errorf(wire.CodeNotFound, "no client %q on network %d (clients.attach first)", p.Client, p.Network)
	}
	if !c.srv.backend.Ready() || !sess.Registered() {
		return nil, wire.Errorf(wire.CodeNotReady, "network %d is not ready", p.Network)
	}
	msg, err := irc.Parse(p.Line)
	if err != nil || msg.Command == "" {
		return nil, wire.Errorf(wire.CodeBadParams, "unparsable line")
	}
	if p.Label != "" {
		msg.Tags = msg.CopyTags()
		if msg.Tags == nil {
			msg.Tags = map[string]string{}
		}
		msg.Tags["label"] = p.Label
		msg.Raw = ""
	}
	if err := sess.HandleClientMessage(vc, msg); err != nil {
		return nil, wire.Errorf(wire.CodeNotReady, "%v", err)
	}
	return nil, nil
}
