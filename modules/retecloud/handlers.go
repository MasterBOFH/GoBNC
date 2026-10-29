package retecloud

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// The base-protocol messages lite relies on, ported from retecloud-api websocket/handlers.go.

const (
	getMessagesDefaultLimit = 100
	getMessagesMaxLimit     = 500 // the brain's history.messages page limit
)

// syncedNetwork resolves msg's session for a request that needs it synced on this connection.
func (m *Module) syncedNetwork(c *wsConn, msg *ClientMessage, kind string) (*network, *liteSession, bool) {
	if msg.SessionID == "" {
		c.send(errorFrame(kind, msg.RequestID, "session_id is required"))
		return nil, nil, false
	}
	n := m.networkFor(msg.SessionID)
	if n == nil {
		c.send(errorFrame(kind, msg.RequestID, "No access to session"))
		return nil, nil, false
	}
	ls := c.sessions[n.id]
	if ls == nil {
		c.send(errorFrame(kind, msg.RequestID, "sync_session required before "+kind))
		return nil, nil, false
	}
	return n, ls, true
}

func (m *Module) handleGetMessages(c *wsConn, msg *ClientMessage) {
	var req GetMessagesRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		c.send(errorFrame("get_messages", msg.RequestID, "Invalid request data"))
		return
	}
	n := m.networkFor(msg.SessionID)
	if n == nil {
		c.send(errorFrame("get_messages", msg.RequestID, "No access to session"))
		return
	}
	if req.Buffer == nil || strings.TrimSpace(*req.Buffer) == "" {
		c.send(errorFrame("get_messages", msg.RequestID, "buffer is required"))
		return
	}
	limit := getMessagesDefaultLimit
	if req.Limit != nil && *req.Limit > 0 {
		limit = min(*req.Limit, getMessagesMaxLimit)
	}
	p := wire.HistoryMessagesParams{Network: n.id, Buffer: *req.Buffer, Limit: limit}
	page := map[string]interface{}{}
	aroundID := req.AroundID
	if req.Anchor != nil {
		if *req.Anchor != wire.AnchorFirstUnread {
			c.send(errorFrame("get_messages", msg.RequestID, "Unknown anchor"))
			return
		}
		var mark wire.ReadMarkerGetResult
		if err := m.call(wire.MethodReadMarkersGet, wire.ReadMarkerGetParams{Network: n.id, Buffer: *req.Buffer}, &mark); err == nil && mark.ID > 0 {
			first := mark.ID + 1
			aroundID = &first
		} // no cursor: everything is unread, the newest page
	}
	switch {
	case aroundID != nil:
		p.AroundID = aroundID
		page["anchor_id"] = *aroundID
	case req.BeforeID != nil:
		p.BeforeID = req.BeforeID
	case req.AfterID != nil:
		p.AfterID = req.AfterID
	}
	var res wire.HistoryMessagesResult
	if err := m.call(wire.MethodHistoryMessages, p, &res); err != nil {
		c.send(errorFrame("get_messages", msg.RequestID, "Error retrieving messages"))
		return
	}
	page["has_more_before"] = res.HasBefore
	page["has_more_after"] = res.HasAfter
	out := make([]MessageResponse, 0, len(res.Messages))
	for _, hm := range res.Messages {
		out = append(out, messageResponse(hm))
	}
	if len(res.Messages) > 0 {
		page["oldest_id"] = res.Messages[0].ID
		page["newest_id"] = res.Messages[len(res.Messages)-1].ID
	}
	data := map[string]interface{}{"messages": out, "has_more": res.HasBefore}
	for k, v := range page {
		data[k] = v
	}
	c.send(ServerMessage{Type: "get_messages_success", RequestID: msg.RequestID, SessionID: msg.SessionID, Data: data})
}

// handleReadCursor moves the shared read cursor (the brain's read marker, seen by IRC clients
// as MARKREAD); the other followers hear read_cursor.
func (m *Module) handleReadCursor(c *wsConn, msg *ClientMessage) {
	var req ReadCursorRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		c.send(errorFrame("read_cursor", msg.RequestID, "Invalid request data"))
		return
	}
	n, _, ok := m.syncedNetwork(c, msg, "read_cursor")
	if !ok {
		return
	}
	if strings.TrimSpace(req.Buffer) == "" || req.LastMessageID <= 0 {
		c.send(errorFrame("read_cursor", msg.RequestID, "buffer and last_message_id are required"))
		return
	}
	m.noteOwnAction(c, "read_cursor", n.key(req.Buffer))
	if err := m.call(wire.MethodReadMarkersSet, wire.ReadMarkerSetParams{Network: n.id, Buffer: req.Buffer, ID: req.LastMessageID}, nil); err != nil {
		m.ownAction = ownAction{}
		c.send(errorFrame("read_cursor", msg.RequestID, "Failed to update read cursor"))
		return
	}
	c.send(ServerMessage{Type: "read_cursor_success", RequestID: msg.RequestID, SessionID: msg.SessionID})
}

// handleOpenBuffer marks a buffer open on this device (each device has its own open set).
func (m *Module) handleOpenBuffer(c *wsConn, msg *ClientMessage) {
	m.openOrClose(c, msg, "open_buffer", wire.MethodDevicesOpen)
}

// handleCloseBuffer records an intentional close on this device: the buffer stays hidden there
// until a new message reopens it, and nothing before the close counts as unread again.
func (m *Module) handleCloseBuffer(c *wsConn, msg *ClientMessage) {
	m.openOrClose(c, msg, "close_buffer", wire.MethodDevicesClose)
}

func (m *Module) openOrClose(c *wsConn, msg *ClientMessage, kind, method string) {
	var req OpenBufferRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		c.send(errorFrame(kind, msg.RequestID, "Invalid request data"))
		return
	}
	n, _, ok := m.syncedNetwork(c, msg, kind)
	if !ok {
		return
	}
	buffer := strings.TrimSpace(req.Buffer)
	if buffer == "" {
		c.send(errorFrame(kind, msg.RequestID, "buffer is required"))
		return
	}
	m.noteOwnAction(c, kind, n.key(buffer))
	if err := m.call(method, wire.DeviceBufferParams{ClientID: c.deviceID, Network: n.id, Buffer: buffer}, nil); err != nil {
		m.ownAction = ownAction{}
		c.send(errorFrame(kind, msg.RequestID, "Failed to update buffer"))
		return
	}
	c.send(ServerMessage{Type: kind + "_success", RequestID: msg.RequestID, SessionID: msg.SessionID})
}

// handleSend sends a raw IRC command as this connection's virtual client.
func (m *Module) handleSend(c *wsConn, msg *ClientMessage) {
	var req SendRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		c.send(errorFrame("send", msg.RequestID, "Invalid request data"))
		return
	}
	n, ls, ok := m.syncedNetwork(c, msg, "send")
	if !ok {
		return
	}
	ls.notePending(req.Command, n.isChannel) // expect this query's replies before they can come
	err := m.call(wire.MethodSendLine, wire.SendLineParams{Network: n.id, Client: c.clientName, Line: req.Command}, nil)
	var we *wire.Error
	switch {
	case errors.As(err, &we) && we.Code == wire.CodeNotReady, err != nil && !errors.As(err, &we):
		c.send(errorFrame("send", msg.RequestID, "Bouncer is reloading"))
		return
	case err != nil:
		c.send(errorFrame("send", msg.RequestID, "Failed to send command"))
		return
	}
	c.send(ServerMessage{Type: "send_success", RequestID: msg.RequestID, SessionID: msg.SessionID,
		Data: map[string]interface{}{"command": req.Command}})
}

func (m *Module) handleConnectSession(c *wsConn, msg *ClientMessage) {
	m.networkControl(c, msg, "connect_session", wire.MethodNetworksConnect)
}

func (m *Module) handleDisconnectSession(c *wsConn, msg *ClientMessage) {
	m.networkControl(c, msg, "disconnect_session", wire.MethodNetworksDisconnect)
}

func (m *Module) networkControl(c *wsConn, msg *ClientMessage, kind, method string) {
	if msg.SessionID == "" {
		c.send(errorFrame(kind, msg.RequestID, "session_id is required"))
		return
	}
	n := m.networkFor(msg.SessionID)
	if n == nil {
		c.send(errorFrame(kind, msg.RequestID, "No access to session"))
		return
	}
	if err := m.call(method, wire.NetworkParams{Network: n.id}, nil); err != nil {
		text := "Failed to connect session"
		if kind == "disconnect_session" {
			text = "Failed to disconnect session"
		}
		c.send(errorFrame(kind, msg.RequestID, text))
		return
	}
	c.send(ServerMessage{Type: kind + "_success", RequestID: msg.RequestID, SessionID: msg.SessionID})
}

// configUpdate tells a network's followers its configuration changed (GoBNC network settings
// are not sent field by field; the name is the one a client shows).
func (m *Module) configUpdate(n *network) {
	value, _ := json.Marshal(n.name)
	m.broadcastFollowers(n.id, ServerMessage{Type: "config_update", SessionID: sessionID(n.id),
		Data: map[string]interface{}{"updates": []map[string]interface{}{
			{"key": "name", "value": string(value), "updated_at": time.Now().UTC()}}}})
}
