package retecloud

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Frames, ported from retecloud-api websocket/protocol.go: the JSON shapes Rete expects.

// ClientMessage is a frame from the client.
type ClientMessage struct {
	Type      string          `json:"type"`
	RequestID string          `json:"request_id,omitempty"`
	SessionID string          `json:"session_id,omitempty"`
	Data      json.RawMessage `json:"data"`
}

// ServerMessage is a frame to the client.
type ServerMessage struct {
	Type       string      `json:"type"`
	RequestID  string      `json:"request_id,omitempty"`
	SessionID  string      `json:"session_id,omitempty"`
	Data       interface{} `json:"data,omitempty"`
	Error      string      `json:"error,omitempty"`
	Code       string      `json:"code,omitempty"`        // stable error code (authenticate_error)
	RetryAfter int         `json:"retry_after,omitempty"` // seconds, with code rate_limited
}

// AuthenticateRequest is GoBNC's authenticate: the bouncer password, or (without one) the TLS
// client certificate; client_id is the device id later sent in sync_session.
type AuthenticateRequest struct {
	Password    string `json:"password,omitempty"`
	ClientID    string `json:"client_id"`
	Compression string `json:"compression,omitempty"`
}

// AuthenticateResponse is authenticate_success's data.
type AuthenticateResponse struct {
	UserID      int              `json:"user_id,omitempty"`
	ClientID    string           `json:"client_id,omitempty"`
	AssignedIPs []struct{}       `json:"assigned_ips"`
	Compression string           `json:"compression,omitempty"`
	Sessions    []SessionSummary `json:"sessions,omitempty"`
}

// SessionSummary is one network in authenticate_success.
type SessionSummary struct {
	SessionID  string `json:"session_id"`
	Name       string `json:"name"`
	State      string `json:"state"`
	Subscribed bool   `json:"subscribed"`
}

// SyncSessionRequest is sync_session's data (lite fields only).
type SyncSessionRequest struct {
	ClientID       string                   `json:"client_id"`
	LastConfigTime *string                  `json:"last_config_time,omitempty"`
	SyncMode       string                   `json:"sync_mode,omitempty"`
	Subscribe      []SubscribeBufferRequest `json:"subscribe,omitempty"`
	Cache          map[string]string        `json:"cache,omitempty"`
}

// UnsyncSessionRequest is unsync_session's data.
type UnsyncSessionRequest struct {
	ClientID string `json:"client_id"`
}

// SubscribeBufferRequest is subscribe_buffer's data (and an inline sync_session subscribe).
type SubscribeBufferRequest struct {
	Buffer  string        `json:"buffer"`
	History *int          `json:"history,omitempty"`
	Members MembersOption `json:"members,omitempty"`
	AfterID *int64        `json:"after_id,omitempty"`
	Anchor  string        `json:"anchor,omitempty"`
	// AnchorID opens the buffer on one stored row (a tapped push).
	AnchorID *int64 `json:"anchor_id,omitempty"`
}

type membersMode int

const (
	membersFull membersMode = iota
	membersNone
)

// MembersOption is subscribe_buffer's "members": true/"full" or false/"none". The former
// "compact" form is answered like "full".
type MembersOption struct{ mode membersMode }

func (m *MembersOption) UnmarshalJSON(b []byte) error {
	var asBool bool
	if json.Unmarshal(b, &asBool) == nil {
		if !asBool {
			m.mode = membersNone
		}
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	switch s {
	case "", "full", "compact":
		m.mode = membersFull
	case "none":
		m.mode = membersNone
	default:
		return fmt.Errorf("members must be true, false, \"full\" or \"none\"")
	}
	return nil
}

// GetMessagesRequest is get_messages' data.
type GetMessagesRequest struct {
	Buffer   *string `json:"buffer,omitempty"`
	BeforeID *int64  `json:"before_id,omitempty"`
	AfterID  *int64  `json:"after_id,omitempty"`
	Limit    *int    `json:"limit,omitempty"`
	AroundID *int64  `json:"around_id,omitempty"`
	Anchor   *string `json:"anchor,omitempty"`
}

// SendRequest is send's data.
type SendRequest struct {
	Command string `json:"command"`
}

// ReadCursorRequest is read_cursor's data.
type ReadCursorRequest struct {
	Buffer        string `json:"buffer"`
	LastMessageID int64  `json:"last_message_id"`
}

// OpenBufferRequest is open_buffer / close_buffer's data.
type OpenBufferRequest struct {
	Buffer string `json:"buffer"`
}

// MessageResponse is one history line (subscribe history, get_messages).
type MessageResponse struct {
	ID        int64           `json:"id"`
	Timestamp time.Time       `json:"timestamp"`
	Event     string          `json:"event"`
	EventData json.RawMessage `json:"event_data"`
}

// storedLine is a history line's event_data, as retecloud-api stores raw lines.
type storedLine struct {
	Raw        string `json:"raw"`
	BufferName string `json:"buffer_name"`
}

// LiteBuffer is one entry of the lite bootstrap's buffer list.
type LiteBuffer struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	Joined     bool   `json:"joined,omitempty"`
	Unread     int    `json:"unread,omitempty"`
	Highlights int    `json:"highlights,omitempty"`
	ReadID     int64  `json:"read_id,omitempty"`
	// RenamedFrom: the peer changed nick since the device's previous bootstrap; the old name.
	RenamedFrom string `json:"renamed_from,omitempty"`
}

type liteActivityEntry struct {
	Buffer     string `json:"buffer"`
	Unread     int    `json:"unread"`
	Highlights int    `json:"highlights"`
	LastID     int64  `json:"last_id,omitempty"`
}

// Frame types.
const (
	frameIRCLine           = "irc_line"
	frameActivity          = "activity"
	frameBufferState       = "buffer_state"
	frameSilence           = "silence"
	frameSubscribeBuffer   = "subscribe_buffer"
	frameUnsubscribeBuffer = "unsubscribe_buffer"
)

// Session ids are "net-<GoBNC network id>".
func sessionID(network int64) string { return "net-" + strconv.FormatInt(network, 10) }

func parseSessionID(s string) (int64, bool) {
	if !strings.HasPrefix(s, "net-") {
		return 0, false
	}
	id, err := strconv.ParseInt(s[len("net-"):], 10, 64)
	return id, err == nil && id > 0
}

func errorFrame(msgType, requestID, text string) ServerMessage {
	return ServerMessage{Type: msgType + "_error", RequestID: requestID, Error: text}
}

// Rete A869: a session the account cannot reach (deleted, or not this account's) answers every
// per-session request with this text and code, and the request's session_id. A client stops
// reconnecting to the session on it, so it decides by the code, never the wording.
const (
	errNoAccessToSession = "No access to session"
	codeNoAccess         = "no_access"
)

// noAccessFrame answers a per-session request for a session the account cannot reach.
func noAccessFrame(msgType, requestID, sessionID string) ServerMessage {
	f := errorFrame(msgType, requestID, errNoAccessToSession)
	f.SessionID = sessionID
	f.Code = codeNoAccess
	return f
}

// authenticate_error codes.
const (
	authInvalidRequest     = "invalid_request"     // malformed data or no client_id
	authInvalidCredentials = "invalid_credentials" // refused by the brain, or no credential at all
	authRateLimited        = "rate_limited"        // the address is locked out; retry_after says how long
	authUnavailable        = "unavailable"         // a brain call failed (unreachable, not ready, timeout)
)

// authError is an authenticate_error with its code (and retry_after, when rate limited).
func authError(requestID, text, code string, retryAfter int) ServerMessage {
	f := errorFrame("authenticate", requestID, text)
	f.Code, f.RetryAfter = code, retryAfter
	return f
}
