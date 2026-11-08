// Package wire is the module API's wire format: newline-delimited JSON frames on the brain's
// module socket. See docs/module-api-design.md for the protocol.
//
// Three frame kinds share the socket:
//
//	request  (module → brain)  {"id": 7, "method": "events.subscribe", "params": {...}}
//	response (brain → module)  {"id": 7, "result": {...}} or {"id": 7, "error": {"code", "message"}}
//	event    (brain → module)  {"event": "line", "data": {...}}
package wire

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// APIVersion is the protocol version a module names in hello.
const APIVersion = 1

// MaxFrameSize is the longest frame (excluding its newline) either side accepts.
const MaxFrameSize = 1 << 20

// ErrFrameTooLarge is returned by ReadFrame for a line longer than MaxFrameSize. The stream is
// not resynchronised: the caller closes the connection.
var ErrFrameTooLarge = errors.New("wire: frame larger than 1 MiB")

// Request is a module → brain call.
type Request struct {
	ID     uint64          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

// Response answers the Request with the same ID: exactly one of Result and Error is set.
type Response struct {
	ID     uint64          `json:"id"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *Error          `json:"error,omitempty"`
}

// Event is an unsolicited brain → module frame.
type Event struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data,omitempty"`
}

// Error is a response's error. It implements error so handlers and clients can return it as-is.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return e.Code + ": " + e.Message
}

// Errorf builds an *Error with a formatted message.
func Errorf(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// FrameKind tells which field of a Frame is set.
type FrameKind int

const (
	KindRequest FrameKind = iota + 1
	KindResponse
	KindEvent
)

// Frame is one decoded line: exactly the field named by Kind is non-nil.
type Frame struct {
	Kind     FrameKind
	Request  *Request
	Response *Response
	Event    *Event
}

// ReadFrame reads and classifies one frame: "method" makes a request, "event" an event, and
// anything else with an "id" a response. Blank lines are skipped.
func ReadFrame(r *bufio.Reader) (Frame, error) {
	for {
		line, err := readLine(r)
		if err != nil {
			return Frame{}, err
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		return decodeFrame(line)
	}
}

func readLine(r *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		chunk, err := r.ReadSlice('\n')
		buf = append(buf, chunk...)
		switch {
		case err == nil:
			line := buf[:len(buf)-1]
			if len(line) > MaxFrameSize {
				return nil, ErrFrameTooLarge
			}
			return line, nil
		case errors.Is(err, bufio.ErrBufferFull):
			if len(buf) > MaxFrameSize {
				return nil, ErrFrameTooLarge
			}
		case errors.Is(err, io.EOF) && len(buf) > 0:
			return nil, io.ErrUnexpectedEOF
		default:
			return nil, err
		}
	}
}

func decodeFrame(line []byte) (Frame, error) {
	var probe struct {
		ID     *uint64 `json:"id"`
		Method *string `json:"method"`
		Event  *string `json:"event"`
	}
	if err := json.Unmarshal(line, &probe); err != nil {
		return Frame{}, fmt.Errorf("wire: bad frame: %w", err)
	}
	switch {
	case probe.Method != nil:
		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			return Frame{}, fmt.Errorf("wire: bad request: %w", err)
		}
		return Frame{Kind: KindRequest, Request: &req}, nil
	case probe.Event != nil:
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return Frame{}, fmt.Errorf("wire: bad event: %w", err)
		}
		return Frame{Kind: KindEvent, Event: &ev}, nil
	case probe.ID != nil:
		var resp Response
		if err := json.Unmarshal(line, &resp); err != nil {
			return Frame{}, fmt.Errorf("wire: bad response: %w", err)
		}
		return Frame{Kind: KindResponse, Response: &resp}, nil
	}
	return Frame{}, errors.New("wire: frame is neither request, response nor event")
}

// WriteFrame writes v as one JSON line.
func WriteFrame(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > MaxFrameSize {
		return ErrFrameTooLarge
	}
	b = append(b, '\n')
	_, err = w.Write(b)
	return err
}

// NewEvent marshals data into an Event frame.
func NewEvent(name string, data any) (Event, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return Event{}, err
	}
	return Event{Event: name, Data: b}, nil
}

// Error codes.
const (
	CodeHelloRequired  = "hello_required"
	CodeUnsupportedAPI = "unsupported_api"
	CodeNotGranted     = "not_granted"
	CodeUnknownMethod  = "unknown_method"
	CodeBadParams      = "bad_params"
	CodeNotFound       = "not_found"
	CodeNotReady       = "not_ready"
	CodeOverflow       = "overflow"
	CodeBadFrame       = "bad_frame"
	CodeInternal       = "internal"
)

// Features a module requests in hello and a config grants per module.
const (
	FeatureNetworks    = "networks"
	FeatureEvents      = "events"
	FeatureHistory     = "history"
	FeatureReadMarkers = "read_markers"
	FeatureClients     = "clients"
	FeatureSend        = "send"
	FeatureDevices     = "devices"
	FeatureAuth        = "auth"
	FeatureStorage     = "storage"
	FeatureCommands    = "commands"

	// State features have no methods: granting one to a configured module makes the brain keep,
	// or ask the server for, state IRC clients poll for themselves (off for IRC-only users).
	// member_meta: userhost-in-names and GoBNC's own WHOX polls (member hosts, accounts, flags);
	// silence: the SILENCE list query after registration; channel_meta: channel modes and
	// topics re-asked after a brain reload.
	FeatureMemberMeta  = "member_meta"
	FeatureSilence     = "silence"
	FeatureChannelMeta = "channel_meta"
)

// Methods.
const (
	MethodHello = "hello"

	MethodNetworksList       = "networks.list"
	MethodNetworksConnect    = "networks.connect"
	MethodNetworksDisconnect = "networks.disconnect"
	MethodNetworksReconnect  = "networks.reconnect"

	MethodEventsSubscribe   = "events.subscribe"
	MethodEventsUnsubscribe = "events.unsubscribe"

	MethodHistoryMessages = "history.messages"
	MethodHistoryActivity = "history.activity"

	MethodReadMarkersGet = "read_markers.get"
	MethodReadMarkersSet = "read_markers.set"

	MethodClientsAttach = "clients.attach"
	MethodClientsDetach = "clients.detach"

	MethodSendLine = "send.line"

	MethodDevicesTouch        = "devices.touch"
	MethodDevicesNetworks     = "devices.networks"
	MethodDevicesSetNetwork   = "devices.set_network"
	MethodDevicesBuffers      = "devices.buffers"
	MethodDevicesOpen         = "devices.open"
	MethodDevicesClose        = "devices.close"
	MethodDevicesSeed         = "devices.seed"
	MethodDevicesClearRenames = "devices.clear_renames"

	MethodAuthCheck = "auth.check"

	MethodStorageGet    = "storage.get"
	MethodStorageSet    = "storage.set"
	MethodStorageDelete = "storage.delete"
	MethodStorageList   = "storage.list"

	MethodCommandsRegister = "commands.register"
	MethodCommandsReply    = "commands.reply"
)

// FeatureMethods lists each feature's methods; it is also the set of known features.
var FeatureMethods = map[string][]string{
	FeatureNetworks:    {MethodNetworksList, MethodNetworksConnect, MethodNetworksDisconnect, MethodNetworksReconnect},
	FeatureEvents:      {MethodEventsSubscribe, MethodEventsUnsubscribe},
	FeatureHistory:     {MethodHistoryMessages, MethodHistoryActivity},
	FeatureReadMarkers: {MethodReadMarkersGet, MethodReadMarkersSet},
	FeatureClients:     {MethodClientsAttach, MethodClientsDetach},
	FeatureSend:        {MethodSendLine},
	FeatureDevices: {MethodDevicesTouch, MethodDevicesNetworks, MethodDevicesSetNetwork, MethodDevicesBuffers,
		MethodDevicesOpen, MethodDevicesClose, MethodDevicesSeed, MethodDevicesClearRenames},
	FeatureAuth:     {MethodAuthCheck},
	FeatureStorage:  {MethodStorageGet, MethodStorageSet, MethodStorageDelete, MethodStorageList},
	FeatureCommands: {MethodCommandsRegister, MethodCommandsReply},

	FeatureMemberMeta:  nil,
	FeatureSilence:     nil,
	FeatureChannelMeta: nil,
}

// KnownFeature reports whether name is a feature of this API version.
func KnownFeature(name string) bool {
	_, ok := FeatureMethods[name]
	return ok
}

// Events.
const (
	EventReady         = "ready"
	EventNetwork       = "network"
	EventLine          = "line"
	EventClientLine    = "client_line"
	EventState         = "state"
	EventReadMarker    = "read_marker"
	EventBufferOpen    = "buffer_open"
	EventBufferClose   = "buffer_close"
	EventBufferRename  = "buffer_rename"
	EventDeviceNetwork = "device_network"
	EventCommand       = "command"
	EventOverflow      = "overflow"
	EventReload        = "reload"
)
