// Keeper<->brain wire protocol: a length-prefixed, typed-frame contract over
// a unix socket. This is a permanent contract in the sense the design brief
// means it — an old keeper must be able to talk to a new brain — so two
// properties matter more than anything else on this file:
//
//  1. Adding a field must never be a breaking change. Codec: JSON-per-frame.
//     encoding/json ignores fields it doesn't recognize on decode by
//     default (no DisallowUnknownFields), which is exactly the property
//     needed; see TestUnknownFieldIgnoredNotRejected. Chosen over protobuf
//     because this repo has no codegen step today and no protoc in the
//     dev/CI toolchain — adding one is a bigger commitment than a local IPC
//     protocol on a unix socket needs. JSON is human-readable on the wire,
//     which matters while this protocol is still being iterated on.
//  2. The IRC line is opaque bytes, never a string. IRC lines are not
//     guaranteed valid UTF-8 (Latin-1 networks exist), and json.Marshal
//     silently mangles invalid UTF-8 in a Go string into the replacement
//     character — a silent corruption bug, not a crash. LineMsg.Raw is
//     []byte, which encoding/json base64-encodes without inspecting content;
//     see TestLineMsgPreservesNonUTF8Bytes.
package keeper

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// ProtocolVersion is this build's protocol version. A client advertises the
// highest version it supports in Hello; the keeper negotiates down to the
// highest version both sides support, or rejects if there's no overlap.
const ProtocolVersion = 1

// keeperMinVersion/keeperMaxVersion bound what this keeper build accepts.
// Both equal ProtocolVersion today; they'll diverge once there's a version 2
// and old keepers still need to serve version-1 brains.
const (
	keeperMinVersion = 1
	keeperMaxVersion = ProtocolVersion
)

// maxFrameSize bounds a single frame — a defense against a peer (or a bug)
// claiming an unreasonable length and exhausting memory. Large enough for
// any message this protocol defines with wide margin; IRC lines are bounded
// by irc.MaxServerLine long before they'd approach this.
const maxFrameSize = 1 << 20 // 1MiB

// msgType identifies the schema of a frame's body. Frames carry an explicit
// type rather than being disambiguated by inspecting the body — a control
// message and an IRC line are different shapes on the wire, not one shape
// guessed apart.
type msgType uint8

const (
	msgHello            msgType = 1  // client -> keeper, first message on the connection
	msgHelloAck         msgType = 2  // keeper -> client, negotiation result
	msgError            msgType = 3  // either direction; connection closes after
	msgValidateReady    msgType = 4  // client -> keeper, validate mode only
	msgLiveReady        msgType = 5  // client -> keeper, live mode only; triggers delivery
	msgLine             msgType = 6  // keeper -> client, live mode only, post-LiveReady
	msgDialRequest      msgType = 7  // client -> keeper, live mode only
	msgDialResult       msgType = 8  // keeper -> client, in response to msgDialRequest
	msgCloseRequest     msgType = 9  // client -> keeper, live mode only
	msgCloseResult      msgType = 10 // keeper -> client, in response to msgCloseRequest
	msgNetworkEvent     msgType = 11 // keeper -> client, unsolicited: a network connected or disconnected
	msgWriteRequest     msgType = 12 // client -> keeper, live mode only: write a raw line to one network's uplink
	msgWriteResult      msgType = 13 // keeper -> client, in response to msgWriteRequest
	msgQuitCloseRequest msgType = 14 // client -> keeper, live mode only: write a final line with a bounded deadline, then close
	msgQuitCloseResult  msgType = 15 // keeper -> client, in response to msgQuitCloseRequest
)

// Mode is the attach mode a client requests in Hello.
type Mode string

const (
	// ModeValidate: keeper delivers a read-only snapshot and no live lines,
	// enforced keeper-side regardless of what the client asks for after
	// attaching. See Listener's connection handler.
	ModeValidate Mode = "validate"
	// ModeLive: keeper streams lines from FromSeq once the client signals
	// LiveReady. Exactly one live attach is permitted at a time.
	ModeLive Mode = "live"
)

// HelloMsg is the client's opening message. One connection attaches to the
// whole keeper process — every network it currently holds — not to a single
// network; there is one brain, and a brain restart must preserve every
// uplink simultaneously.
type HelloMsg struct {
	ClientVersion int  `json:"client_version"` // highest protocol version the client supports
	Mode          Mode `json:"mode"`
	// FromSeq is live mode only: per-network resume points, keyed by
	// NetworkID. A network the keeper holds but that's absent from this map
	// is streamed from seq 0 (from oldest retained) — this is the normal
	// case for a network the client has no prior checkpoint for yet.
	//
	// Deliberately always replayed in full, never reduced to "only what's
	// new since some checkpoint": Session's entire state reconstruction on
	// a resumed attach (registration completion, self nick, ISUPPORT,
	// channel membership — not just chat history) depends on watching the
	// complete transcript, since there is no separate state snapshot (the
	// blob store docs/keeper-design.md defers). A brain-side checkpoint
	// that skipped replay was tried and reverted — see internal/history's
	// keeper_seq-based idempotent storage for how replay-safe duplication
	// is actually avoided instead: by making replaying an already-stored
	// line a safe no-op, not by not replaying it.
	FromSeq map[NetworkID]uint64 `json:"from_seq,omitempty"`
}

// HelloAckMsg is the keeper's response to a successful Hello.
//
// Networks is a one-way, attach-time-only report, not a live roster the
// keeper keeps pushed to the client — and that asymmetry is deliberate, not
// a gap to close later. Dial/Close are wire operations the brain issues (see
// DialRequestMsg/CloseRequestMsg): the brain is the source of truth for
// which networks exist, because it owns configuration (the database). The
// keeper never needs to announce a network the brain didn't tell it to
// create, so no push-notification mechanism is needed for additions.
// Networks exists purely for the reverse direction: letting a newly
// attached (or reattached) brain discover what the keeper already had
// dialed — most importantly, what survived the brain's own restart.
type HelloAckMsg struct {
	NegotiatedVersion int             `json:"negotiated_version"`
	Mode              Mode            `json:"mode"`
	Networks          []NetworkStatus `json:"networks"`
}

// ErrorMsg is fatal: the sender closes the connection immediately after.
type ErrorMsg struct {
	Reason string `json:"reason"`
}

// ValidateReadyMsg: "I parsed the snapshot and built state without dying."
// A pure statement of fact about the client's own state, not a request —
// the keeper delivers nothing in validate mode regardless, and awaits
// promotion or teardown (out of scope until the blob store exists).
type ValidateReadyMsg struct{}

// DialRequestMsg asks the keeper to dial (or redial) one network. Live mode
// only. Config is keeper.DialConfig verbatim — the brain owns configuration
// (it has the database), so the keeper is told everything it needs for this
// one attempt and caches none of it; the "read TLS material from disk at
// dial time, never cache it" invariant this package already follows extends
// naturally to "the keeper doesn't remember dial config between dials" at
// the process level too. FromSeq only matters if this network already has
// retained backlog from a prior epoch the brain wants to resume from
// (e.g. reattaching to a network the keeper kept holding); 0 for a network
// being dialed for the first time.
type DialRequestMsg struct {
	Network NetworkID  `json:"network"`
	Config  DialConfig `json:"config"`
	FromSeq uint64     `json:"from_seq"`
}

// DialResultMsg reports the outcome of a DialRequestMsg. Epoch is only
// meaningful when OK is true.
type DialResultMsg struct {
	Network NetworkID `json:"network"`
	OK      bool      `json:"ok"`
	Error   string    `json:"error,omitempty"`
	Epoch   uint64    `json:"epoch"`
}

// CloseRequestMsg asks the keeper to close one network's uplink. Live mode
// only. Does not remove the network from the keeper — Close, not
// RemoveNetwork; the brain can Dial the same network again later and resume
// from where its backlog allows.
type CloseRequestMsg struct {
	Network NetworkID `json:"network"`
}

// CloseResultMsg reports the outcome of a CloseRequestMsg.
type CloseResultMsg struct {
	Network NetworkID `json:"network"`
	OK      bool      `json:"ok"`
	Error   string    `json:"error,omitempty"`
}

// NetworkEventMsg reports a connect/disconnect transition for one network,
// pushed unsolicited as it happens — this is how the brain learns a socket
// died without polling. Kind mirrors keeper.EventKind; Error is set only
// for EventDisconnected and only when the socket died on its own (mirrors
// Event.Err — nil/empty for a deliberate Close).
type NetworkEventMsg struct {
	Network NetworkID `json:"network"`
	Kind    EventKind `json:"kind"`
	Epoch   uint64    `json:"epoch"`
	Error   string    `json:"error,omitempty"`
}

// WriteRequestMsg asks the keeper to write one raw line to a network's
// uplink verbatim — this is how a brain-side driver turns a
// registration.Action{Kind: ActionSend} (or any other line the brain
// decides to send) into something that actually reaches the wire. Line is
// a string, not []byte like LineMsg.Raw: unlike a line the *server* sent
// (which can be anything, including non-UTF-8), a line the brain sends is
// always something it constructed itself from known-safe content —
// commands, nicknames, capability names, base64 SASL payloads — so there's
// no analogous risk of JSON mangling it.
type WriteRequestMsg struct {
	Network NetworkID `json:"network"`
	Line    string    `json:"line"`
}

// WriteResultMsg reports the outcome of a WriteRequestMsg. This is the
// backpressure signal a brain-side flood pacer reacts to instead of
// checking a raw socket for liveness (internal/uplink/flood.go's
// enqueueFlood read u.conn == nil before queuing; the brain has no socket
// to read, so this is what replaces that check — see
// docs/keeper-design.md's Part 3b-i section). Refused distinguishes two
// different failures a pacer needs to react to differently: Refused=true
// means there was no live connection to write to at all (the network is
// down — matches the old u.conn==nil check, and the right reaction is the
// same one that check produced: don't send, don't retry this particular
// line, wait for reconnection) versus Refused=false with OK=false, which
// means a write was attempted on a connection that appeared live and
// failed anyway (e.g. the socket died in the gap between the pacer's own
// liveness assumption and this write reaching the keeper) — ordinary
// connection-loss handling applies, not a pacing decision.
type WriteResultMsg struct {
	Network NetworkID `json:"network"`
	OK      bool      `json:"ok"`
	Refused bool      `json:"refused,omitempty"`
	Error   string    `json:"error,omitempty"`
}

// QuitCloseRequestMsg asks the keeper to write one final line (typically
// QUIT) to a network's uplink with a bounded deadline, then close the
// connection regardless of whether the write completed — see
// Keeper.QuitClose's doc comment for why this is one wire operation, not a
// WriteRequest followed by a CloseRequest: bounding the whole write-then-
// close sequence with a single deadline is the property internal/uplink's
// writeQuit got for free by owning the raw socket directly, which the
// brain no longer does. This is the ONLY message that represents the
// brain deliberately disconnecting from a network — it must never be sent
// merely because the brain itself is exiting (see
// docs/keeper-design.md's shutdown-vs-disconnect distinction).
type QuitCloseRequestMsg struct {
	Network NetworkID     `json:"network"`
	Line    string        `json:"line"`
	Timeout time.Duration `json:"timeout,omitempty"` // <=0: keeper default
}

// QuitCloseResultMsg reports the outcome of a QuitCloseRequestMsg.
type QuitCloseResultMsg struct {
	Network NetworkID `json:"network"`
	OK      bool      `json:"ok"`
	Error   string    `json:"error,omitempty"`
}

// LiveReadyMsg: "load me and start delivering." Triggers the keeper to
// begin streaming from HelloMsg.FromSeq.
type LiveReadyMsg struct{}

// LineMsg is one buffered line, delivered in live mode after LiveReady.
// Lines from every network the client is attached to arrive interleaved on
// the same connection; Network is how the client demultiplexes them.
type LineMsg struct {
	Network NetworkID `json:"network"`
	Seq     uint64    `json:"seq"`
	Epoch   uint64    `json:"epoch"`
	Raw     []byte    `json:"raw"` // exact bytes read from the uplink — see package doc
	Time    time.Time `json:"time"`
}

// writeFrame writes one length-prefixed, typed frame: 4-byte big-endian
// length (of type byte + body), 1-byte type, then the JSON body.
func writeFrame(w io.Writer, t msgType, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("keeper protocol: marshal %v: %w", t, err)
	}
	if len(body)+1 > maxFrameSize {
		return fmt.Errorf("keeper protocol: frame too large (%d bytes)", len(body)+1)
	}
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(body)+1))
	hdr[4] = byte(t)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}

// readFrame reads one frame and returns its type and raw JSON body,
// undecoded — the caller knows from t which struct to decode into.
func readFrame(r io.Reader) (t msgType, body []byte, err error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n == 0 {
		return 0, nil, fmt.Errorf("keeper protocol: empty frame")
	}
	if n > maxFrameSize {
		return 0, nil, fmt.Errorf("keeper protocol: frame too large (%d bytes)", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, err
	}
	return msgType(buf[0]), buf[1:], nil
}

func decodeFrame[T any](t msgType, want msgType, body []byte) (T, error) {
	var v T
	if t != want {
		return v, fmt.Errorf("keeper protocol: expected type %d, got %d", want, t)
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return v, fmt.Errorf("keeper protocol: unmarshal type %d: %w", t, err)
	}
	return v, nil
}
