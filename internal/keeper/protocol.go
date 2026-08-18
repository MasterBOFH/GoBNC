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

// ProtocolVersion is this build's highest wire-protocol version. A client
// advertises it in Hello; the keeper negotiates down to the highest version
// both sides support, or rejects if there's no overlap. Additive JSON
// fields do not bump this — that's KeeperVersion in internal/version.
const ProtocolVersion = 1

// ProtocolMinVersion is the oldest wire-protocol version this brain will
// accept. Bump it (with ProtocolVersion) on a breaking wire change. Old
// keepers ignore the Hello field and may still negotiate down; Attach
// refuses after HelloAck if the negotiated version is below this.
const ProtocolMinVersion = 1

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
	msgBlobPush         msgType = 16 // client -> keeper, live mode only: apply one derived entry to a network's blob store
	msgBlobPushResult   msgType = 17 // keeper -> client, in response to msgBlobPush
	msgSeqAck           msgType = 18 // client -> keeper, live mode only: fire-and-forget resume-watermark advance
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
	ClientVersion int `json:"client_version"` // highest protocol version the client supports
	// MinProtocol is the oldest protocol version this client will accept.
	// 0 (omitted by pre-versioning brains) is treated as 1. A new keeper
	// rejects Hello when it cannot meet this; an old keeper ignores the
	// field and the brain refuses after HelloAck — see checkBrainCompat.
	MinProtocol int `json:"min_protocol,omitempty"`
	// BrainVersion is this brain's generation (internal/version.BrainVersion).
	BrainVersion int `json:"brain_version,omitempty"`
	// MinKeeperVersion is the oldest keeper generation this brain will
	// attach to. 0 means "do not require a minimum" (the Probe path, so
	// can-upgrade can always read HelloAck). Attach fills this from
	// internal/version.MinKeeperVersion. Checked on the keeper before
	// claiming the live slot, and again on the brain after HelloAck so
	// an old keeper that ignores the field still cannot serve a breaking
	// brain.
	MinKeeperVersion int  `json:"min_keeper_version,omitempty"`
	Mode             Mode `json:"mode"`
	// FromSeq is live mode only: per-network resume points, keyed by
	// NetworkID. A network absent from this map streams from that
	// Keeper's own tracked resume watermark (Keeper.DeliveredSeq) — the
	// highest seq a brain has explicitly acked as fully processed via
	// SeqAckMsg — not from oldest-retained. This is gap-only delivery:
	// from the exact point a previous live attach last acked, not a full
	// backlog replay. An explicit entry in this map (including an
	// explicit 0) overrides the watermark for that one network; this is
	// an escape hatch for tooling/debugging, not something a normal
	// attach needs to set — the normal case is an empty map.
	//
	// A first attempt at gap-only delivery (skipping replay without a
	// blob store to carry the state replay was standing in for) was tried
	// and reverted — Session's state reconstruction (self nick, ISUPPORT,
	// caps, channel membership, account) depended on watching the replayed
	// transcript at the time. That dependency is gone now that Session
	// pushes each of those as a blob entry as it learns them (see
	// docs/keeper-design.md's blob store section) and seeds itself from
	// the delivered blob snapshot on resume instead of from replay.
	// internal/history's keeper_seq-based idempotent storage remains as
	// defense in depth against a duplicate, not the primary mechanism
	// preventing one — gap-only delivery means there is normally nothing
	// to duplicate.
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
	NegotiatedVersion int `json:"negotiated_version"`
	// KeeperVersion is this keeper process's generation
	// (internal/version.KeeperVersion). Omitted (0) by pre-versioning
	// keepers; NormalizeKeeperVersion treats that as generation 1.
	KeeperVersion int `json:"keeper_version,omitempty"`
	// KeeperRelease is this keeper process's display version
	// (internal/version.DisplayVersion).
	KeeperRelease string          `json:"keeper_release,omitempty"`
	Mode          Mode            `json:"mode"`
	Networks      []NetworkStatus `json:"networks"`
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
// the process level too. FromSeq == 0 means "start from this network's own
// tracked resume watermark" (Keeper.DeliveredSeq) — the normal case for a
// still-attached brain redialing a network it's already partly consumed;
// a network being dialed for the first time already has a zero watermark,
// so this is a no-op then. A nonzero value overrides the watermark
// explicitly (tooling/debugging escape hatch, mirroring HelloMsg.FromSeq).
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

// BlobPushMsg asks the keeper to apply one derived entry to a network's
// blob store — the wire form of Keeper.PushBlob. Live mode only. Value is
// opaque to the keeper (it matches on Key/Mode only, never inspects
// Value), so — like WriteRequestMsg.Line — it is whatever the brain
// constructed itself from known-safe content (a resolved cap set, an
// ISUPPORT token, a channel key), not raw server bytes, so there's no
// non-UTF-8 concern requiring a []byte-specific encoding the way
// LineMsg.Raw needs one.
type BlobPushMsg struct {
	Network NetworkID `json:"network"`
	Key     string    `json:"key"`
	Mode    BlobMode  `json:"mode"`
	Value   []byte    `json:"value"`
}

// BlobPushResultMsg reports the outcome of a BlobPushMsg. Driver blocks on
// this before sending the SeqAckMsg for the line that triggered the push —
// see SeqAckMsg's doc comment for why that ordering is load-bearing, not
// just tidy.
type BlobPushResultMsg struct {
	Network NetworkID `json:"network"`
	OK      bool      `json:"ok"`
	Error   string    `json:"error,omitempty"`
}

// SeqAckMsg tells the keeper a brain has fully finished processing the
// line at Seq for Network — including pushing any blob entry that line's
// processing derived, which must have already completed (its
// BlobPushResultMsg received) before this is sent. Live mode only,
// fire-and-forget: no result message, since Keeper.AckSeq is a pure
// monotonic-max advance and there is nothing meaningful to fail. The
// keeper advances that network's resume watermark (Keeper.DeliveredSeq)
// to Seq on receipt and never earlier — never merely because it wrote the
// line to the wire. This is what makes a brain crash between receiving a
// line and pushing its derived blob entry safe: the ack for that line
// simply never arrives, so the watermark never passes it, and the next
// attach receives that line (and everything after it) again.
type SeqAckMsg struct {
	Network NetworkID `json:"network"`
	Seq     uint64    `json:"seq"`
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
