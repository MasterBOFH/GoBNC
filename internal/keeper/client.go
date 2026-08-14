package keeper

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// AttachClient is the protocol-level client side of the keeper<->brain
// contract: dial, negotiate, request a mode, receive lines. It has no
// opinion about IRC — no registration, no CAP, no SASL. Wiring that state
// machine to this client is separate work (build step 2, part 3).
type AttachClient struct {
	conn              net.Conn
	NegotiatedVersion int
	Mode              Mode
	// Networks lists every network the keeper held at the moment of this
	// attach — see HelloAckMsg.Networks.
	Networks []NetworkStatus

	// wmu serializes every writeFrame call on conn. One AttachClient is
	// shared by every network a brain process holds (see this type's own
	// doc comment and docs/keeper-design.md), so Send* is called
	// concurrently in normal operation — a per-network nick-recovery loop
	// and flood-pacing drain loop each have their own goroutine (see
	// internal/brain/nickrecovery.go, flood.go), on top of
	// armReconnect's timer callbacks and admin-triggered Dial/Close/
	// Reconnect calls. writeFrame is two separate Write calls (header,
	// then body); without serializing them here, two goroutines' calls
	// can interleave on the wire and desync the whole frame stream —
	// the keeper's readLoop has no way to resync a corrupted frame and
	// tears down the entire live session (every network on it, not just
	// the one whose write collided) the moment it fails to decode one.
	// Mirrors connio.Conn's own wmu, which exists for exactly the same
	// reason on the uplink side.
	wmu sync.Mutex
}

// Attach dials sockPath and performs the Hello/HelloAck handshake. On
// success the connection is ready for ValidateReady (validate mode) or
// LiveReady (live mode).
//
// Before dialing, Attach verifies sockPath is owned by this process's own
// UID (see security.go's verifySocketOwner) — defence in depth on the
// client side, mirroring the keeper's own SO_PEERCRED check on accept. The
// keeper's socket-directory permissions should already make connecting to
// an attacker-controlled socket at this path impossible; this check
// doesn't depend on trusting that they do.
func Attach(ctx context.Context, sockPath string, hello HelloMsg) (*AttachClient, error) {
	if err := verifySocketOwner(sockPath); err != nil {
		return nil, err
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("keeper attach: %w", err)
	}
	if hello.ClientVersion == 0 {
		hello.ClientVersion = ProtocolVersion
	}
	if err := writeFrame(conn, msgHello, hello); err != nil {
		conn.Close()
		return nil, fmt.Errorf("keeper attach: write Hello: %w", err)
	}
	t, body, err := readFrame(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("keeper attach: read HelloAck: %w", err)
	}
	if t == msgError {
		errMsg, _ := decodeFrame[ErrorMsg](t, msgError, body)
		conn.Close()
		return nil, fmt.Errorf("keeper attach: rejected: %s", errMsg.Reason)
	}
	ack, err := decodeFrame[HelloAckMsg](t, msgHelloAck, body)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("keeper attach: %w", err)
	}
	return &AttachClient{
		conn:              conn,
		NegotiatedVersion: ack.NegotiatedVersion,
		Mode:              ack.Mode,
		Networks:          ack.Networks,
	}, nil
}

// Close closes the underlying connection.
func (c *AttachClient) Close() error {
	return c.conn.Close()
}

// SendValidateReady signals the keeper that the client parsed the validate
// snapshot without dying. Validate mode only.
func (c *AttachClient) SendValidateReady() error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return writeFrame(c.conn, msgValidateReady, ValidateReadyMsg{})
}

// SendLiveReady signals the keeper to begin streaming from the FromSeq
// given in Hello. Live mode only.
func (c *AttachClient) SendLiveReady() error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return writeFrame(c.conn, msgLiveReady, LiveReadyMsg{})
}

// SendDial asks the keeper to dial (or redial) one network. Live mode only.
func (c *AttachClient) SendDial(network NetworkID, cfg DialConfig, fromSeq uint64) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return writeFrame(c.conn, msgDialRequest, DialRequestMsg{Network: network, Config: cfg, FromSeq: fromSeq})
}

// SendClose asks the keeper to close one network's uplink (not remove it —
// the network can be dialed again later). Live mode only.
func (c *AttachClient) SendClose(network NetworkID) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return writeFrame(c.conn, msgCloseRequest, CloseRequestMsg{Network: network})
}

// SendWrite asks the keeper to write line verbatim to network's uplink.
// Live mode only. This is the only way a brain-side driver can actually
// send anything — e.g. turning a registration.Action{Kind: ActionSend}
// into real traffic.
func (c *AttachClient) SendWrite(network NetworkID, line string) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return writeFrame(c.conn, msgWriteRequest, WriteRequestMsg{Network: network, Line: line})
}

// SendQuitClose asks the keeper to write line (typically "QUIT :reason")
// to network's uplink with a bounded deadline, then close the connection —
// see QuitCloseRequestMsg's doc comment. Live mode only. This is the only
// wire message that represents deliberately disconnecting from a network;
// it must never be sent just because the caller itself is exiting.
// timeout<=0 uses the keeper's default.
func (c *AttachClient) SendQuitClose(network NetworkID, line string, timeout time.Duration) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return writeFrame(c.conn, msgQuitCloseRequest, QuitCloseRequestMsg{Network: network, Line: line, Timeout: timeout})
}

// AttachEvent is a tagged union of what Next can return on a live
// connection: exactly one field is non-nil.
type AttachEvent struct {
	Line            *LineMsg
	DialResult      *DialResultMsg
	CloseResult     *CloseResultMsg
	NetworkEvent    *NetworkEventMsg
	WriteResult     *WriteResultMsg
	QuitCloseResult *QuitCloseResultMsg
}

// Next blocks for the next frame on a live connection — a delivered line,
// the result of a Dial/Close this client requested, or an unsolicited
// network connect/disconnect event. Live mode only.
func (c *AttachClient) Next() (AttachEvent, error) {
	t, body, err := readFrame(c.conn)
	if err != nil {
		return AttachEvent{}, err
	}
	switch t {
	case msgError:
		errMsg, err := decodeFrame[ErrorMsg](t, msgError, body)
		if err != nil {
			return AttachEvent{}, err
		}
		return AttachEvent{}, fmt.Errorf("keeper: %s", errMsg.Reason)
	case msgLine:
		m, err := decodeFrame[LineMsg](t, msgLine, body)
		return AttachEvent{Line: &m}, err
	case msgDialResult:
		m, err := decodeFrame[DialResultMsg](t, msgDialResult, body)
		return AttachEvent{DialResult: &m}, err
	case msgCloseResult:
		m, err := decodeFrame[CloseResultMsg](t, msgCloseResult, body)
		return AttachEvent{CloseResult: &m}, err
	case msgNetworkEvent:
		m, err := decodeFrame[NetworkEventMsg](t, msgNetworkEvent, body)
		return AttachEvent{NetworkEvent: &m}, err
	case msgWriteResult:
		m, err := decodeFrame[WriteResultMsg](t, msgWriteResult, body)
		return AttachEvent{WriteResult: &m}, err
	case msgQuitCloseResult:
		m, err := decodeFrame[QuitCloseResultMsg](t, msgQuitCloseResult, body)
		return AttachEvent{QuitCloseResult: &m}, err
	default:
		return AttachEvent{}, fmt.Errorf("keeper: unexpected message type %d", t)
	}
}

// NextLine blocks for the next delivered line specifically, discarding
// nothing — it errors if the next frame is some other event type. Live
// mode only, and only meaningful after SendLiveReady. Tests that don't care
// about Dial/Close/NetworkEvent traffic can use this instead of Next.
func (c *AttachClient) NextLine() (LineMsg, error) {
	ev, err := c.Next()
	if err != nil {
		return LineMsg{}, err
	}
	if ev.Line == nil {
		return LineMsg{}, fmt.Errorf("keeper: expected a line, got %+v", ev)
	}
	return *ev.Line, nil
}
