package keeper

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/version"
)

// Listener serves the keeper<->brain protocol over a unix socket, backed by
// a Manager — one connection attaches to every network the process holds,
// not to a single uplink. It enforces the rules that must not depend on the
// client behaving: a validate-mode connection never receives a line, and at
// most one live-mode connection is attached across the whole process at a
// time (there is one brain; two brains consuming any network concurrently
// is the corruption case this exists to prevent).
//
// It also enforces who may connect at all — see security.go. That
// enforcement depends on this being a unix socket specifically (a 0700
// directory and SO_PEERCRED both only mean anything for a unix socket);
// there is deliberately no way to make Serve listen on TCP instead, not
// even behind a flag. If a future change wants that, it has to go read
// security.go's doc comment first and reckon with every guarantee this
// file currently makes silently becoming false.
type Listener struct {
	mgr         *Manager
	log         *slog.Logger
	expectedUID uint32

	mu           sync.Mutex
	liveAttached bool
}

// ListenerOption configures a Listener at construction.
type ListenerOption func(*Listener)

// WithExpectedUID overrides the UID authorizePeer requires a connecting
// peer to match — defaults to os.Getuid() (the keeper and brain are
// expected to run as the same user; see security.go). Exists for two
// reasons: it's the injection point a deployment running the brain as a
// different user from the keeper would need, and it's what makes
// UID-mismatch rejection testable end-to-end in this environment, which
// has no second real user account to connect from — set it to a UID that
// deliberately isn't this process's own and connect normally, rather than
// needing a genuinely different peer identity.
func WithExpectedUID(uid uint32) ListenerOption {
	return func(l *Listener) { l.expectedUID = uid }
}

// NewListener wraps mgr for protocol serving.
func NewListener(mgr *Manager, log *slog.Logger, opts ...ListenerOption) *Listener {
	if log == nil {
		log = slog.Default()
	}
	l := &Listener{mgr: mgr, log: log, expectedUID: uint32(os.Getuid())}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Serve accepts connections on sockPath until ctx is canceled. sockPath's
// directory must already be, or be creatable as, a 0700 directory owned by
// the calling user — see ensureSocketDir; Serve refuses to run otherwise.
// It removes any pre-existing file at sockPath before binding (a stale
// socket from a prior run), and removes the socket file on return.
func (l *Listener) Serve(ctx context.Context, sockPath string) error {
	if err := ensureSocketDir(filepath.Dir(sockPath)); err != nil {
		return err
	}
	_ = os.Remove(sockPath)
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("keeper listener: %w", err)
	}
	defer func() {
		_ = ln.Close()
		_ = os.Remove(sockPath)
	}()

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	if !peerCredSupported {
		l.log.Warn("keeper listener: peer credential verification is not supported on this platform; relying on socket directory permissions alone")
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if !l.authorizePeer(conn) {
			_ = conn.Close()
			continue
		}
		go l.handleConn(ctx, conn)
	}
}

// authorizePeer is the server-side half of the security.go peer-check
// layer. It is defence in depth, not the primary control — the socket
// directory's permissions should already have kept an unauthorized process
// from connecting at all — but a misconfigured deployment (e.g. a socket
// directory whose permissions were loosened after Serve last checked them)
// is exactly what this catches instead of silently trusting the directory.
func (l *Listener) authorizePeer(conn net.Conn) bool {
	if !peerCredSupported {
		return true // already warned once in Serve
	}
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		l.log.Warn("keeper listener: accepted a non-unix connection, rejecting")
		return false
	}
	uid, err := peerUID(uc)
	if err != nil {
		l.log.Warn("keeper listener: peer credential check failed, rejecting connection", "err", err)
		return false
	}
	if !uidAuthorized(uid, l.expectedUID) {
		l.log.Warn("keeper listener: rejected connection from unexpected uid", "uid", uid, "want", l.expectedUID)
		return false
	}
	return true
}

func (l *Listener) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	t, body, err := readFrame(conn)
	if err != nil {
		l.log.Warn("keeper listener: read Hello failed", "err", err)
		return
	}
	hello, err := decodeFrame[HelloMsg](t, msgHello, body)
	if err != nil {
		l.log.Warn("keeper listener: bad Hello", "err", err)
		_ = writeFrame(conn, msgError, ErrorMsg{Reason: err.Error()})
		return
	}

	l.log.Debug("keeper listener: received Hello",
		"mode", hello.Mode,
		"client_version", hello.ClientVersion,
		"min_protocol", hello.MinProtocol,
		"brain_version", hello.BrainVersion,
		"min_keeper_version", hello.MinKeeperVersion,
	)

	negotiated, err := negotiateVersion(hello.ClientVersion, hello.MinProtocol)
	if err != nil {
		_ = writeFrame(conn, msgError, ErrorMsg{Reason: err.Error()})
		return
	}

	// Component-version check before claiming the live slot: a breaking
	// new brain must fail Hello while the old live brain is still
	// attached (validate probes never claim live; this also covers a
	// ModeLive attempt that would otherwise wait on liveAttached).
	if hello.MinKeeperVersion > version.KeeperVersion {
		reason := fmt.Sprintf("keeper version %d is below brain minimum %d", version.KeeperVersion, hello.MinKeeperVersion)
		_ = writeFrame(conn, msgError, ErrorMsg{Reason: reason})
		l.log.Warn("keeper listener: rejected Hello: keeper too old for this brain",
			"keeper_version", version.KeeperVersion,
			"min_keeper_version", hello.MinKeeperVersion,
		)
		return
	}

	if hello.Mode == ModeLive {
		l.mu.Lock()
		if l.liveAttached {
			l.mu.Unlock()
			_ = writeFrame(conn, msgError, ErrorMsg{Reason: "a live attach is already active"})
			l.log.Warn("keeper listener: rejected second live attach")
			return
		}
		l.liveAttached = true
		l.mu.Unlock()
		defer func() {
			l.mu.Lock()
			l.liveAttached = false
			l.mu.Unlock()
		}()
	} else if hello.Mode != ModeValidate {
		_ = writeFrame(conn, msgError, ErrorMsg{Reason: fmt.Sprintf("unknown mode %q", hello.Mode)})
		return
	}

	networks := l.mgr.All()
	ack := HelloAckMsg{
		NegotiatedVersion: negotiated,
		KeeperVersion:     version.KeeperVersion,
		KeeperRelease:     version.DisplayVersion(),
		Mode:              hello.Mode,
		Networks:          statusOf(networks),
	}
	l.log.Debug("keeper listener: sending HelloAck",
		"negotiated_version", negotiated,
		"keeper_version", ack.KeeperVersion,
		"mode", hello.Mode,
		"networks", len(ack.Networks),
	)
	if err := writeFrame(conn, msgHelloAck, ack); err != nil {
		l.log.Warn("keeper listener: write HelloAck failed", "err", err)
		return
	}

	if hello.Mode == ModeValidate {
		l.serveValidate(conn)
		return
	}
	l.serveLive(ctx, conn, networks, hello.FromSeq)
}

// serveValidate never delivers a line, regardless of what the client sends
// after Hello — that guarantee does not depend on the client only ever
// sending ValidateReady; there is no code path in this function that can
// write a msgLine frame. It waits for either ValidateReady (logged, no
// further action — promotion/teardown orchestration doesn't exist until the
// blob store does) or the client disconnecting. Validate mode covers every
// network in one pass, matching live mode: cutover is a whole-process
// decision, so there is nothing per-network to validate separately. Dial
// and Close are live-mode-only operations — a validate connection is
// read-only by construction, so it has no wire path to request either.
func (l *Listener) serveValidate(conn net.Conn) {
	for {
		t, body, err := readFrame(conn)
		if err != nil {
			return // client disconnected (teardown) or errored
		}
		switch t {
		case msgValidateReady:
			if _, err := decodeFrame[ValidateReadyMsg](t, msgValidateReady, body); err != nil {
				return
			}
			l.log.Info("keeper listener: validate-ready received")
			// Intentionally no response, no line delivery. Awaits promotion
			// (not yet built) or the client disconnecting.
		default:
			_ = writeFrame(conn, msgError, ErrorMsg{Reason: fmt.Sprintf("unexpected message type %d in validate mode", t)})
			return
		}
	}
}

// outMsg is one frame queued for the live connection's single writer,
// tagged with the type so the writer doesn't need per-message-kind
// channels. Every producer (per-network fan-in, dial/close handlers) feeds
// the same channel so writes to conn are never interleaved from two
// goroutines at once.
type outMsg struct {
	t    msgType
	body any
}

// liveSession holds the state one live-attached connection needs beyond
// what fits in a handful of function arguments: which networks already have
// a fan-in goroutine running (so a Dial for an already-streaming network
// doesn't start a second one), the shared outbound queue, and the kill
// switch a per-network overflow or eviction gap trips.
type liveSession struct {
	l    *Listener
	ctx  context.Context
	conn net.Conn

	out  chan outMsg
	kill func(error)

	mu      sync.Mutex
	active  map[NetworkID]bool
	stopped bool // set under mu before s.wg.Wait() in teardown — see beginWork

	wg sync.WaitGroup
}

// beginWork registers one more goroutine with s.wg, unless teardown has
// already begun — the two are made mutually exclusive via s.mu so there is
// a real happens-before edge between "no more Add calls will succeed" and
// the s.wg.Wait() teardown does next, not just a best-effort ctx check.
// sync.WaitGroup's own docs are explicit that an Add racing a Wait that has
// already observed the counter reach zero is undefined — readLoop calling
// s.wg.Add(1) for a request it finished reading just as the connection
// started teardown is exactly that race, caught live by the race detector
// once this package started being exercised harder (many rapid
// attach/detach cycles) than its own tests previously did. Returns false
// if the caller should stop dispatching further requests.
func (s *liveSession) beginWork() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return false
	}
	s.wg.Add(1)
	return true
}

// serveLive waits for LiveReady, then streams every network in networks —
// snapshotted at Hello time — starting from each one's entry in fromSeq (or
// seq 0 if absent), interleaved onto the one connection. Per the
// attach-sequence ordering requirement: no delivery happens before
// LiveReady is received, so a client still loading a snapshot can't race a
// line against its own state build.
//
// After LiveReady, the connection is full-duplex: the brain may send
// DialRequest/CloseRequest at any point to bring networks up or down (this
// is the only way networks are dialed at all — the keeper never dials on
// its own), and the keeper pushes Line frames and unsolicited
// NetworkEvent frames (connect/disconnect) as they happen.
//
// A per-network subscriber that overflows (the client fell behind the
// live buffer) catches up from the ring rather than killing the attach.
// A WHO/NAMES burst is faster than unix-socket JSON framing; tearing the
// keeper↔brain link down over that is what produced the broken-pipe on
// a subsequent WriteRequest. The ring is the real buffer; Overflow just
// means the small live channel lagged. Kill only if Since reports the
// catch-up window has already been evicted — a genuine unrecoverable gap.
func (l *Listener) serveLive(ctx context.Context, conn net.Conn, networks map[NetworkID]*Keeper, fromSeq map[NetworkID]uint64) {
	t, body, err := readFrame(conn)
	if err != nil {
		return
	}
	if _, err := decodeFrame[LiveReadyMsg](t, msgLiveReady, body); err != nil {
		_ = writeFrame(conn, msgError, ErrorMsg{Reason: err.Error()})
		return
	}
	l.log.Debug("keeper listener: received LiveReady", "networks", len(networks))

	// A per-connection context, distinct from the listener-wide ctx: when
	// this connection is torn down for any reason (kill, peer gone, or the
	// listener itself shutting down), every fan-in/handler goroutine it
	// spawned needs to unblock immediately, including ones sending to s.out
	// for a network unrelated to why the connection is closing. Without
	// this, a kill triggered by one network's overflow would leave every
	// other network's fan-in goroutine permanently blocked on `s.out <-`
	// once the writer loop below stops draining it — the listener-wide ctx
	// alone doesn't cancel until the whole process shuts down.
	connCtx, connCancel := context.WithCancel(ctx)

	s := &liveSession{
		l:      l,
		ctx:    connCtx,
		conn:   conn,
		out:    make(chan outMsg, 1024),
		active: make(map[NetworkID]bool),
	}

	defer func() {
		connCancel()
		// readLoop blocks in readFrame(s.conn) — a plain net.Conn read,
		// not gated by connCtx at all — so canceling connCtx alone never
		// unblocks it. Without closing conn here, readLoop can stay alive
		// after this function has otherwise decided to tear down,
		// including successfully reading one more request and calling
		// s.wg.Add(1) for it (Dial/Close/QuitClose still dispatch via
		// goroutines — see readLoop's own cases) at the same time the
		// s.wg.Wait() below runs: a real, race-detector-caught instance
		// of the exact "Add racing a Wait that's already observed zero"
		// hazard flagged (for a different reason) in the comment above
		// serveLive's DialRequest handling. handleConn's own deferred
		// conn.Close() (the connection's other close path) fires too
		// late to prevent this — only after this whole function, and its
		// own deferred cleanup, has already returned. A second Close()
		// call from handleConn afterward is a harmless no-op/error.
		_ = conn.Close()
		// The unix connection is dead: free the live-attach slot before
		// waiting for fan-in goroutines. A brain reload closes the attach
		// and immediately reattaches; holding liveAttached through
		// wg.Wait() (up to 2s) rejects that reattach with "a live attach
		// is already active". handleConn's own defer also clears this;
		// clearing twice is a no-op.
		l.mu.Lock()
		l.liveAttached = false
		l.mu.Unlock()
		// Bounded best-effort wait for every fan-in/request-handler
		// goroutine this session spawned to actually exit, now that
		// they've all been told to via connCancel. Not required for
		// correctness (they will exit on their own regardless of whether
		// anyone waits here) but it turns "eventually stops leaking" into
		// something a caller — or a test — can observe deterministically
		// instead of polling runtime.NumGoroutine() and hoping.
		// The happens-before edge beginWork/startFanIn depend on: no
		// wg.Add(1) started after this point can succeed, since both
		// check s.stopped inside the same s.mu critical section they
		// call wg.Add in.
		s.mu.Lock()
		s.stopped = true
		s.mu.Unlock()
		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			l.log.Warn("keeper listener: live session goroutines did not wind down within 2s of teardown")
		}
	}()

	killCh := make(chan error, 1)
	var killOnce sync.Once
	s.kill = func(err error) {
		killOnce.Do(func() {
			select {
			case killCh <- err:
			default:
			}
		})
	}

	for id, k := range networks {
		// Absent from fromSeq (the normal case — see HelloMsg.FromSeq's doc
		// comment) means "gap only": start from this network's own tracked
		// resume watermark, not from oldest-retained. An explicit entry,
		// including an explicit 0, overrides it.
		from, ok := fromSeq[id]
		if !ok {
			from = k.DeliveredSeq()
		}
		s.startFanIn(id, k, from)
	}
	// s.out is deliberately never closed here. It used to be, gated on
	// wg.Wait(), but the producer set isn't fixed at connection start —
	// wire DialRequest can add a new fan-in goroutine (wg.Add) at any
	// point, including after wg's count has already dropped to zero (an
	// empty-at-attach connection with zero initial networks hits exactly
	// this: Wait returns immediately). sync.WaitGroup explicitly forbids
	// that ordering — an Add racing a Wait that's already observed zero is
	// undefined. Teardown instead goes through ctx/readerDone/killCh below;
	// s.out just stops being drained when this function returns, and
	// connCancel (deferred above) is what actually stops every producer
	// still trying to send to it.
	//
	// Full-duplex from here: a dedicated reader processes DialRequest and
	// CloseRequest as they arrive, independent of the writer loop below —
	// a slow or absent request stream must not stall line delivery, and
	// vice versa.
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		s.readLoop()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-readerDone:
			return
		case err := <-killCh:
			_ = writeFrame(conn, msgError, ErrorMsg{Reason: err.Error()})
			l.log.Warn("keeper listener: killing live connection", "err", err)
			return
		case m := <-s.out:
			if err := writeFrame(conn, m.t, m.body); err != nil {
				return
			}
		}
	}
}

// readLoop processes DialRequest/CloseRequest from the brain until the
// connection errors or the peer disconnects. Each request is handled in
// its own goroutine (added to s.wg, same as fan-in) so a slow dial attempt
// never blocks reading the next request.
func (s *liveSession) readLoop() {
	for {
		t, body, err := readFrame(s.conn)
		if err != nil {
			return
		}
		if s.ctx.Err() != nil {
			// Cheap early exit once teardown has started (see serveLive's
			// deferred cleanup, which closes s.conn to unblock the
			// readFrame above) — beginWork/startFanIn's own s.stopped
			// check below is what actually makes this race-free; this is
			// just skipping needless decode work on a request nothing
			// will use the result of anyway.
			return
		}
		switch t {
		case msgDialRequest:
			req, err := decodeFrame[DialRequestMsg](t, msgDialRequest, body)
			if err != nil {
				return
			}
			s.l.log.Debug("keeper listener: received DialRequest", "network", req.Network, "host", req.Config.Host, "port", req.Config.Port, "tls", req.Config.TLS, "from_seq", req.FromSeq)
			if !s.beginWork() {
				return
			}
			go s.handleDial(req)
		case msgCloseRequest:
			req, err := decodeFrame[CloseRequestMsg](t, msgCloseRequest, body)
			if err != nil {
				return
			}
			s.l.log.Debug("keeper listener: received CloseRequest", "network", req.Network)
			if !s.beginWork() {
				return
			}
			go s.handleClose(req)
		case msgWriteRequest:
			req, err := decodeFrame[WriteRequestMsg](t, msgWriteRequest, body)
			if err != nil {
				return
			}
			s.l.log.Debug("keeper listener: received WriteRequest", "network", req.Network, "bytes", len(req.Line))
			// Deliberately synchronous, unlike Dial/Close/QuitClose above:
			// a caller that fires a rapid burst of writes to the same
			// network (e.g. registration.Start's CAP LS/PASS/NICK/USER)
			// needs them to reach the wire in the order they were sent.
			// Dispatching each to its own goroutine -- which is exactly
			// what this case used to do -- gave every write a race with
			// every other write to the same network, with no ordering
			// guarantee between them; found live, by hand, once a test
			// harness switched from a net.Pipe (whose synchronous
			// Read/Write happened to mask the race almost every time) to
			// a real TCP loopback connection, which exposed it reliably.
			// Keeper.WriteLine is a fast, buffered call under normal
			// conditions, so serializing it here costs little: it only
			// delays reading this connection's *next* request (for any
			// network) behind one write actually reaching the wire, never
			// blocks on a slow remote the way Dial legitimately can.
			s.handleWrite(req)
		case msgQuitCloseRequest:
			req, err := decodeFrame[QuitCloseRequestMsg](t, msgQuitCloseRequest, body)
			if err != nil {
				return
			}
			s.l.log.Debug("keeper listener: received QuitCloseRequest", "network", req.Network, "bytes", len(req.Line))
			if !s.beginWork() {
				return
			}
			go s.handleQuitClose(req)
		case msgBlobPush:
			req, err := decodeFrame[BlobPushMsg](t, msgBlobPush, body)
			if err != nil {
				return
			}
			s.l.log.Debug("keeper listener: received BlobPush", "network", req.Network, "key", req.Key, "mode", req.Mode, "bytes", len(req.Value))
			// Synchronous, like handleWrite: Driver blocks on this result
			// before sending the SeqAck for the line that triggered it (see
			// SeqAckMsg's doc comment), so a burst of pushes for the same
			// network must land in the order they were sent, the same
			// ordering reason handleWrite is synchronous.
			s.handleBlobPush(req)
		case msgSeqAck:
			req, err := decodeFrame[SeqAckMsg](t, msgSeqAck, body)
			if err != nil {
				return
			}
			s.l.log.Debug("keeper listener: received SeqAck", "network", req.Network, "seq", req.Seq)
			// Fire-and-forget, no s.wg tracking: Keeper.AckSeq is a fast,
			// non-blocking monotonic-max advance with nothing to report
			// back (see SeqAckMsg's doc comment).
			if k := s.l.mgr.Network(req.Network); k != nil {
				k.AckSeq(req.Seq)
			}
		default:
			s.trySend(outMsg{msgError, ErrorMsg{Reason: fmt.Sprintf("unexpected message type %d on a live connection", t)}})
			return
		}
	}
}

func (s *liveSession) handleDial(req DialRequestMsg) {
	defer s.wg.Done()
	k := s.l.mgr.EnsureNetwork(req.Network)
	err := k.Dial(s.ctx, req.Config)
	_, epoch := k.State()
	result := DialResultMsg{Network: req.Network, OK: err == nil, Epoch: epoch}
	if err != nil {
		result.Error = err.Error()
	}
	s.l.log.Debug("keeper listener: sending DialResult", "network", result.Network, "ok", result.OK, "epoch", result.Epoch, "err", result.Error)
	s.trySend(outMsg{msgDialResult, result})
	if err == nil {
		// req.FromSeq == 0 means "use this network's own tracked resume
		// watermark" (see DialRequestMsg's doc comment) — the normal case
		// for a still-attached brain redialing a network it already has a
		// watermark for. A freshly created network's watermark is already
		// 0, so this is a no-op for a genuinely first-ever dial.
		from := req.FromSeq
		if from == 0 {
			from = k.DeliveredSeq()
		}
		s.startFanIn(req.Network, k, from)
	}
}

func (s *liveSession) handleClose(req CloseRequestMsg) {
	defer s.wg.Done()
	k := s.l.mgr.Network(req.Network)
	if k == nil {
		s.trySend(outMsg{msgCloseResult, CloseResultMsg{Network: req.Network, OK: false, Error: "unknown network"}})
		return
	}
	err := k.Close()
	result := CloseResultMsg{Network: req.Network, OK: err == nil}
	if err != nil {
		result.Error = err.Error()
	}
	s.l.log.Debug("keeper listener: sending CloseResult", "network", result.Network, "ok", result.OK, "err", result.Error)
	s.trySend(outMsg{msgCloseResult, result})
}

// handleWrite writes one brain-supplied line to a network's uplink
// verbatim via Keeper.WriteLine — this is the only path a brain-side
// driver (e.g. internal/brain wiring registration.Action{Kind: ActionSend}
// to the wire) has for actually sending anything.
// handleWrite is called synchronously from readLoop (see its
// msgWriteRequest case) — no s.wg tracking needed here, unlike
// handleDial/handleClose/handleQuitClose, which still run on their own
// goroutine.
func (s *liveSession) handleWrite(req WriteRequestMsg) {
	k := s.l.mgr.Network(req.Network)
	if k == nil {
		// Same category as errNotConnected below: nothing to write to
		// either way, so Refused=true for the same reason.
		s.trySend(outMsg{msgWriteResult, WriteResultMsg{Network: req.Network, OK: false, Refused: true, Error: "unknown network"}})
		return
	}
	err := k.WriteLine(req.Line)
	result := WriteResultMsg{Network: req.Network, OK: err == nil}
	if err != nil {
		result.Error = err.Error()
		result.Refused = errors.Is(err, errNotConnected)
	}
	s.l.log.Debug("keeper listener: sending WriteResult", "network", result.Network, "ok", result.OK, "refused", result.Refused, "err", result.Error)
	s.trySend(outMsg{msgWriteResult, result})
}

// handleBlobPush is the wire side of Keeper.PushBlob — see msgBlobPush's
// dispatch comment in readLoop for why this runs synchronously rather
// than on its own goroutine like handleDial/handleClose/handleQuitClose.
func (s *liveSession) handleBlobPush(req BlobPushMsg) {
	k := s.l.mgr.Network(req.Network)
	if k == nil {
		s.trySend(outMsg{msgBlobPushResult, BlobPushResultMsg{Network: req.Network, OK: false, Error: "unknown network"}})
		return
	}
	k.PushBlob(req.Key, req.Mode, req.Value)
	s.l.log.Debug("keeper listener: sending BlobPushResult", "network", req.Network, "ok", true)
	s.trySend(outMsg{msgBlobPushResult, BlobPushResultMsg{Network: req.Network, OK: true}})
}

// handleQuitClose is the wire side of Keeper.QuitClose — the brain's only
// path for deliberately disconnecting from a network (see
// QuitCloseRequestMsg's doc comment for why this must never be reached
// merely because the brain itself is exiting).
func (s *liveSession) handleQuitClose(req QuitCloseRequestMsg) {
	defer s.wg.Done()
	k := s.l.mgr.Network(req.Network)
	if k == nil {
		s.trySend(outMsg{msgQuitCloseResult, QuitCloseResultMsg{Network: req.Network, OK: false, Error: "unknown network"}})
		return
	}
	err := k.QuitClose(req.Line, req.Timeout)
	result := QuitCloseResultMsg{Network: req.Network, OK: err == nil}
	if err != nil {
		result.Error = err.Error()
	}
	s.l.log.Debug("keeper listener: sending QuitCloseResult", "network", result.Network, "ok", result.OK, "err", result.Error)
	s.trySend(outMsg{msgQuitCloseResult, result})
}

// startFanIn begins streaming id if it isn't already running on this
// connection — idempotent, since a Dial on a network already being
// streamed (e.g. a redial after a transient drop) must not start a second
// fan-in goroutine racing the first.
func (s *liveSession) startFanIn(id NetworkID, k *Keeper, from uint64) {
	s.mu.Lock()
	// stopped is checked in the same critical section as the wg.Add below
	// for the same reason beginWork does — see its doc comment. A Dial
	// that completes (via handleDial, itself dispatched through
	// beginWork) after teardown has already begun must not register a
	// new fan-in goroutine with s.wg once s.wg.Wait() may already be
	// running.
	if s.active[id] || s.stopped {
		s.mu.Unlock()
		return
	}
	s.active[id] = true
	s.wg.Add(1)
	s.mu.Unlock()

	go s.fanInNetwork(id, k, from)
}

func (s *liveSession) trySend(m outMsg) {
	select {
	case s.out <- m:
	case <-s.ctx.Done():
	}
}

// catchUpFromRing queues every ring entry after lastSent onto s.out.
// Returns false if the attach should stop: the ring has evicted the
// requested seq (a genuine gap — this is the one case that still kills)
// or the session context is already done. lastSent is advanced to the
// last queued seq.
func (s *liveSession) catchUpFromRing(id NetworkID, k *Keeper, lastSent *uint64) bool {
	backlog, ok := k.Since(*lastSent)
	if !ok {
		s.kill(fmt.Errorf("network %d: requested seq %d has already been evicted", id, *lastSent))
		return false
	}
	for _, e := range backlog {
		s.trySend(outMsg{msgLine, entryToLineMsg(id, e)})
		*lastSent = e.Seq
		if s.ctx.Err() != nil {
			return false
		}
	}
	return true
}

// fanInNetwork drains one network's backlog, then its live line feed and
// connect/disconnect events, into the session's shared out channel — a
// network's own goroutine, isolated from every other network's. It reports
// at most one error to kill, then returns.
//
// It does not exit on a normal disconnect (EventDisconnected with no
// further action from the brain) — SubscribeLines/Subscribe both survive a
// Keeper's Close/Dial cycle by design (see Keeper.Retire's doc comment), so
// this goroutine just keeps waiting and picks up the new epoch's lines
// automatically once the brain redials. It only exits early on: the
// connection's context ending, a ring eviction it cannot catch up from
// (kill), or its subscriptions being closed out from under it — which only
// happens via Retire, i.e. the network being permanently removed, not
// merely disconnected.
//
// Live-buffer overflow is not fatal. SubscribeLines is a small notification
// channel; the ring still holds the lines. On Overflow we resubscribe and
// Since(lastSent), the same subscribe-then-Since race window the initial
// attach already uses (duplicates by seq are skipped). Killing the attach
// here used to turn a normal WHO burst into a broken pipe on the keeper
// unix socket — TestListenerLiveBurstDoesNotKillAttach.
func (s *liveSession) fanInNetwork(id NetworkID, k *Keeper, from uint64) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.active, id)
		s.mu.Unlock()
	}()

	events, unsubEvents := k.Subscribe()
	defer unsubEvents()

	lineSub, unsubLines := k.SubscribeLines()
	defer func() { unsubLines() }()

	lastSent := from
	if !s.catchUpFromRing(id, k, &lastSent) {
		return
	}

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-lineSub.Overflow:
			// Subscribe the replacement before dropping the overflowed
			// feed so hasLineSubscribers never goes false in the window —
			// a ring push in that gap would take the no-subscriber
			// self-close path (ring overflow case 2) and drop the uplink.
			// Then Since(lastSent): a line arriving between Since and the
			// new subscription is either in the backlog or on the new
			// feed (deduped by seq).
			s.l.log.Debug("keeper listener: live subscriber overflow, catching up from ring", "network", id, "after_seq", lastSent)
			next, unsubNext := k.SubscribeLines()
			unsubLines()
			lineSub, unsubLines = next, unsubNext
			if !s.catchUpFromRing(id, k, &lastSent) {
				return
			}
		case e, ok := <-lineSub.Lines:
			if !ok {
				return // Retire: this network is gone for good
			}
			if e.Seq <= lastSent {
				continue // already sent as part of the backlog drain
			}
			s.trySend(outMsg{msgLine, entryToLineMsg(id, e)})
			lastSent = e.Seq
		case ev, ok := <-events:
			if !ok {
				return // Retire
			}
			// readLoop always calls publishLine for a connection's final
			// line before publish(EventDisconnected) for that same
			// connection (see Keeper.readLoop) — but lineSub.Lines and
			// events are two independent channels, and select's choice
			// among simultaneously-ready cases is unspecified, not FIFO
			// across them. Left alone, a disconnect published microseconds
			// after e.g. the welcome line can race ahead of it here and
			// reach the brain first. Drain whatever's already buffered on
			// Lines before forwarding the event, restoring the order
			// readLoop actually published in — the case this exists for:
			// internal/brain.Driver treats NetworkEvent{Disconnected}
			// during registration as a failure signal, so a completion
			// line reordered behind its own connection's disconnect event
			// would spuriously fail a registration that actually
			// succeeded.
		drainBeforeEvent:
			for {
				select {
				case e, ok := <-lineSub.Lines:
					if !ok {
						break drainBeforeEvent
					}
					if e.Seq > lastSent {
						s.trySend(outMsg{msgLine, entryToLineMsg(id, e)})
						lastSent = e.Seq
					}
				default:
					break drainBeforeEvent
				}
			}
			msg := NetworkEventMsg{Network: id, Kind: ev.Kind, Epoch: ev.Epoch}
			if ev.Err != nil {
				msg.Error = ev.Err.Error()
			}
			s.l.log.Debug("keeper listener: sending NetworkEvent", "network", msg.Network, "kind", msg.Kind, "epoch", msg.Epoch, "err", msg.Error)
			s.trySend(outMsg{msgNetworkEvent, msg})
		}
	}
}

func entryToLineMsg(network NetworkID, e Entry) LineMsg {
	return LineMsg{Network: network, Seq: e.Seq, Epoch: e.Epoch, Raw: []byte(e.Line), Time: e.Time}
}

func negotiateVersion(clientVersion, clientMin int) (int, error) {
	if clientMin <= 0 {
		clientMin = 1 // pre-versioning brains omit min_protocol
	}
	negotiated := clientVersion
	if negotiated > keeperMaxVersion {
		negotiated = keeperMaxVersion
	}
	if negotiated < keeperMinVersion || negotiated < clientMin {
		return 0, errors.New("keeper protocol: no compatible version")
	}
	return negotiated, nil
}
