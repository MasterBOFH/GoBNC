package retecloud

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// errClosing ends writeLoop after a close frame.
var errClosing = errors.New("retecloud: connection closing")

// WebSocket timing and limits, as retecloud-api.
const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512 * 1024
	sendQueueSize  = 256
)

// outFrame is one queued frame; startCompression switches the connection to deflate-stream
// after it is written (the authenticate_success that accepted it).
type outFrame struct {
	msg              ServerMessage
	startCompression bool
	closeCode        int // non-zero: write a close frame with closeText, then end the connection
	closeText        string
}

// wsConn is one WebSocket. Its frames are written in queue order by one writer goroutine; its
// requests are handled one at a time, in order, on the module loop. Fields below the mutex
// are owned by the module loop.
type wsConn struct {
	id         uint64
	m          *Module
	ws         *websocket.Conn
	certSHA256 string
	remoteIP   string

	out       chan outFrame
	closeOnce sync.Once
	closed    chan struct{}
	writeDone chan struct{} // closed when writeLoop returns

	// Loop-owned.
	authed     bool
	deviceID   string                 // authenticate's client_id
	compress   bool                   // deflate-stream accepted
	clientName string                 // virtual client name on every network
	sessions   map[int64]*liteSession // followed networks (sync_session lite)
}

func newWSConn(m *Module, id uint64, ws *websocket.Conn, certSHA256, remoteIP string) *wsConn {
	return &wsConn{id: id, m: m, ws: ws, certSHA256: certSHA256, remoteIP: remoteIP,
		out: make(chan outFrame, sendQueueSize), closed: make(chan struct{}), writeDone: make(chan struct{}),
		// The pid keeps names apart from a replacement's during a hot reload.
		clientName: "p" + itoa(uint64(os.Getpid())) + "-ws" + itoa(id),
		sessions:   map[int64]*liteSession{}}
}

// send queues a frame without blocking; a full queue closes the connection with 1013 (as
// retecloud-api).
func (c *wsConn) send(msg ServerMessage) { c.queue(outFrame{msg: msg}) }

func (c *wsConn) queue(f outFrame) {
	select {
	case <-c.closed:
		return
	default:
	}
	select {
	case c.out <- f:
	default:
		c.overflow()
	}
}

// overflow ends a connection whose send queue is full (a slow consumer) with 1013 "try again
// later". The queue has no room for the close frame, so it is written directly, from its own
// goroutine: the loop never waits on a peer that is not reading (WriteControl may run
// concurrently with writeLoop's writes and waits at most writeWait for them).
func (c *wsConn) overflow() {
	c.closeOnce.Do(func() {
		close(c.closed)
		go func() {
			_ = c.ws.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(closeTryAgainLater, "try again later"), time.Now().Add(writeWait))
			_ = c.ws.Close()
		}()
	})
}

// closeWith ends the connection with a close frame, after the frames already queued.
func (c *wsConn) closeWith(code int, text string) {
	c.queue(outFrame{closeCode: code, closeText: text})
}

func (c *wsConn) close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		_ = c.ws.Close()
	})
}

// readLoop hands each client frame to the module loop, in order.
func (c *wsConn) readLoop() {
	defer c.close()
	c.ws.SetReadLimit(maxMessageSize)
	_ = c.ws.SetReadDeadline(time.Now().Add(pongWait))
	c.ws.SetPongHandler(func(string) error {
		return c.ws.SetReadDeadline(time.Now().Add(pongWait))
	})
	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			return
		}
		var msg ClientMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			c.send(errorFrame("invalid_message", msg.RequestID, "Invalid message format"))
			continue
		}
		if !c.m.post(func() { c.m.handle(c, &msg) }) {
			return
		}
	}
}

// writeLoop writes queued frames and keepalive pings until the connection closes.
func (c *wsConn) writeLoop() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	defer close(c.writeDone)
	defer c.close()
	var stream *streamCompressor
	for {
		select {
		case <-c.closed:
			return
		case f := <-c.out:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.writeFrame(f, &stream); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// writeFrame writes one frame: JSON text (newline-terminated, as retecloud-api's encoder), or a
// binary chunk of the connection's deflate stream once compression started.
func (c *wsConn) writeFrame(f outFrame, stream **streamCompressor) error {
	if f.closeCode != 0 {
		_ = c.ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(f.closeCode, f.closeText),
			time.Now().Add(writeWait))
		return errClosing
	}
	if *stream != nil {
		chunk, err := (*stream).encode(f.msg)
		if err != nil {
			return err
		}
		return c.ws.WriteMessage(websocket.BinaryMessage, chunk)
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(f.msg); err != nil {
		return err
	}
	if err := c.ws.WriteMessage(websocket.TextMessage, buf.Bytes()); err != nil {
		return err
	}
	if f.startCompression {
		sc, err := newStreamCompressor(c.m.compressionLevel())
		if err != nil {
			return err
		}
		c.ws.EnableWriteCompression(false)
		*stream = sc
	}
	return nil
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
