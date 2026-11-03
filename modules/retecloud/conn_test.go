package retecloud

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestSendQueueOverflowClosesWith1013: a connection whose send queue is full (a slow consumer)
// is closed with 1013 "try again later", not dropped without a close frame.
func TestSendQueueOverflowClosesWith1013(t *testing.T) {
	serverSide := make(chan *websocket.Conn, 1)
	hold := make(chan struct{})
	defer close(hold)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		serverSide <- ws
		<-hold
	}))
	defer srv.Close()
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	// No writeLoop: nothing drains the queue, so the next frame past its size overflows it.
	c := newWSConn(&Module{}, 1, <-serverSide, "", "")
	for i := 0; i <= sendQueueSize; i++ {
		c.send(ServerMessage{Type: "irc_line"})
	}
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, err = client.ReadMessage()
	var ce *websocket.CloseError
	if !errors.As(err, &ce) {
		t.Fatalf("connection ended with %v, not a close frame", err)
	}
	if ce.Code != 1013 || ce.Text != "try again later" {
		t.Fatalf("close = %d %q, want 1013 try again later", ce.Code, ce.Text)
	}
	select {
	case <-c.closed:
	default:
		t.Fatal("connection not marked closed after the overflow")
	}
}
