package module

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

type echoParams struct {
	X int `json:"x"`
}

func init() {
	registerMethod("test.echo", func(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
		p, err := decodeParams[echoParams](params)
		if err != nil {
			return nil, err
		}
		return p, nil
	}, wire.FeatureEvents)
	registerMethod("test.needs_send", func(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
		return nil, nil
	}, wire.FeatureSend)
}

func TestOverflowClosesWithoutBlocking(t *testing.T) {
	brainSide, moduleSide := net.Pipe() // unbuffered: a module that never reads blocks the writer
	defer moduleSide.Close()
	srv := &Server{log: slog.Default(), conns: map[uint64]*Conn{}}
	c := newConn(1, srv, brainSide, "m", nil)
	closedCB := make(chan struct{})
	c.OnClose(func() { close(closedCB) })
	go c.run(context.Background(), bufio.NewReader(brainSide))

	done := make(chan bool, 1)
	go func() {
		ev, _ := wire.NewEvent(wire.EventLine, wire.Empty{})
		refused := false
		for i := 0; i < QueueSize+2; i++ {
			if !c.Enqueue(ev) {
				refused = true
			}
		}
		done <- refused
	}()
	select {
	case refused := <-done:
		if !refused {
			t.Fatal("every Enqueue succeeded although the queue overflowed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Enqueue blocked")
	}
	select {
	case <-closedCB:
	case <-time.After(2 * time.Second):
		t.Fatal("connection not closed after overflow")
	}
	if c.Enqueue(wire.Event{Event: "x"}) {
		t.Fatal("Enqueue on a closed connection succeeded")
	}
}

func dispatchServer(t *testing.T) *testConn {
	t.Helper()
	_, path := startServer(t, []config.ModuleConfig{{Name: "m", Features: []string{"events"}}}, &fakeBackend{})
	tc := dialModule(t, path)
	tc.hello("m", "events")
	return tc
}

func TestDispatchUnknownMethod(t *testing.T) {
	tc := dispatchServer(t)
	if code := tc.callErr("no.such", wire.Empty{}); code != wire.CodeUnknownMethod {
		t.Fatalf("code = %q, want unknown_method", code)
	}
}

func TestDispatchNotGranted(t *testing.T) {
	tc := dispatchServer(t)
	if code := tc.callErr("test.needs_send", wire.Empty{}); code != wire.CodeNotGranted {
		t.Fatalf("code = %q, want not_granted", code)
	}
}

func TestDispatchBadParams(t *testing.T) {
	tc := dispatchServer(t)
	if code := tc.callErr("test.echo", "not an object"); code != wire.CodeBadParams {
		t.Fatalf("code = %q, want bad_params", code)
	}
}

func TestResponsesCarryRequestID(t *testing.T) {
	tc := dispatchServer(t)
	id1 := tc.send("test.echo", echoParams{X: 1})
	id2 := tc.send("test.echo", echoParams{X: 2})
	for _, want := range []struct {
		id uint64
		x  int
	}{{id1, 1}, {id2, 2}} {
		f, err := tc.read()
		if err != nil {
			t.Fatal(err)
		}
		if f.Kind != wire.KindResponse || f.Response.ID != want.id {
			t.Fatalf("frame %+v, want response %d", f, want.id)
		}
		var got echoParams
		if err := json.Unmarshal(f.Response.Result, &got); err != nil || got.X != want.x {
			t.Fatalf("result %s, want x=%d", f.Response.Result, want.x)
		}
	}
}
