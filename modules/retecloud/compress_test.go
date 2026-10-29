package retecloud

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"io"
	"testing"
	"time"
)

func TestStreamCompressorChunksInflateAcrossMessages(t *testing.T) {
	sc, err := newStreamCompressor(1)
	if err != nil {
		t.Fatal(err)
	}
	pr, pw := io.Pipe()
	dec := json.NewDecoder(flate.NewReader(pr))
	got := make(chan ServerMessage, 3)
	go func() {
		for {
			var m ServerMessage
			if dec.Decode(&m) != nil {
				close(got)
				return
			}
			got <- m
		}
	}()
	for i, typ := range []string{"a", "b", "c"} {
		chunk, err := sc.encode(ServerMessage{Type: typ, RequestID: "r"})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasSuffix(chunk, []byte{0, 0, 0xff, 0xff}) {
			t.Fatalf("chunk %d does not end with a sync flush", i)
		}
		if _, err := pw.Write(append([]byte(nil), chunk...)); err != nil {
			t.Fatal(err)
		}
		select {
		case m := <-got:
			if m.Type != typ {
				t.Fatalf("message %d = %+v", i, m)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("message %d never inflated from its chunk alone", i)
		}
	}
	pw.Close()
}

func TestCompressionNegotiatedOnAuthenticate(t *testing.T) {
	e := newTestEnv(t, "undernet")
	c := e.dial(false)
	rid := c.request("authenticate", map[string]any{"password": testPassword, "client_id": "ios-z", "compression": "deflate-stream"}, "")
	f := c.waitFor(0, 5*time.Second, reply(rid, "authenticate_success"))
	if f["data"].(map[string]any)["compression"] != "deflate-stream" {
		t.Fatalf("authenticate_success = %v", f)
	}
	rid = c.request("create_session", map[string]any{}, "")
	c.waitFor(0, 5*time.Second, reply(rid, "create_session_error"))
	rid = c.request("create_session", map[string]any{}, "")
	c.waitFor(0, 5*time.Second, reply(rid, "create_session_error"))
	if c.text.Load() != 1 || c.binary.Load() != 2 {
		t.Fatalf("text frames %d, binary %d; want authenticate_success as text, then binary", c.text.Load(), c.binary.Load())
	}
}

func TestNoCompressionWithoutOptIn(t *testing.T) {
	e := newTestEnv(t, "undernet")
	c := e.dial(false)
	if data := c.authenticate("ios-plain"); data["compression"] != nil {
		t.Fatalf("compression without asking: %v", data)
	}
	rid := c.request("create_session", map[string]any{}, "")
	c.waitFor(0, 5*time.Second, reply(rid, "create_session_error"))
	if c.binary.Load() != 0 || c.text.Load() != 2 {
		t.Fatalf("text %d binary %d, want 2 text", c.text.Load(), c.binary.Load())
	}
}

func TestCompressionLevelNegativeNotOffered(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.mod.cfg.CompressionLevel = -1
	c := e.dial(false)
	rid := c.request("authenticate", map[string]any{"password": testPassword, "client_id": "ios-n", "compression": "deflate-stream"}, "")
	if f := c.waitFor(0, 5*time.Second, reply(rid, "authenticate_success")); f["data"].(map[string]any)["compression"] != nil {
		t.Fatalf("compression offered with level -1: %v", f)
	}
}
