package push

import (
	"encoding/json"
	"strings"
	"testing"
)

// A3033: `skipped` is the last plaintext field and absent when 0, so the shared vectors (none
// carry it) still match byte for byte.
func TestPlaintextSkipped(t *testing.T) {
	m := Message{EID: "e", Src: "gobnc", SessionID: "net-3", Network: "N", Buffer: "#b", Kind: "dm", Sender: "s",
		Text: "t", Time: 5, ID: 9}
	pt, _ := MessagePlaintext(m)
	if strings.Contains(string(pt), "skipped") {
		t.Fatalf("skipped on a zero count: %s", pt)
	}
	m.Skipped = 4
	pt, _ = MessagePlaintext(m)
	if got := strings.TrimRight(string(pt), " "); !strings.HasSuffix(got, `,"time":5,"id":9,"skipped":4}`) {
		t.Fatalf("got %s", got)
	}
}

// A3033: collapse_id and silent go to the relay only when set.
func TestRelayRequestCoalescingFields(t *testing.T) {
	req := NewRelayRequest(TypeMessage, "e1", 5, make([]byte, 304))
	b, _ := json.Marshal(req)
	if strings.Contains(string(b), "collapse_id") || strings.Contains(string(b), "silent") {
		t.Fatalf("unset fields sent: %s", b)
	}
	req.CollapseID, req.Silent = "conv1", true
	b, _ = json.Marshal(req)
	if !strings.HasSuffix(string(b), `,"collapse_id":"conv1","silent":true}`) {
		t.Fatalf("got %s", b)
	}
}
