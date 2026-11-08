package push

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

var coalesceT0 = time.Unix(1762430400, 0)

func testConv(client, buffer, sender string) Conversation {
	return Conversation{ClientID: client, SessionID: "net-1", Buffer: buffer, Sender: sender}
}

func TestCollapseIDIsStableOpaqueAndPerDevice(t *testing.T) {
	c := NewCoalescer([]byte("secret one"))
	a := c.CollapseID(testConv("phone", "#rete", "alice"))
	if a != c.CollapseID(testConv("phone", "#rete", "alice")) {
		t.Fatal("not stable")
	}
	if _, err := hex.DecodeString(a); len(a) != 32 || err != nil {
		t.Fatalf("collapse id %q: want 32 hex characters", a)
	}
	for _, other := range []Conversation{
		testConv("tablet", "#rete", "alice"),
		testConv("phone", "#other", "alice"),
		testConv("phone", "#rete", "bob"),
		{ClientID: "phone", SessionID: "net-2", Buffer: "#rete", Sender: "alice"},
		testConv("phone", "#re", "tealice"), // field boundaries count
	} {
		if c.CollapseID(other) == a {
			t.Errorf("%+v shares the collapse id", other)
		}
	}
	for _, name := range []string{"phone", "rete", "alice", "net-1"} {
		if strings.Contains(a, name) {
			t.Errorf("collapse id %q shows %q", a, name)
		}
	}
	if NewCoalescer([]byte("secret two")).CollapseID(testConv("phone", "#rete", "alice")) == a {
		t.Fatal("the secret does not key the id")
	}
	if NewCoalescer(nil).CollapseID(testConv("phone", "#rete", "alice")) == a {
		t.Fatal("a nil secret must be random")
	}
}

func TestCoolDownOnlyFirstPushPerBufferSounds(t *testing.T) {
	c := NewCoalescer([]byte("k"))
	alice, bob := testConv("phone", "#rete", "alice"), testConv("phone", "#rete", "bob")
	steps := []struct {
		conv   Conversation
		at     time.Duration
		silent bool
	}{
		{alice, 0, false},
		{alice, 10 * time.Second, true},
		{bob, 20 * time.Second, true},    // the cool-down is per buffer, not per sender
		{alice, 59 * time.Second, true},  // still inside the window
		{alice, 60 * time.Second, false}, // the window does not slide: 60 s after the sound
		{alice, 61 * time.Second, true},
		{testConv("phone", "#other", "alice"), 62 * time.Second, false}, // another buffer
		{testConv("tablet", "#rete", "alice"), 63 * time.Second, false}, // another device
	}
	for i, s := range steps {
		d := c.Decide(s.conv, coalesceT0.Add(s.at))
		if !d.Send || d.Silent != s.silent || d.CollapseID != c.CollapseID(s.conv) {
			t.Fatalf("step %d: %+v, want send, silent=%v", i, d, s.silent)
		}
	}
}

func TestCapPerBufferPerMinuteCarriesSkipped(t *testing.T) {
	c := NewCoalescer([]byte("k"))
	alice, bob := testConv("phone", "#rete", "alice"), testConv("phone", "#rete", "bob")
	for i := 0; i < CapPerMinute; i++ {
		if d := c.Decide(alice, coalesceT0.Add(time.Duration(i)*time.Second)); !d.Send || d.Skipped != 0 {
			t.Fatalf("push %d: %+v", i, d)
		}
	}
	for i := 0; i < 3; i++ {
		if d := c.Decide(alice, coalesceT0.Add(20*time.Second)); d.Send {
			t.Fatalf("over the cap sent: %+v", d)
		}
	}
	if d := c.Decide(bob, coalesceT0.Add(21*time.Second)); d.Send {
		t.Fatalf("the cap is per buffer: %+v", d)
	}
	d := c.Decide(alice, coalesceT0.Add(60*time.Second))
	if !d.Send || d.Skipped != 3 || d.Silent {
		t.Fatalf("after the window: %+v, want sent with skipped 3, sounding", d)
	}
	if d := c.Decide(bob, coalesceT0.Add(61*time.Second)); !d.Send || d.Skipped != 1 || !d.Silent {
		t.Fatalf("bob: %+v, want sent with skipped 1, silent", d)
	}
	if d := c.Decide(alice, coalesceT0.Add(62*time.Second)); !d.Send || d.Skipped != 0 {
		t.Fatalf("skipped is carried once: %+v", d)
	}
}

func TestCoalescerForgetsADevice(t *testing.T) {
	c := NewCoalescer([]byte("k"))
	for i := 0; i < CapPerMinute+1; i++ {
		c.Decide(testConv("phone", "#rete", "alice"), coalesceT0)
	}
	c.Decide(testConv("tablet", "#rete", "alice"), coalesceT0)
	c.Forget("phone")
	if len(c.buffers) != 1 || len(c.skipped) != 0 {
		t.Fatalf("after Forget: %d buffers, %d skipped", len(c.buffers), len(c.skipped))
	}
	if d := c.Decide(testConv("phone", "#rete", "alice"), coalesceT0); !d.Send || d.Silent || d.Skipped != 0 {
		t.Fatalf("a forgotten device starts afresh: %+v", d)
	}
}

func TestCoalescerSweepsIdleState(t *testing.T) {
	c := NewCoalescer([]byte("k"))
	c.Decide(testConv("phone", "#rete", "alice"), coalesceT0)
	c.skipped[convKeyOf(testConv("phone", "#rete", "bob"))] = skippedCount{n: 2, at: coalesceT0}
	c.sweep(coalesceT0.Add(30 * time.Second))
	if len(c.buffers) != 1 || len(c.skipped) != 1 {
		t.Fatal("live state swept")
	}
	c.sweep(coalesceT0.Add(skippedKeep + time.Second))
	if len(c.buffers) != 0 || len(c.skipped) != 0 {
		t.Fatalf("idle state kept: %d buffers, %d skipped", len(c.buffers), len(c.skipped))
	}
}
