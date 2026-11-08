package push

// Coalescing a flood (retecloud-api PUSH_ENCRYPTION.md, "Coalescing a flood"): one
// notification per conversation (an opaque, stable collapse id per device, session, buffer and
// sender), a 60-second cool-down per buffer after which later pushes are silent, and a cap of
// message pushes per buffer per minute whose held-back lines the conversation's next push
// reports in `skipped`. The relay cannot see the buffer or the sender, so the bouncer decides
// all three and sends the collapse id and `silent` with the request. In memory; not safe for
// concurrent use (the module's loop owns it).

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

const (
	// CoolDown: after a push in a buffer sounds, the buffer's later pushes within this window
	// are silent. The window starts at the sounding push and does not slide.
	CoolDown = 60 * time.Second
	// CapPerMinute caps message pushes, sounding or silent, per (device, buffer) per capWindow.
	CapPerMinute = 10
	capWindow    = time.Minute
	// skippedKeep: a held-back count unreported this long is forgotten.
	skippedKeep = time.Hour
	// sweepEvery: decisions between sweeps of idle state.
	sweepEvery = 256
)

// Conversation names one device's pushes from one sender in one buffer. Buffer and Sender are
// folded with the network's CASEMAPPING by the caller.
type Conversation struct {
	ClientID  string
	SessionID string
	Buffer    string
	Sender    string
}

// Decision is what to do with one qualifying line for one device.
type Decision struct {
	Send       bool
	CollapseID string // the conversation's collapse id
	Silent     bool   // inside the buffer's cool-down
	Skipped    int    // lines of this conversation held back by the cap since its last push
}

type bufferKey struct{ clientID, sessionID, buffer string }

type convKey struct {
	bufferKey
	sender string
}

func convKeyOf(c Conversation) convKey {
	return convKey{bufferKey{c.ClientID, c.SessionID, c.Buffer}, c.Sender}
}

type bufferState struct {
	lastSound time.Time
	sent      []time.Time // pushes in the last capWindow, oldest first
}

type skippedCount struct {
	n  int
	at time.Time // the last line held back
}

// Coalescer holds the cool-down, the cap and the held-back counts.
type Coalescer struct {
	secret    []byte
	buffers   map[bufferKey]*bufferState
	skipped   map[convKey]skippedCount
	decisions int
}

// NewCoalescer keys collapse ids with secret; nil makes a random one (a restart then starts
// new notifications, which is all a changed secret does).
func NewCoalescer(secret []byte) *Coalescer {
	if secret == nil {
		secret = make([]byte, 32)
		_, _ = rand.Read(secret)
	}
	return &Coalescer{secret: secret, buffers: map[bufferKey]*bufferState{}, skipped: map[convKey]skippedCount{}}
}

// CollapseID is the conversation's apns-collapse-id: the first 16 bytes, hex, of
// HMAC-SHA256(secret, client_id 0x00 session_id 0x00 buffer 0x00 sender).
func (c *Coalescer) CollapseID(conv Conversation) string {
	mac := hmac.New(sha256.New, c.secret)
	mac.Write([]byte(conv.ClientID))
	mac.Write([]byte{0})
	mac.Write([]byte(conv.SessionID))
	mac.Write([]byte{0})
	mac.Write([]byte(conv.Buffer))
	mac.Write([]byte{0})
	mac.Write([]byte(conv.Sender))
	return hex.EncodeToString(mac.Sum(nil)[:16])
}

// Decide applies the cap and the cool-down to one line of conv at now, and records it.
func (c *Coalescer) Decide(conv Conversation, now time.Time) Decision {
	if c.decisions++; c.decisions%sweepEvery == 0 {
		c.sweep(now)
	}
	ck := convKeyOf(conv)
	b := c.buffers[ck.bufferKey]
	if b == nil {
		b = &bufferState{}
		c.buffers[ck.bufferKey] = b
	}
	b.prune(now)
	if len(b.sent) >= CapPerMinute {
		s := c.skipped[ck]
		c.skipped[ck] = skippedCount{n: s.n + 1, at: now}
		return Decision{}
	}
	b.sent = append(b.sent, now)
	d := Decision{Send: true, CollapseID: c.CollapseID(conv)}
	if !b.lastSound.IsZero() && now.Sub(b.lastSound) < CoolDown {
		d.Silent = true
	} else {
		b.lastSound = now
	}
	if s, ok := c.skipped[ck]; ok {
		d.Skipped = s.n
		delete(c.skipped, ck)
	}
	return d
}

// Forget drops a device's state (it unregistered).
func (c *Coalescer) Forget(clientID string) {
	for k := range c.buffers {
		if k.clientID == clientID {
			delete(c.buffers, k)
		}
	}
	for k := range c.skipped {
		if k.clientID == clientID {
			delete(c.skipped, k)
		}
	}
}

func (b *bufferState) prune(now time.Time) {
	k := 0
	for k < len(b.sent) && now.Sub(b.sent[k]) >= capWindow {
		k++
	}
	b.sent = b.sent[k:]
}

// sweep forgets buffers whose state can no longer change a decision, and held-back counts
// older than skippedKeep.
func (c *Coalescer) sweep(now time.Time) {
	for k, b := range c.buffers {
		b.prune(now)
		if len(b.sent) == 0 && now.Sub(b.lastSound) >= CoolDown {
			delete(c.buffers, k)
		}
	}
	for k, s := range c.skipped {
		if now.Sub(s.at) > skippedKeep {
			delete(c.skipped, k)
		}
	}
}
