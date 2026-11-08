package push

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"
)

// RelayRequest is the body of POST /api/push/relay (PUBLIC_API.md).
type RelayRequest struct {
	V       int    `json:"v"`
	Type    string `json:"type"`
	EventID string `json:"event_id"`
	Time    int64  `json:"time"`
	Enc     string `json:"enc"`
	// A message's conversation and cool-down (coalesce.go); absent: the relay collapses on the
	// event id and the push sounds.
	CollapseID string `json:"collapse_id,omitempty"`
	Silent     bool   `json:"silent,omitempty"`
}

// NewRelayRequest wraps a sealed blob.
func NewRelayRequest(typ, eventID string, t int64, blob []byte) RelayRequest {
	return RelayRequest{V: 1, Type: typ, EventID: eventID, Time: t, Enc: base64.StdEncoding.EncodeToString(blob)}
}

// Outcome is what the bouncer does with a relay answer.
type Outcome int

const (
	Sent        Outcome = iota // 200 sent / duplicate
	Dropped                    // 400, 413, 422, 429, or an unknown answer: the push is dropped
	Rejected                   // 401, 410: the key is dead; stop pushing to the device
	Retry                      // 502 or a transport failure: retry with the same event_id
	Unavailable                // 503: push is not configured on the relay; stop for an hour
)

func (o Outcome) String() string {
	switch o {
	case Sent:
		return "sent"
	case Dropped:
		return "dropped"
	case Rejected:
		return "rejected"
	case Retry:
		return "retry"
	case Unavailable:
		return "unavailable"
	}
	return "unknown"
}

// RelayResult is one relay answer.
type RelayResult struct {
	Outcome    Outcome
	Status     int    // HTTP status; 0 on a transport failure
	Code       string // the body's "error" (or "status" on 200)
	RetryAfter time.Duration
	Err        error // transport failure
}

// Relay posts sealed pushes to ReteCloud's relay.
type Relay struct {
	Client *http.Client
	// Backoff is the wait before each retry of a Retry outcome; len(Backoff)+1 attempts in all.
	Backoff []time.Duration
}

// DefaultBackoff gives 3 attempts within 5 minutes (PUBLIC_API.md: 502).
var DefaultBackoff = []time.Duration{15 * time.Second, 90 * time.Second}

// Post makes one relay request.
func (r *Relay) Post(ctx context.Context, url, key string, req RelayRequest) RelayResult {
	body, err := json.Marshal(req)
	if err != nil {
		return RelayResult{Outcome: Dropped, Err: err}
	}
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return RelayResult{Outcome: Dropped, Err: err}
	}
	hr.Header.Set("Authorization", "Bearer "+key)
	hr.Header.Set("Content-Type", "application/json")
	client := r.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(hr)
	if err != nil {
		return RelayResult{Outcome: Retry, Err: err}
	}
	defer resp.Body.Close()
	var answer struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&answer)
	res := RelayResult{Status: resp.StatusCode, Code: answer.Error}
	switch resp.StatusCode {
	case http.StatusOK:
		res.Outcome, res.Code = Sent, answer.Status
	case http.StatusUnauthorized, http.StatusGone:
		res.Outcome = Rejected
	case http.StatusBadGateway:
		res.Outcome = Retry
	case http.StatusServiceUnavailable:
		res.Outcome = Unavailable
	case http.StatusTooManyRequests:
		res.Outcome = Dropped // no backlog
		if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s > 0 {
			res.RetryAfter = time.Duration(s) * time.Second
		}
	default:
		res.Outcome = Dropped
	}
	return res
}

// Send posts req, retrying a Retry outcome after each Backoff wait with the same event_id. It
// returns the last answer (or a Retry with ctx's error when ctx ended first).
func (r *Relay) Send(ctx context.Context, url, key string, req RelayRequest) RelayResult {
	res := r.Post(ctx, url, key, req)
	for _, wait := range r.Backoff {
		if res.Outcome != Retry {
			return res
		}
		select {
		case <-ctx.Done():
			return RelayResult{Outcome: Retry, Err: ctx.Err()}
		case <-time.After(wait):
		}
		res = r.Post(ctx, url, key, req)
	}
	return res
}
