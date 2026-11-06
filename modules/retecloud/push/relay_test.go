package push

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type relayRecorder struct {
	mu       sync.Mutex
	requests []RelayRequest
	auth     []string
	answers  []func(http.ResponseWriter) // one per request; the last repeats
}

func (rr *relayRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req RelayRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	rr.mu.Lock()
	rr.requests = append(rr.requests, req)
	rr.auth = append(rr.auth, r.Header.Get("Authorization")+"|"+r.Header.Get("Content-Type"))
	i := min(len(rr.requests), len(rr.answers)) - 1
	answer := rr.answers[i]
	rr.mu.Unlock()
	answer(w)
}

func status(code int, body string, headers ...string) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		for i := 0; i+1 < len(headers); i += 2 {
			w.Header().Set(headers[i], headers[i+1])
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}
}

func TestRelayPostOutcomes(t *testing.T) {
	cases := []struct {
		answer  func(http.ResponseWriter)
		outcome Outcome
		code    string
	}{
		{status(200, `{"status":"sent"}`), Sent, "sent"},
		{status(200, `{"status":"duplicate"}`), Sent, "duplicate"},
		{status(400, `{"error":"invalid_request"}`), Dropped, "invalid_request"},
		{status(401, `{"error":"invalid_relay_key"}`), Rejected, "invalid_relay_key"},
		{status(410, `{"error":"device_gone"}`), Rejected, "device_gone"},
		{status(413, `{"error":"payload_too_large"}`), Dropped, "payload_too_large"},
		{status(422, `{"error":"stale"}`), Dropped, "stale"},
		{status(429, `{"error":"rate_limited"}`, "Retry-After", "30"), Dropped, "rate_limited"},
		{status(502, `{"error":"apns_error"}`), Retry, "apns_error"},
		{status(503, `{"error":"push_unavailable"}`), Unavailable, "push_unavailable"},
	}
	for _, tc := range cases {
		rec := &relayRecorder{answers: []func(http.ResponseWriter){tc.answer}}
		srv := httptest.NewTLSServer(rec)
		r := &Relay{Client: srv.Client()}
		req := NewRelayRequest(TypeMessage, "e1", 1762430400, make([]byte, 304))
		res := r.Post(context.Background(), srv.URL+"/api/push/relay", "rrk_k", req)
		srv.Close()
		if res.Outcome != tc.outcome || res.Code != tc.code {
			t.Errorf("%s: outcome %v code %q", tc.code, res.Outcome, res.Code)
		}
		if tc.code == "rate_limited" && res.RetryAfter != 30*time.Second {
			t.Errorf("retry after %v", res.RetryAfter)
		}
		if len(rec.requests) != 1 {
			t.Fatalf("%d requests", len(rec.requests))
		}
		got := rec.requests[0]
		if got.V != 1 || got.Type != "message" || got.EventID != "e1" || got.Time != 1762430400 {
			t.Errorf("body %+v", got)
		}
		if blob, err := base64.StdEncoding.DecodeString(got.Enc); err != nil || len(blob) != 304 {
			t.Errorf("enc: %v %d", err, len(blob))
		}
		if rec.auth[0] != "Bearer rrk_k|application/json" {
			t.Errorf("headers %q", rec.auth[0])
		}
	}
}

func TestRelaySendRetriesOnlyTransientWithSameEventID(t *testing.T) {
	rec := &relayRecorder{answers: []func(http.ResponseWriter){status(502, `{"error":"apns_error"}`),
		status(502, `{"error":"apns_error"}`), status(200, `{"status":"sent"}`)}}
	srv := httptest.NewTLSServer(rec)
	defer srv.Close()
	r := &Relay{Client: srv.Client(), Backoff: []time.Duration{time.Millisecond, time.Millisecond}}
	res := r.Send(context.Background(), srv.URL, "k", NewRelayRequest(TypeMessage, "same", 1, make([]byte, 304)))
	if res.Outcome != Sent || len(rec.requests) != 3 {
		t.Fatalf("outcome %v after %d requests", res.Outcome, len(rec.requests))
	}
	for _, q := range rec.requests {
		if q.EventID != "same" {
			t.Fatalf("event ids differ: %+v", rec.requests)
		}
	}

	// At most 3 attempts.
	rec = &relayRecorder{answers: []func(http.ResponseWriter){status(502, `{"error":"apns_error"}`)}}
	srv2 := httptest.NewTLSServer(rec)
	defer srv2.Close()
	r.Client = srv2.Client()
	if res := r.Send(context.Background(), srv2.URL, "k", NewRelayRequest(TypeMessage, "x", 1, nil)); res.Outcome != Retry || len(rec.requests) != 3 {
		t.Fatalf("outcome %v after %d requests", res.Outcome, len(rec.requests))
	}

	// 429 is not retried (no backlog).
	rec = &relayRecorder{answers: []func(http.ResponseWriter){status(429, `{"error":"rate_limited"}`)}}
	srv3 := httptest.NewTLSServer(rec)
	defer srv3.Close()
	r.Client = srv3.Client()
	if res := r.Send(context.Background(), srv3.URL, "k", NewRelayRequest(TypeMessage, "y", 1, nil)); res.Outcome != Dropped || len(rec.requests) != 1 {
		t.Fatalf("outcome %v after %d requests", res.Outcome, len(rec.requests))
	}
}
