package module

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Handler serves one method. It returns the result (marshalled into the response), or an error:
// a *wire.Error is sent as-is, anything else as "internal". A handler that must place its
// response in the connection's frame order itself (events.subscribe: before the first event)
// enqueues it with Respond and returns errResponded.
type Handler func(ctx context.Context, c *Conn, params json.RawMessage) (any, error)

type methodEntry struct {
	features []string // all must be granted
	handler  Handler
}

var methods = map[string]methodEntry{}

// registerMethod adds a method, callable when every listed feature is granted. Called from the
// feature files' init functions.
func registerMethod(method string, handler Handler, features ...string) {
	if _, dup := methods[method]; dup {
		panic("module: method registered twice: " + method)
	}
	methods[method] = methodEntry{features: features, handler: handler}
}

// errResponded tells dispatch the handler already enqueued its response.
var errResponded = errors.New("module: response already enqueued")

type requestIDKey struct{}

// RequestID returns the id of the request a handler is serving.
func RequestID(ctx context.Context) uint64 {
	id, _ := ctx.Value(requestIDKey{}).(uint64)
	return id
}

// Respond enqueues the success response for the request served under ctx.
func (c *Conn) Respond(ctx context.Context, result any) bool {
	resp, err := resultResponse(RequestID(ctx), result)
	if err != nil {
		return c.Enqueue(errorResponse(RequestID(ctx), wire.Errorf(wire.CodeInternal, "%v", err)))
	}
	return c.Enqueue(resp)
}

// dispatch runs a request; ok is false when the handler already responded.
func (s *Server) dispatch(ctx context.Context, c *Conn, req *wire.Request) (wire.Response, bool) {
	m, found := methods[req.Method]
	if !found || req.Method == wire.MethodHello {
		return errorResponse(req.ID, wire.Errorf(wire.CodeUnknownMethod, "%s", req.Method)), true
	}
	for _, f := range m.features {
		if !c.Granted(f) {
			return errorResponse(req.ID, wire.Errorf(wire.CodeNotGranted, "%s needs feature %q", req.Method, f)), true
		}
	}
	params := req.Params
	if len(params) == 0 {
		params = json.RawMessage("{}")
	}
	result, err := m.handler(context.WithValue(ctx, requestIDKey{}, req.ID), c, params)
	if errors.Is(err, errResponded) {
		return wire.Response{}, false
	}
	if err != nil {
		var we *wire.Error
		if !errors.As(err, &we) {
			s.log.Error("module: method failed", "module", c.module, "method", req.Method, "err", err)
			we = &wire.Error{Code: wire.CodeInternal, Message: err.Error()}
		}
		return errorResponse(req.ID, we), true
	}
	resp, err := resultResponse(req.ID, result)
	if err != nil {
		return errorResponse(req.ID, wire.Errorf(wire.CodeInternal, "%v", err)), true
	}
	return resp, true
}

// decodeParams unmarshals params into T, mapping a decode error to bad_params.
func decodeParams[T any](params json.RawMessage) (T, error) {
	var v T
	if err := json.Unmarshal(params, &v); err != nil {
		return v, wire.Errorf(wire.CodeBadParams, "%v", err)
	}
	return v, nil
}

func resultResponse(id uint64, result any) (wire.Response, error) {
	if result == nil {
		result = wire.Empty{}
	}
	b, err := json.Marshal(result)
	if err != nil {
		return wire.Response{}, fmt.Errorf("marshal result: %w", err)
	}
	return wire.Response{ID: id, Result: b}, nil
}

func errorResponse(id uint64, e *wire.Error) wire.Response {
	return wire.Response{ID: id, Error: e}
}
