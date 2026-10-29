package retecloud

import (
	"bytes"
	"compress/flate"
	"encoding/json"
)

// Stream compression ("deflate-stream"), opted into on authenticate, as retecloud-api: after
// authenticate_success every frame is a binary WebSocket message holding the raw DEFLATE
// (RFC 1951) bytes of one JSON message, from one compressor for the whole connection,
// sync-flushed per message (each chunk ends 00 00 ff ff). The client keeps one inflater for the
// connection; each binary frame yields exactly one JSON object.

// compressionDeflateStream is authenticate's compression value.
const compressionDeflateStream = "deflate-stream"

type streamCompressor struct {
	w   *flate.Writer
	buf bytes.Buffer
}

func newStreamCompressor(level int) (*streamCompressor, error) {
	sc := &streamCompressor{}
	w, err := flate.NewWriter(&sc.buf, level)
	if err != nil {
		return nil, err
	}
	sc.w = w
	return sc, nil
}

// encode compresses one message into a self-delimited chunk of the stream; the slice is valid
// until the next call.
func (sc *streamCompressor) encode(msg ServerMessage) ([]byte, error) {
	payload, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	sc.buf.Reset()
	if _, err := sc.w.Write(payload); err != nil {
		return nil, err
	}
	if err := sc.w.Flush(); err != nil {
		return nil, err
	}
	return sc.buf.Bytes(), nil
}
