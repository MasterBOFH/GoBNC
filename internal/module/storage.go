package module

import (
	"context"
	"encoding/json"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func init() {
	registerMethod(wire.MethodStorageGet, handleStorageGet, wire.FeatureStorage)
	registerMethod(wire.MethodStorageSet, handleStorageSet, wire.FeatureStorage)
	registerMethod(wire.MethodStorageDelete, handleStorageDelete, wire.FeatureStorage)
	registerMethod(wire.MethodStorageList, handleStorageList, wire.FeatureStorage)
}

// Storage limits per key and value.
const (
	maxStorageKey   = 256
	maxStorageValue = 64 << 10
)

func checkStorageKey(key string) error {
	if key == "" || len(key) > maxStorageKey {
		return wire.Errorf(wire.CodeBadParams, "key must be 1..%d bytes", maxStorageKey)
	}
	return nil
}

// Every storage method is namespaced by the connection's module name: a module only ever sees
// its own keys.

func handleStorageGet(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.StorageKeyParams](params)
	if err != nil {
		return nil, err
	}
	if err := checkStorageKey(p.Key); err != nil {
		return nil, err
	}
	v, found, err := c.srv.backend.Store().ModuleKVGet(ctx, c.Module(), p.Key)
	return wire.StorageGetResult{Value: v, Found: found}, err
}

func handleStorageSet(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.StorageSetParams](params)
	if err != nil {
		return nil, err
	}
	if err := checkStorageKey(p.Key); err != nil {
		return nil, err
	}
	if len(p.Value) > maxStorageValue {
		return nil, wire.Errorf(wire.CodeBadParams, "value larger than %d bytes", maxStorageValue)
	}
	return nil, c.srv.backend.Store().ModuleKVSet(ctx, c.Module(), p.Key, p.Value)
}

func handleStorageDelete(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.StorageKeyParams](params)
	if err != nil {
		return nil, err
	}
	if err := checkStorageKey(p.Key); err != nil {
		return nil, err
	}
	return nil, c.srv.backend.Store().ModuleKVDelete(ctx, c.Module(), p.Key)
}

func handleStorageList(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.StorageListParams](params)
	if err != nil {
		return nil, err
	}
	keys, err := c.srv.backend.Store().ModuleKVList(ctx, c.Module(), p.Prefix)
	return wire.StorageListResult{Keys: keys}, err
}
