package module

import (
	"strings"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func TestStorageNamespacedByModule(t *testing.T) {
	_, db, _, _ := historyFixture(t, "storage")
	be := &fakeBackend{ready: true, db: db}
	_, path := startServer(t, []config.ModuleConfig{{Name: "a", Features: []string{"storage"}}, {Name: "b", Features: []string{"storage"}}}, be)
	a, b := dialModule(t, path), dialModule(t, path)
	a.hello("a", "storage")
	b.hello("b", "storage")

	a.call(wire.MethodStorageSet, wire.StorageSetParams{Key: "relay/key", Value: "secret-a"}, nil)
	var got wire.StorageGetResult
	b.call(wire.MethodStorageGet, wire.StorageKeyParams{Key: "relay/key"}, &got)
	if got.Found {
		t.Fatalf("module b read module a's key: %+v", got)
	}
	a.call(wire.MethodStorageGet, wire.StorageKeyParams{Key: "relay/key"}, &got)
	if !got.Found || got.Value != "secret-a" {
		t.Fatalf("module a's own key = %+v", got)
	}
	var list wire.StorageListResult
	a.call(wire.MethodStorageList, wire.StorageListParams{Prefix: "relay/"}, &list)
	if len(list.Keys) != 1 || list.Keys[0] != "relay/key" {
		t.Fatalf("list = %+v", list)
	}
	a.call(wire.MethodStorageDelete, wire.StorageKeyParams{Key: "relay/key"}, nil)
	a.call(wire.MethodStorageGet, wire.StorageKeyParams{Key: "relay/key"}, &got)
	if got.Found {
		t.Fatal("deleted key still found")
	}
}

func TestStorageLimits(t *testing.T) {
	tc, _, _, _ := historyFixture(t, "storage")
	if code := tc.callErr(wire.MethodStorageSet, wire.StorageSetParams{Key: "k", Value: strings.Repeat("x", 64<<10+1)}); code != wire.CodeBadParams {
		t.Fatalf("oversize value: code %q", code)
	}
	if code := tc.callErr(wire.MethodStorageSet, wire.StorageSetParams{Key: strings.Repeat("k", 257), Value: "v"}); code != wire.CodeBadParams {
		t.Fatalf("oversize key: code %q", code)
	}
	if code := tc.callErr(wire.MethodStorageSet, wire.StorageSetParams{Key: "k", Value: strings.Repeat("x", 64<<10)}); code != "" {
		t.Fatalf("value at the limit: code %q", code)
	}
}
