package keeper

import (
	"reflect"
	"testing"
)

func TestBlobStoreAppendAccumulatesInOrder(t *testing.T) {
	b := newBlobStore()
	b.Push("isupport", BlobModeAppend, []byte("CHANTYPES=#"))
	b.Push("isupport", BlobModeAppend, []byte("NICKLEN=30"))

	snap := b.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("Snapshot len=%d, want 1", len(snap))
	}
	want := [][]byte{[]byte("CHANTYPES=#"), []byte("NICKLEN=30")}
	if !reflect.DeepEqual(snap[0].Values, want) {
		t.Fatalf("Values=%v, want %v", snap[0].Values, want)
	}
}

func TestBlobStoreReplaceIsLatestWins(t *testing.T) {
	b := newBlobStore()
	b.Push("self-nick", BlobModeReplace, []byte("alice"))
	b.Push("self-nick", BlobModeReplace, []byte("alice2"))

	snap := b.Snapshot()
	if len(snap) != 1 || len(snap[0].Values) != 1 || string(snap[0].Values[0]) != "alice2" {
		t.Fatalf("Snapshot=%+v, want single entry \"alice2\"", snap)
	}
}

func TestBlobStoreDeleteRemovesKey(t *testing.T) {
	b := newBlobStore()
	b.Push("channel:#foo", BlobModeReplace, []byte("key123"))
	b.Push("channel:#bar", BlobModeReplace, []byte(""))
	b.Push("channel:#foo", BlobModeDelete, nil)

	snap := b.Snapshot()
	if len(snap) != 1 || snap[0].Key != "channel:#bar" {
		t.Fatalf("Snapshot=%+v, want only channel:#bar left", snap)
	}
}

func TestBlobStoreDeleteUnknownKeyIsNoop(t *testing.T) {
	b := newBlobStore()
	b.Push("channel:#foo", BlobModeDelete, nil) // never pushed
	if snap := b.Snapshot(); len(snap) != 0 {
		t.Fatalf("Snapshot=%+v, want empty", snap)
	}
}

func TestBlobStoreClearRemovesEverything(t *testing.T) {
	b := newBlobStore()
	b.Push("isupport", BlobModeAppend, []byte("CHANTYPES=#"))
	b.Push("self-nick", BlobModeReplace, []byte("alice"))
	b.Clear()
	if snap := b.Snapshot(); len(snap) != 0 {
		t.Fatalf("Snapshot after Clear=%+v, want empty", snap)
	}
	// Clear must leave the store usable, not just empty once.
	b.Push("self-nick", BlobModeReplace, []byte("bob"))
	if snap := b.Snapshot(); len(snap) != 1 || string(snap[0].Values[0]) != "bob" {
		t.Fatalf("Snapshot after post-Clear push=%+v, want single \"bob\" entry", snap)
	}
}

func TestBlobStoreSnapshotOrderIsFirstPushOrder(t *testing.T) {
	b := newBlobStore()
	b.Push("z", BlobModeReplace, []byte("1"))
	b.Push("a", BlobModeReplace, []byte("2"))
	b.Push("z", BlobModeReplace, []byte("3")) // re-push must not move its position
	snap := b.Snapshot()
	if len(snap) != 2 || snap[0].Key != "z" || snap[1].Key != "a" {
		t.Fatalf("Snapshot order=%+v, want [z a]", snap)
	}
}
