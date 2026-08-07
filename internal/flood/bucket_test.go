package flood

import (
	"context"
	"testing"
	"time"
)

func TestByteBucketDisabled(t *testing.T) {
	b := NewByteBucket(0, 0)
	if b.Enabled() {
		t.Fatal("expected disabled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Take(ctx, 1<<20); err != nil {
		t.Fatal(err)
	}
}

func TestByteBucketBurstThenWait(t *testing.T) {
	b := NewByteBucket(100, 100) // 100 byte/s
	ctx := context.Background()
	start := time.Now()
	if err := b.Take(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Fatal("burst should be immediate")
	}
	start = time.Now()
	if err := b.Take(ctx, 50); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed < 400*time.Millisecond {
		t.Fatalf("expected ~500ms wait for 50 bytes @ 100B/s, got %v", elapsed)
	}
	if elapsed > 900*time.Millisecond {
		t.Fatalf("wait too long: %v", elapsed)
	}
}

func TestByteBucketConfigureLive(t *testing.T) {
	b := NewByteBucket(10, 10)
	b.Configure(0, 0)
	if b.Enabled() {
		t.Fatal("disabled after configure")
	}
}
