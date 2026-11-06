package config

import (
	"testing"
	"time"
)

func TestWHOPollInterval(t *testing.T) {
	for minutes, want := range map[int]time.Duration{
		0:  DefaultWHOPollMinutes * time.Minute,
		-1: 0,
		60: time.Hour,
	} {
		if got := (Config{WHOPollMinutes: minutes}).WHOPollInterval(); got != want {
			t.Errorf("who_poll_minutes %d -> %v, want %v", minutes, got, want)
		}
	}
	if DefaultWHOPollMinutes <= 30 {
		t.Fatal("the default must be longer than Rete's own 30 minutes per channel")
	}
}

func TestWHOSeedPace(t *testing.T) {
	for seconds, want := range map[int]time.Duration{
		0:  DefaultWHOSeedSeconds * time.Second,
		-1: WHOPollPace,
		30: 30 * time.Second,
	} {
		if got := (Config{WHOSeedSeconds: seconds}).WHOSeedPace(); got != want {
			t.Errorf("who_seed_seconds %d -> %v, want %v", seconds, got, want)
		}
	}
	if DefaultWHOSeedSeconds != 10 {
		t.Fatal("the owner's seed pace is one channel per 10 s")
	}
}
