package version

import "testing"

func TestFormatVersion(t *testing.T) {
	cases := []struct {
		base, stamp, rev string
		dirty            bool
		want             string
	}{
		{base: "0.1.1", want: "0.1.1"},
		{base: "0.1.1", stamp: "0.1.1", rev: "abcdef1234deadbeef", want: "0.1.1"},
		{base: "0.1.1", stamp: "0.1.1-5-gabcdef1", want: "0.1.1-5-gabcdef1"},
		{base: "0.1.1", rev: "abcdef1234deadbeef", want: "0.1.1+abcdef1"},
		{base: "0.1.1", rev: "abc", want: "0.1.1+abc"},
		{base: "0.1.1", rev: "abcdef1234deadbeef", dirty: true, want: "0.1.1+abcdef1-dirty"},
		{base: "0.1.1", stamp: "0.1.1", rev: "abcdef1234deadbeef", dirty: true, want: "0.1.1"},
	}
	for _, c := range cases {
		got := formatVersion(c.base, c.stamp, c.rev, c.dirty)
		if got != c.want {
			t.Errorf("formatVersion(%q, %q, %q, %v)=%q, want %q",
				c.base, c.stamp, c.rev, c.dirty, got, c.want)
		}
	}
}

func TestDisplayVersionStampWinsOverVCS(t *testing.T) {
	origStamp, origVersion := stamp, Version
	t.Cleanup(func() {
		stamp, Version = origStamp, origVersion
	})
	Version = "0.1.1"
	stamp = "0.1.1-5-gabcdef1"
	if got := DisplayVersion(); got != stamp {
		t.Fatalf("DisplayVersion()=%q, want stamp %q (VCS must not be appended)", got, stamp)
	}
	stamp = "0.1.1"
	if got := DisplayVersion(); got != "0.1.1" {
		t.Fatalf("DisplayVersion()=%q, want 0.1.1 for a stamped release", got)
	}
}

func TestClassifyUpgrade(t *testing.T) {
	cases := []struct {
		running, current, min int
		want                  Upgrade
	}{
		{running: 1, current: 1, min: 1, want: UpgradeNone},
		{running: 2, current: 1, min: 1, want: UpgradeNone}, // keeper newer than this brain
		{running: 1, current: 2, min: 1, want: UpgradeShould},
		{running: 1, current: 2, min: 2, want: UpgradeMust},
		{running: 2, current: 3, min: 2, want: UpgradeShould},
		{running: 1, current: 3, min: 2, want: UpgradeMust},
	}
	for _, c := range cases {
		got := ClassifyUpgrade(c.running, c.current, c.min)
		if got != c.want {
			t.Errorf("ClassifyUpgrade(%d,%d,%d)=%s, want %s",
				c.running, c.current, c.min, got, c.want)
		}
	}
}

func TestCanUpgradeNormalizesUnversionedKeeper(t *testing.T) {
	// Current constants are all 1: a missing keeper_version (0) is
	// generation 1, so introducing versioning is not a must-upgrade.
	if KeeperVersion != 1 || MinKeeperVersion != 1 {
		t.Skipf("package constants moved (KeeperVersion=%d MinKeeperVersion=%d); adjust this test",
			KeeperVersion, MinKeeperVersion)
	}
	if got := CanUpgrade(0); got != UpgradeNone {
		t.Errorf("CanUpgrade(0)=%s, want none (unversioned keeper ≡ 1)", got)
	}
	if got := CanUpgrade(1); got != UpgradeNone {
		t.Errorf("CanUpgrade(1)=%s, want none", got)
	}
}

func TestNormalizeKeeperVersion(t *testing.T) {
	if got := NormalizeKeeperVersion(0); got != 1 {
		t.Errorf("NormalizeKeeperVersion(0)=%d, want 1", got)
	}
	if got := NormalizeKeeperVersion(-1); got != 1 {
		t.Errorf("NormalizeKeeperVersion(-1)=%d, want 1", got)
	}
	if got := NormalizeKeeperVersion(3); got != 3 {
		t.Errorf("NormalizeKeeperVersion(3)=%d, want 3", got)
	}
}

func TestUpgradeString(t *testing.T) {
	if UpgradeNone.String() != "none" || UpgradeShould.String() != "should" || UpgradeMust.String() != "must" {
		t.Fatalf("String: none=%q should=%q must=%q", UpgradeNone, UpgradeShould, UpgradeMust)
	}
}
