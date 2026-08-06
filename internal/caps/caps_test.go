package caps_test

import (
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/caps"
)

func TestOffered(t *testing.T) {
	got := caps.Offered(nil)
	for _, c := range caps.AlwaysOffer {
		if !contains(got, c) {
			t.Fatalf("missing always %s in %v", c, got)
		}
	}
	for _, c := range caps.UplinkOffer {
		if contains(got, c) {
			t.Fatalf("unexpected uplink cap %s without uplink", c)
		}
	}
	up := map[string]bool{"away-notify": true, "chghost": true}
	got = caps.Offered(up)
	if !contains(got, "away-notify") || !contains(got, "chghost") {
		t.Fatal(got)
	}
	if contains(got, "extended-join") {
		t.Fatal("extended-join not on uplink")
	}
}

func TestDiff(t *testing.T) {
	before := []string{"a", "b"}
	after := []string{"b", "c", "d"}
	got := caps.Diff(before, after)
	if len(got) != 2 || got[0] != "c" || got[1] != "d" {
		t.Fatal(got)
	}
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
