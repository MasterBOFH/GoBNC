package keeper

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"
)

// TestQuitCloseAllSendsQuitToEveryConnectedNetwork proves the keeper-exit
// counterpart to Driver.QuitNetwork: every connected network gets a real
// QUIT line before its socket closes, in one call, and a network the
// Manager knows about but never actually connected is a silent no-op —
// not a panic, not a block.
func TestQuitCloseAllSendsQuitToEveryConnectedNetwork(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)

	k1, srv1, conn1 := dialedTestNetwork(t, mgr, testNetID, 4096)
	k2, srv2, conn2 := dialedTestNetwork(t, mgr, testNetID2, 4096)
	_ = mgr.EnsureNetwork(testNetID3) // known, never dialed — must not block or panic

	_ = k1
	_ = k2

	start := time.Now()
	mgr.QuitCloseAll("test shutdown", 2*time.Second, 5*time.Second)
	elapsed := time.Since(start)
	if elapsed > 3*time.Second {
		t.Fatalf("QuitCloseAll took %s, want well under its 5s overall bound", elapsed)
	}

	for i, args := range []struct {
		srv  *fakeServer
		conn net.Conn
	}{
		{srv1, conn1},
		{srv2, conn2},
	} {
		_ = args.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		r := bufio.NewReader(args.conn)
		got, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("network %d: read from uplink: %v", i, err)
		}
		if got := strings.TrimRight(got, "\r\n"); got != "QUIT :test shutdown" {
			t.Fatalf("network %d: uplink received %q, want %q", i, got, "QUIT :test shutdown")
		}
		args.srv.close()
	}

	if st, _ := k1.State(); st != NotConnected {
		t.Fatalf("network 1 state after QuitCloseAll=%v, want NotConnected", st)
	}
	if st, _ := k2.State(); st != NotConnected {
		t.Fatalf("network 2 state after QuitCloseAll=%v, want NotConnected", st)
	}
}

// TestQuitCloseAllBoundedByOverallTimeout is a shape/regression test for
// the outer bound, reported honestly as weaker than the revert-and-confirm
// bar the rest of this session held to: with perNetworkTimeout set far
// larger than overallTimeout, QuitCloseAll still returns fast. Attempted
// revert-and-confirm on the `select`/`time.After(overallTimeout)` guard
// (internal/keeper/manager.go) and could not get it to fail — a single
// short QUIT line fits in the OS send buffer virtually instantly whether
// or not the peer reads it, and Keeper.Close() closing the underlying
// net.Conn unblocks the read loop promptly on its own, so with only one
// real, working fake server this test passes whether or not the outer
// `select` is even present; there was no cheap, reliable way found to
// force a genuinely stuck QuitClose call without adding TCP-buffer-filling
// complexity this test isn't worth the fragility of. The `select` guard is
// kept because it's correct by construction and free to have, not because
// this test proves it's load-bearing.
func TestQuitCloseAllBoundedByOverallTimeout(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
	_, srv, _ := dialedTestNetwork(t, mgr, testNetID, 4096)
	defer srv.close()

	start := time.Now()
	mgr.QuitCloseAll("test shutdown", 5*time.Second, 1*time.Millisecond)
	elapsed := time.Since(start)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("QuitCloseAll took %s with overallTimeout=1ms and perNetworkTimeout=5s — the outer bound did not apply", elapsed)
	}
}

// TestQuitCloseAllSendsBRBToResumableNetworks: the one blob key the keeper
// reads (BlobKeyResumable, by presence only) turns the shutdown QUIT into
// BRB for that network, leaving every other network's QUIT untouched.
func TestQuitCloseAllSendsBRBToResumableNetworks(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)

	k1, srv1, conn1 := dialedTestNetwork(t, mgr, testNetID, 4096)
	_, srv2, conn2 := dialedTestNetwork(t, mgr, testNetID2, 4096)
	k1.PushBlob(BlobKeyResumable, BlobModeReplace, []byte("1"))
	if !k1.Resumable() {
		t.Fatal("k1 not resumable after pushing the key")
	}

	mgr.QuitCloseAll("upgrading", 2*time.Second, 5*time.Second)

	for _, tc := range []struct {
		name string
		srv  *fakeServer
		conn net.Conn
		want string
	}{
		{"resumable", srv1, conn1, "BRB :upgrading"},
		{"plain", srv2, conn2, "QUIT :upgrading"},
	} {
		_ = tc.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		got, err := bufio.NewReader(tc.conn).ReadString('\n')
		if err != nil {
			t.Fatalf("%s: read from uplink: %v", tc.name, err)
		}
		if got := strings.TrimRight(got, "\r\n"); got != tc.want {
			t.Fatalf("%s: uplink received %q, want %q", tc.name, got, tc.want)
		}
		tc.srv.close()
	}
}

func TestResumableKeyDeletedAndClearedWithBlob(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
	k, srv, _ := dialedTestNetwork(t, mgr, testNetID, 4096)
	k.PushBlob(BlobKeyResumable, BlobModeReplace, []byte("1"))
	k.PushBlob(BlobKeyResumable, BlobModeDelete, nil)
	if k.Resumable() {
		t.Fatal("still resumable after BlobModeDelete")
	}
	k.PushBlob(BlobKeyResumable, BlobModeReplace, []byte("1"))
	srv.close()
	_ = k.Close()
	if k.Resumable() {
		t.Fatal("still resumable after the connection closed and the blob was cleared")
	}
}
