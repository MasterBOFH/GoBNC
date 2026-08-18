package session

import (
	"context"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
)

// TestRPLLoggedInPushesCloakBlob is the regression test for RPL_LOGGEDIN
// (900) updating self's user/host in memory (applyAccountFromSASL's
// UpdateFromPrefix, via the nick!user@host mask in Params[1]) without ever
// pushing that host to the keeper's "cloak" blob key. The live session gets
// self.Host right immediately (User.Prefix() works fine for the rest of
// that process's life), but nothing ever tells the keeper about it — so a
// later brain resume has no way to recover it, even though 900 is one of
// the two sources (900, CHGHOST) state_numerics.go documents as
// authoritative for self's identity. Uses runBouncerSASLServer from
// sasl_routing_e2e_test.go, which already sends 900 with a distinct
// nick!user@host mask ("testnick!u@h") ahead of 001/376.
func TestRPLLoggedInPushesCloakBlob(t *testing.T) {
	db := testutil.TempStore(t)
	ctx := context.Background()
	if _, err := db.UpsertNetwork(ctx, store.Network{
		Name: "n", Host: "irc.example", Port: 1, Nick: "testnick", Enabled: true,
		Username: "u", Realname: "r", SASL: true, SASLUser: "acct", SASLPass: "secret",
	}); err != nil {
		t.Fatal(err)
	}
	netCfg, err := db.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}

	s := New(netCfg, db, nil, nil, nil)

	ln, host, port := newFakeIRCListener(t)
	scriptDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		scriptDone <- runBouncerSASLServer(conn, time.Now().Add(8*time.Second))
	}()

	tu := newTestUplink(t, s, netCfg, host, port)

	waitUntil(t, 5*time.Second, func() bool { return s.Registered() })

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("script timeout")
	}

	waitUntil(t, 2*time.Second, func() bool {
		v, ok := tu.blobValue("cloak")
		return ok && v == "h"
	})
}
