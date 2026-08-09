package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMigrateIdempotent(t *testing.T) {
	p := filepath.Join(t.TempDir(), "t.db")
	s, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s2, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	s2.Close()
}

func TestOpenDBFileModeOwnerOnly(t *testing.T) {
	p := filepath.Join(t.TempDir(), "t.db")
	s, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	assertOwnerOnly(t, p)
}

func TestOpenChmodsWorldReadableDB(t *testing.T) {
	p := filepath.Join(t.TempDir(), "t.db")
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	assertOwnerOnly(t, p)
}

func assertOwnerOnly(t *testing.T, path string) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := st.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("%s mode=%04o want owner-only", path, perm)
	}
}

func TestNetworkFloodFields(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	_, err := s.UpsertNetwork(ctx, Network{
		Name: "n", Host: "h", Port: 6667, Nick: "me", Enabled: true,
		FloodBurst: 4096, FloodRate: 512.5,
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	if n.FloodBurst != 4096 || n.FloodRate != 512.5 {
		t.Fatalf("flood fields: burst=%d rate=%g", n.FloodBurst, n.FloodRate)
	}
	if list, err := s.ListNetworks(ctx); err != nil || len(list) != 1 || list[0].FloodBurst != 4096 {
		t.Fatalf("%+v %v", list, err)
	}
}

func TestNetworkNickRecoveryFields(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	_, err := s.UpsertNetwork(ctx, Network{
		Name: "n", Host: "h", Port: 6667, Nick: "me", AltNick: "me2", NickRecovery: true, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	if n.AltNick != "me2" || !n.NickRecovery {
		t.Fatalf("got alt=%q recovery=%v", n.AltNick, n.NickRecovery)
	}
	n.NickRecovery = false
	n.AltNick = ""
	if _, err := s.UpsertNetwork(ctx, n); err != nil {
		t.Fatal(err)
	}
	n, err = s.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	if n.AltNick != "" || n.NickRecovery {
		t.Fatalf("after clear: alt=%q recovery=%v", n.AltNick, n.NickRecovery)
	}
}

func TestNetworkTLSNoVerify(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	_, err := s.UpsertNetwork(ctx, Network{
		Name: "n", Host: "h", Port: 6697, Nick: "me", TLS: true, TLSNoVerify: true, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	if !n.TLSNoVerify {
		t.Fatal("expected tls_noverify=true")
	}
	n.TLSNoVerify = false
	if _, err := s.UpsertNetwork(ctx, n); err != nil {
		t.Fatal(err)
	}
	n, err = s.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	if n.TLSNoVerify {
		t.Fatal("expected tls_noverify=false after clear")
	}
}

func TestNetworkTLSCertPaths(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	_, err := s.UpsertNetwork(ctx, Network{
		Name: "n", Host: "h", Port: 6697, Nick: "me", TLS: true, Enabled: true,
		TLSCert: "certs/net.crt", TLSKey: "certs/net.key",
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	if n.TLSCert != "certs/net.crt" || n.TLSKey != "certs/net.key" {
		t.Fatalf("got cert=%q key=%q", n.TLSCert, n.TLSKey)
	}
	n.TLSCert = "none"
	n.TLSKey = ""
	if _, err := s.UpsertNetwork(ctx, n); err != nil {
		t.Fatal(err)
	}
	n, err = s.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	if n.TLSCert != "none" || n.TLSKey != "" {
		t.Fatalf("after clear: cert=%q key=%q", n.TLSCert, n.TLSKey)
	}
}

func TestNetworkBindHost(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	_, err := s.UpsertNetwork(ctx, Network{
		Name: "n", Host: "h", Port: 6697, Nick: "me", TLS: true, Enabled: true,
		BindHost: "203.0.113.10",
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	if n.BindHost != "203.0.113.10" {
		t.Fatalf("got bind_host=%q", n.BindHost)
	}
	n.BindHost = "none"
	if _, err := s.UpsertNetwork(ctx, n); err != nil {
		t.Fatal(err)
	}
	n, err = s.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	if n.BindHost != "none" {
		t.Fatalf("after clear: bind_host=%q", n.BindHost)
	}
}

func TestNetworkCRUD(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	id, err := s.UpsertNetwork(ctx, Network{
		Name: "libera", Host: "irc.libera.chat", Port: 6697, TLS: true, Nick: "gobnc", Enabled: true,
	})
	if err != nil || id == 0 {
		t.Fatalf("id=%d err=%v", id, err)
	}
	n, err := s.NetworkByName(ctx, "libera")
	if err != nil || n.Host != "irc.libera.chat" {
		t.Fatalf("%+v %v", n, err)
	}
	if err := s.AddChannel(ctx, id, "#gobnc", "secret"); err != nil {
		t.Fatal(err)
	}
	chs, err := s.ListChannels(ctx, id)
	if err != nil || len(chs) != 1 || chs[0].Name != "#gobnc" || chs[0].Key != "secret" {
		t.Fatalf("%v %v", chs, err)
	}
	if err := s.RemoveChannel(ctx, id, "#gobnc"); err != nil {
		t.Fatal(err)
	}
	chs, err = s.ListChannels(ctx, id)
	if err != nil || len(chs) != 0 {
		t.Fatalf("after remove: %v %v", chs, err)
	}
	list, err := s.ListNetworks(ctx)
	if err != nil || len(list) != 1 {
		t.Fatal(list, err)
	}
	if err := s.DeleteNetwork(ctx, "libera"); err != nil {
		t.Fatal(err)
	}
}

func TestAuthFingerprint(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	if err := s.SetPasswordHash(ctx, "argon2id$..."); err != nil {
		t.Fatal(err)
	}
	h, err := s.PasswordHash(ctx)
	if err != nil || h != "argon2id$..." {
		t.Fatal(h, err)
	}
	fp := "aabbccdd00112233445566778899aabbccddeeff00112233445566778899aaaa"
	if err := s.AddFingerprint(ctx, fp, "laptop"); err != nil {
		t.Fatal(err)
	}
	ok, err := s.HasFingerprint(ctx, fp)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	ok, _ = s.HasFingerprint(ctx, "nope")
	if ok {
		t.Fatal("expected miss")
	}
	list, err := s.ListFingerprints(ctx)
	if err != nil || len(list) != 1 || list[0].Fingerprint != fp || list[0].Label != "laptop" {
		t.Fatalf("%+v %v", list, err)
	}
	fp2 := "bbccddee00112233445566778899aabbccddeeff00112233445566778899bbbb"
	if err := s.AddFingerprint(ctx, fp2, "phone"); err != nil {
		t.Fatal(err)
	}
	got, err := s.ResolveFingerprint(ctx, "#2")
	if err != nil || got != fp2 {
		t.Fatalf("resolve #2: %q %v", got, err)
	}
	got, err = s.ResolveFingerprint(ctx, "2")
	if err != nil || got != fp2 {
		t.Fatalf("resolve 2: %q %v", got, err)
	}
	got, err = s.ResolveFingerprint(ctx, "bbccddee")
	if err != nil || got != fp2 {
		t.Fatalf("resolve prefix: %q %v", got, err)
	}
	if err := s.RemoveFingerprint(ctx, fp2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveFingerprint(ctx, "#2"); err == nil {
		t.Fatal("expected missing #2")
	}
	// Relabel via add conflict
	if err := s.AddFingerprint(ctx, fp, "desk"); err != nil {
		t.Fatal(err)
	}
	list, err = s.ListFingerprints(ctx)
	if err != nil || len(list) != 1 || list[0].Label != "desk" {
		t.Fatalf("relabel: %+v %v", list, err)
	}
}

func TestMessagesQueryRetention(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	id, err := s.UpsertNetwork(ctx, Network{Name: "n", Host: "h", Port: 6667, Nick: "x", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		m := Message{
			NetworkID: id, Target: "#c", Time: t0.Add(time.Duration(i) * time.Minute),
			MsgID: "m" + string(rune('a'+i)), Command: "PRIVMSG", Source: "a!b@c",
			Raw: "x", Text: "msg",
		}
		if err := s.InsertMessage(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	before := t0.Add(3 * time.Minute)
	msgs, err := s.QueryMessages(ctx, HistoryQuery{NetworkID: id, Target: "#c", Before: &before, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("got %d", len(msgs))
	}
	latest, err := s.QueryMessages(ctx, HistoryQuery{NetworkID: id, Target: "#c", Latest: true, Limit: 2})
	if err != nil || len(latest) != 2 {
		t.Fatalf("%v %v", latest, err)
	}
	if !latest[0].Time.Before(latest[1].Time) && !latest[0].Time.Equal(latest[1].Time) {
		t.Fatal("not ascending")
	}

	anchor, err := s.MessageByMsgID(ctx, id, "#c", "mc")
	if err != nil || anchor == nil || anchor.MsgID != "mc" {
		t.Fatalf("MessageByMsgID: %+v err=%v", anchor, err)
	}
	beforeBound, err := s.QueryMessages(ctx, HistoryQuery{
		NetworkID:  id,
		Target:     "#c",
		BeforeBound: &HistoryBound{Time: anchor.Time, ID: anchor.ID},
		Limit:      10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeBound) != 2 {
		t.Fatalf("BeforeBound want 2 got %d", len(beforeBound))
	}
	for _, m := range beforeBound {
		if m.MsgID == "mc" {
			t.Fatal("BeforeBound must exclude anchor")
		}
	}
	missing, err := s.MessageByMsgID(ctx, id, "#c", "nope")
	if err != nil || missing != nil {
		t.Fatalf("missing msgid: %+v %v", missing, err)
	}

	n, err := s.DeleteOlderThan(ctx, id, t0.Add(2*time.Minute))
	if err != nil || n != 2 {
		t.Fatalf("deleted=%d err=%v", n, err)
	}
}

func TestReadMarkerMonotonic(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	id, err := s.UpsertNetwork(ctx, Network{Name: "n", Host: "h", Port: 6667, Nick: "x", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	_, ok, err := s.GetReadMarker(ctx, id, "#c")
	if err != nil || ok {
		t.Fatalf("empty: ok=%v err=%v", ok, err)
	}
	t1 := "2019-01-04T14:33:26.123Z"
	t2 := "2019-01-04T14:34:00.000Z"
	stored, updated, err := s.SetReadMarkerIfNewer(ctx, id, "#c", t1)
	if err != nil || !updated || stored != t1 {
		t.Fatalf("first set: %q updated=%v err=%v", stored, updated, err)
	}
	stored, updated, err = s.SetReadMarkerIfNewer(ctx, id, "#c", t1)
	if err != nil || updated || stored != t1 {
		t.Fatalf("equal: %q updated=%v err=%v", stored, updated, err)
	}
	stored, updated, err = s.SetReadMarkerIfNewer(ctx, id, "#c", "2019-01-04T14:33:00.000Z")
	if err != nil || updated || stored != t1 {
		t.Fatalf("older: %q updated=%v err=%v", stored, updated, err)
	}
	stored, updated, err = s.SetReadMarkerIfNewer(ctx, id, "#c", t2)
	if err != nil || !updated || stored != t2 {
		t.Fatalf("newer: %q updated=%v err=%v", stored, updated, err)
	}
	got, ok, err := s.GetReadMarker(ctx, id, "#c")
	if err != nil || !ok || got != t2 {
		t.Fatalf("get: %q ok=%v err=%v", got, ok, err)
	}
}

func openTemp(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
