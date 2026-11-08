package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// legacyServerKey is where the Server buffer's lines used to be stored: its name's fold
// ("server" under every CASEMAPPING), which is also the key of a real nick Server's query.
// They are now stored under each server's own name (see server_targets.go).
const legacyServerKey = "server"

// migrateServerKey moves the Server buffer's rows from legacyServerKey to their servers' names,
// per network, in one transaction each. It is idempotent and runs at every Open: once moved,
// nothing under "server" matches again, so it costs one indexed query per network (and a brain
// rolled back to an older build, which stores under "server" again, is migrated again on the
// next upgrade).
//
//   - A row from a server (irc.ServerSource with a source: a name with a dot and no "!"/"@")
//     moves to that name, lowercased (a host name folds the same under every CASEMAPPING).
//   - A row with no source, or addressed to a $mask by a person (a global notice), names no
//     server; it moves to the network's configured host when that is a server name (the
//     server we dialed; the live path uses the server we registered to, which the store does
//     not keep), and otherwise stays.
//   - Everything else under "server" — lines from or to a nick Server — stays: that is the
//     query. A nick Server's line whose source looks like a server would move; no real user's
//     prefix does.
//
// Ids are kept. A row whose keeper line is already stored under its new target is the same
// line twice and is dropped.
//
// The read marker and device rows (open_buffers) carry no source. When this run moved rows and
// nothing is left under "server" (no query of a nick Server shares the key), they move to the
// Server buffer's key "*" (where the module API keeps them; a row "*" has already wins) and the
// legacy playback cursor for "server" is dropped; otherwise they stay with "server", whose they
// are unknown.
func (s *Store) migrateServerKey(ctx context.Context) error {
	nets, err := s.db.QueryContext(ctx, `SELECT id, host FROM networks`)
	if err != nil {
		return err
	}
	hosts := map[int64]string{}
	var ids []int64
	for nets.Next() {
		var id int64
		var host string
		if err := nets.Scan(&id, &host); err != nil {
			nets.Close()
			return err
		}
		ids = append(ids, id)
		hosts[id] = host
	}
	nets.Close()
	if err := nets.Err(); err != nil {
		return err
	}
	for _, nid := range ids {
		if err := s.migrateServerKeyNetwork(ctx, nid, hosts[nid]); err != nil {
			return fmt.Errorf("server key, network %d: %w", nid, err)
		}
	}
	return nil
}

func (s *Store) migrateServerKeyNetwork(ctx context.Context, nid int64, host string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id, source, raw FROM messages WHERE network_id=? AND target=?`, nid, legacyServerKey)
	if err != nil {
		return err
	}
	dialed := strings.ToLower(strings.TrimSpace(host))
	if !irc.IsServerName(dialed) {
		dialed = ""
	}
	move := map[int64]string{}
	var order []int64
	for rows.Next() {
		var id int64
		var source, raw string
		if err := rows.Scan(&id, &source, &raw); err != nil {
			rows.Close()
			return err
		}
		to := ""
		switch {
		case source != "" && irc.ServerSource(source):
			to = strings.ToLower(source)
		case source == "" || toMask(raw):
			to = dialed
		}
		if to != "" {
			move[id] = to
			order = append(order, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(order) == 0 {
		return nil
	}
	for _, id := range order {
		res, err := tx.ExecContext(ctx, `UPDATE OR IGNORE messages SET target=? WHERE id=?`, move[id], id)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			// The keeper line is stored under the new target already (the unique keeper_seq index).
			if _, err := tx.ExecContext(ctx, `DELETE FROM messages WHERE id=?`, id); err != nil {
				return err
			}
		}
	}
	var left int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM messages WHERE network_id=? AND target=?)`, nid, legacyServerKey).Scan(&left); err != nil {
		return err
	}
	if left == 0 {
		for _, table := range []string{"read_markers", "open_buffers"} {
			if err := moveKey(ctx, tx, table, nid, legacyServerKey, wire.ServerBufferKey); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM playback_cursors WHERE network_id=? AND target=?`, nid, legacyServerKey); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	for _, to := range move {
		s.noteTarget(nid, to)
	}
	return nil
}

// moveKey re-keys a table's rows of one network from one target to another; where the new key
// has a row already, that one is kept and the old one dropped.
func moveKey(ctx context.Context, tx *sql.Tx, table string, nid int64, from, to string) error {
	if _, err := tx.ExecContext(ctx, `UPDATE OR IGNORE `+table+` SET target=? WHERE network_id=? AND target=?`, to, nid, from); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE network_id=? AND target=?`, nid, from)
	return err
}

// toMask reports whether a stored raw line was addressed to a $mask (a global notice).
func toMask(raw string) bool {
	m, err := irc.Parse(raw)
	return err == nil && strings.HasPrefix(m.Param(0), "$")
}
