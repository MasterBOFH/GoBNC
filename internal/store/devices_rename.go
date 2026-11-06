package store

import "context"

// RenameBuffer follows a query peer's nick change on every device that has a row for the old
// nick (fromKey, folded), open or closed: the row becomes the new nick's (toKey, display
// toDisplay), keeping is_open and closed_at_id, merged into the device's row for toKey when it has
// one (open if either was, the higher floor). renamed_from keeps the name the device's last sync
// listed it under: an earlier, unreported renamed_from of the old row, else the old row's display
// (a case-only change, fromKey == toKey, sets none). It returns the client_ids whose rows changed.
func (s *Store) RenameBuffer(ctx context.Context, networkID int64, fromKey, toKey, toDisplay string) ([]string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	type row struct {
		client, display, renamedFrom string
		open                         bool
		closedAt                     int64
	}
	read := func(target string) (map[string]row, []string, error) {
		rows, err := tx.QueryContext(ctx, `SELECT client_id, display, is_open, closed_at_id, renamed_from FROM open_buffers
			WHERE network_id=? AND target=? ORDER BY client_id`, networkID, target)
		if err != nil {
			return nil, nil, err
		}
		defer rows.Close()
		out := map[string]row{}
		var order []string
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.client, &r.display, &r.open, &r.closedAt, &r.renamedFrom); err != nil {
				return nil, nil, err
			}
			out[r.client] = r
			order = append(order, r.client)
		}
		return out, order, rows.Err()
	}
	from, order, err := read(fromKey)
	if err != nil || len(order) == 0 {
		return nil, err
	}
	to := map[string]row{}
	if toKey != fromKey {
		if to, _, err = read(toKey); err != nil {
			return nil, err
		}
	}
	now := nowText()
	for _, c := range order {
		f := from[c]
		open, closedAt, renamedFrom := f.open, f.closedAt, f.renamedFrom
		if renamedFrom == "" && toKey != fromKey {
			renamedFrom = f.display
		}
		if t, ok := to[c]; ok {
			open = open || t.open
			closedAt = max(closedAt, t.closedAt)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM open_buffers WHERE client_id=? AND network_id=? AND target IN (?, ?)`,
			c, networkID, fromKey, toKey); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO open_buffers (client_id, network_id, target, display, is_open, closed_at_id, updated_at, renamed_from)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, c, networkID, toKey, toDisplay, open, closedAt, now, renamedFrom); err != nil {
			return nil, err
		}
	}
	return order, tx.Commit()
}

// ClearRenamedFrom forgets a row's renamed_from once a sync reported it. A row renamed again
// since the sync read it (a different renamed_from) is left alone.
func (s *Store) ClearRenamedFrom(ctx context.Context, clientID string, networkID int64, target, renamedFrom string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE open_buffers SET renamed_from='' WHERE client_id=? AND network_id=? AND target=? AND renamed_from=?`,
		clientID, networkID, target, renamedFrom)
	return err
}

// HasBufferRows reports whether any device has a row (open or closed) for the buffer.
func (s *Store) HasBufferRows(ctx context.Context, networkID int64, target string) (bool, error) {
	return s.exists(ctx, `SELECT 1 FROM open_buffers WHERE network_id=? AND target=? LIMIT 1`, networkID, target)
}
