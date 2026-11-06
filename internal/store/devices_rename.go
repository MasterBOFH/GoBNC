package store

import (
	"context"
	"database/sql"
)

// RenameBuffer follows a query peer's nick change on every device that has a row for the old
// nick (fromKey, folded), open or closed: the row becomes the new nick's (toKey, display
// toDisplay), keeping is_open and closed_at_id. A device that already has a row for toKey keeps
// both as they are (owner ruling, "keep both"): nothing changes there. renamed_from keeps the
// name the device's last sync listed it under: an earlier, unreported renamed_from of the old
// row, else the old row's display (a case-only change, fromKey == toKey, sets none). It returns
// the client_ids whose rows were renamed and whether some device kept both. When no device kept
// both and some row was renamed, the shared read marker moves from fromKey to toKey too, unless
// toKey has its own.
func (s *Store) RenameBuffer(ctx context.Context, networkID int64, fromKey, toKey, toDisplay string) (changed []string, keptOld bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()
	type row struct {
		client, display, renamedFrom string
		open                         bool
		closedAt                     int64
	}
	rows, err := tx.QueryContext(ctx, `SELECT client_id, display, is_open, closed_at_id, renamed_from FROM open_buffers
		WHERE network_id=? AND target=? ORDER BY client_id`, networkID, fromKey)
	if err != nil {
		return nil, false, err
	}
	var from []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.client, &r.display, &r.open, &r.closedAt, &r.renamedFrom); err != nil {
			rows.Close()
			return nil, false, err
		}
		from = append(from, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(from) == 0 {
		return nil, false, err
	}
	now := nowText()
	for _, f := range from {
		if toKey != fromKey {
			var one int
			err := tx.QueryRowContext(ctx, `SELECT 1 FROM open_buffers WHERE client_id=? AND network_id=? AND target=?`,
				f.client, networkID, toKey).Scan(&one)
			if err == nil {
				keptOld = true
				continue
			}
			if !isNoRows(err) {
				return nil, false, err
			}
		}
		renamedFrom := f.renamedFrom
		if renamedFrom == "" && toKey != fromKey {
			renamedFrom = f.display
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM open_buffers WHERE client_id=? AND network_id=? AND target=?`,
			f.client, networkID, fromKey); err != nil {
			return nil, false, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO open_buffers (client_id, network_id, target, display, is_open, closed_at_id, updated_at, renamed_from)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, f.client, networkID, toKey, toDisplay, f.open, f.closedAt, now, renamedFrom); err != nil {
			return nil, false, err
		}
		changed = append(changed, f.client)
	}
	if len(changed) > 0 && !keptOld && toKey != fromKey {
		if _, err := tx.ExecContext(ctx, `UPDATE read_markers SET target=? WHERE network_id=? AND target=?
			AND NOT EXISTS (SELECT 1 FROM read_markers o WHERE o.network_id=? AND o.target=?)`,
			toKey, networkID, fromKey, networkID, toKey); err != nil {
			return nil, false, err
		}
	}
	return changed, keptOld, tx.Commit()
}

// DeviceHoldsBoth reports whether some device has rows for both keys: a peer's nick change then
// keeps both queries there (RenameBuffer), and the old one shows the change too.
func (s *Store) DeviceHoldsBoth(ctx context.Context, networkID int64, fromKey, toKey string) (bool, error) {
	if fromKey == toKey {
		return false, nil
	}
	return s.exists(ctx, `SELECT 1 FROM open_buffers a JOIN open_buffers b
		ON b.client_id=a.client_id AND b.network_id=a.network_id AND b.target=?
		WHERE a.network_id=? AND a.target=? LIMIT 1`, toKey, networkID, fromKey)
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

func isNoRows(err error) bool { return err == sql.ErrNoRows }
