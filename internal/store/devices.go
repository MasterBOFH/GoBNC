package store

import (
	"context"
	"database/sql"
	"time"
)

// Devices are module clients' devices (e.g. a phone running Rete), identified by the client_id
// the device chose. Their state lives here so every module connection, and a restarted module,
// sees the same thing: which networks the device follows (device_networks) and its open
// buffers (open_buffers).

// deviceMigrations are appended to migrate's statements.
var deviceMigrations = []string{
	`CREATE TABLE IF NOT EXISTS devices (
		client_id TEXT PRIMARY KEY,
		name TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL,
		last_seen TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS device_networks (
		client_id TEXT NOT NULL REFERENCES devices(client_id) ON DELETE CASCADE,
		network_id INTEGER NOT NULL REFERENCES networks(id) ON DELETE CASCADE,
		subscribed INTEGER NOT NULL DEFAULT 0,
		seeded INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY(client_id, network_id)
	)`,
	`CREATE TABLE IF NOT EXISTS open_buffers (
		client_id TEXT NOT NULL REFERENCES devices(client_id) ON DELETE CASCADE,
		network_id INTEGER NOT NULL REFERENCES networks(id) ON DELETE CASCADE,
		target TEXT NOT NULL,
		display TEXT NOT NULL,
		is_open INTEGER NOT NULL,
		closed_at_id INTEGER NOT NULL DEFAULT 0,
		updated_at TEXT NOT NULL,
		PRIMARY KEY(client_id, network_id, target)
	)`,
}

// Device is one known device.
type Device struct {
	ClientID  string
	Name      string
	CreatedAt time.Time
	LastSeen  time.Time
}

// OpenBuffer is one device's sidebar entry for a buffer (a query or kept-open channel). A row
// with Open false records an intentional close: nothing up to ClosedAtID counts as unread on
// that device.
type OpenBuffer struct {
	Target     string // folded
	Display    string
	Open       bool
	ClosedAtID int64
	// RenamedFrom is the name the device's last sync listed the row under, when a peer's NICK
	// renamed it since (RenameBuffer); the next sync reports it and clears it.
	RenamedFrom string
}

func nowText() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// TouchDevice records that the device was seen (creating it, and naming it when name is set).
func (s *Store) TouchDevice(ctx context.Context, clientID, name string) (created bool, err error) {
	now := nowText()
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO devices (client_id, name, created_at, last_seen)
		VALUES (?, ?, ?, ?)`, clientID, name, now, now)
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return true, nil
	}
	if name != "" {
		_, err = s.db.ExecContext(ctx, `UPDATE devices SET last_seen=?, name=? WHERE client_id=?`, now, name, clientID)
	} else {
		_, err = s.db.ExecContext(ctx, `UPDATE devices SET last_seen=? WHERE client_id=?`, now, clientID)
	}
	return false, err
}

// ensureDevice creates a device row if missing (rows referencing it need one).
func (s *Store) ensureDevice(ctx context.Context, clientID string) error {
	now := nowText()
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO devices (client_id, created_at, last_seen) VALUES (?, ?, ?)`,
		clientID, now, now)
	return err
}

// ListDeviceNetworks returns the device's stored toggles by network id (missing = false).
func (s *Store) ListDeviceNetworks(ctx context.Context, clientID string) (map[int64]bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT network_id, subscribed FROM device_networks WHERE client_id=?`, clientID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]bool{}
	for rows.Next() {
		var id int64
		var sub bool
		if err := rows.Scan(&id, &sub); err != nil {
			return nil, err
		}
		out[id] = sub
	}
	return out, rows.Err()
}

// SetDeviceNetwork stores whether the device follows a network.
func (s *Store) SetDeviceNetwork(ctx context.Context, clientID string, networkID int64, subscribed bool) error {
	if err := s.ensureDevice(ctx, clientID); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO device_networks (client_id, network_id, subscribed) VALUES (?, ?, ?)
		ON CONFLICT(client_id, network_id) DO UPDATE SET subscribed=excluded.subscribed`, clientID, networkID, subscribed)
	return err
}

// ListOpenBuffers returns the device's buffer rows on a network (open and closed).
func (s *Store) ListOpenBuffers(ctx context.Context, clientID string, networkID int64) ([]OpenBuffer, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT target, display, is_open, closed_at_id, renamed_from FROM open_buffers
		WHERE client_id=? AND network_id=? ORDER BY target`, clientID, networkID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OpenBuffer
	for rows.Next() {
		var b OpenBuffer
		if err := rows.Scan(&b.Target, &b.Display, &b.Open, &b.ClosedAtID, &b.RenamedFrom); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// BufferClosedAt is the device's closed_at_id for a buffer (0 when it has no row).
func (s *Store) BufferClosedAt(ctx context.Context, clientID string, networkID int64, target string) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `SELECT closed_at_id FROM open_buffers WHERE client_id=? AND network_id=? AND target=?`,
		clientID, networkID, target).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

// SetBufferOpen opens or closes a buffer on a device. closed_at_id only moves up.
func (s *Store) SetBufferOpen(ctx context.Context, clientID string, networkID int64, target, display string, open bool, closedAtID int64) error {
	if err := s.ensureDevice(ctx, clientID); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO open_buffers (client_id, network_id, target, display, is_open, closed_at_id, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(client_id, network_id, target) DO UPDATE SET display=excluded.display, is_open=excluded.is_open,
			closed_at_id=MAX(open_buffers.closed_at_id, excluded.closed_at_id), updated_at=excluded.updated_at`,
		clientID, networkID, target, display, open, closedAtID, nowText())
	return err
}

// ReopenForMessage is the rule "a new conversation line in a query opens it on every device":
// for each device following the network where the buffer is not open (and was not closed after
// this line — a keeper replay of a stored line), insert it open — or reopen a closed row with
// closed_at_id raised to id-1, so what was unread at the close stays dismissed but the new line
// (id) counts. It returns the client_ids whose state changed.
func (s *Store) ReopenForMessage(ctx context.Context, networkID int64, target, display string, id int64) ([]string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Devices following the network where the buffer is not open, and was not closed after this
	// line (a replayed line must not undo a close).
	rows, err := tx.QueryContext(ctx, `SELECT d.client_id FROM devices d
		JOIN device_networks dn ON dn.client_id=d.client_id AND dn.network_id=? AND dn.subscribed=1
		LEFT JOIN open_buffers b ON b.client_id=d.client_id AND b.network_id=? AND b.target=?
		WHERE b.client_id IS NULL OR (b.is_open=0 AND b.closed_at_id < ?) ORDER BY d.client_id`,
		networkID, networkID, target, id)
	if err != nil {
		return nil, err
	}
	var changed []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			rows.Close()
			return nil, err
		}
		changed = append(changed, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	now := nowText()
	for _, c := range changed {
		if _, err := tx.ExecContext(ctx, `INSERT INTO open_buffers (client_id, network_id, target, display, is_open, closed_at_id, updated_at)
			VALUES (?, ?, ?, ?, 1, 0, ?)
			ON CONFLICT(client_id, network_id, target) DO UPDATE SET is_open=1, display=excluded.display,
				closed_at_id=MAX(open_buffers.closed_at_id, ?), updated_at=excluded.updated_at`,
			c, networkID, target, display, now, id-1); err != nil {
			return nil, err
		}
	}
	return changed, tx.Commit()
}

// DeviceSeeded reports whether the device's open set on a network was seeded (MarkDeviceSeeded).
func (s *Store) DeviceSeeded(ctx context.Context, clientID string, networkID int64) (bool, error) {
	return s.exists(ctx, `SELECT 1 FROM device_networks WHERE client_id=? AND network_id=? AND seeded=1`, clientID, networkID)
}

// MarkDeviceSeeded records that the device's open set on a network was seeded.
func (s *Store) MarkDeviceSeeded(ctx context.Context, clientID string, networkID int64) error {
	if err := s.ensureDevice(ctx, clientID); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO device_networks (client_id, network_id, seeded) VALUES (?, ?, 1)
		ON CONFLICT(client_id, network_id) DO UPDATE SET seeded=1`, clientID, networkID)
	return err
}

// DeviceHasBuffers reports whether the device has any buffer row on a network.
func (s *Store) DeviceHasBuffers(ctx context.Context, clientID string, networkID int64) (bool, error) {
	return s.exists(ctx, `SELECT 1 FROM open_buffers WHERE client_id=? AND network_id=? LIMIT 1`, clientID, networkID)
}

// ListDevices returns every known device.
func (s *Store) ListDevices(ctx context.Context) ([]Device, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT client_id, name, created_at, last_seen FROM devices ORDER BY client_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Device
	for rows.Next() {
		var d Device
		var created, seen string
		if err := rows.Scan(&d.ClientID, &d.Name, &created, &seen); err != nil {
			return nil, err
		}
		d.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		d.LastSeen, _ = time.Parse(time.RFC3339Nano, seen)
		out = append(out, d)
	}
	return out, rows.Err()
}

// DeleteDevice forgets a device and all its rows.
func (s *Store) DeleteDevice(ctx context.Context, clientID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM devices WHERE client_id=?`, clientID)
	return err
}
