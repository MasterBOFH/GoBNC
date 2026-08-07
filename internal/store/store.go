// Package store provides SQLite persistence for config and chat history.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Store wraps a SQLite database.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) a SQLite database and migrates schema.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// DB returns the underlying *sql.DB for advanced use.
func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) migrate() error {
	stmts := []string{
		`PRAGMA foreign_keys = ON`,
		`CREATE TABLE IF NOT EXISTS bouncer_meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS auth (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			password_hash TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS cert_fingerprints (
			fingerprint TEXT PRIMARY KEY,
			label TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS networks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			host TEXT NOT NULL,
			port INTEGER NOT NULL DEFAULT 6697,
			tls INTEGER NOT NULL DEFAULT 1,
			nick TEXT NOT NULL,
			username TEXT NOT NULL DEFAULT 'gobnc',
			realname TEXT NOT NULL DEFAULT 'GoBNC',
			pass TEXT NOT NULL DEFAULT '',
			sasl_user TEXT NOT NULL DEFAULT '',
			sasl_pass TEXT NOT NULL DEFAULT '',
			sasl_required INTEGER NOT NULL DEFAULT 0,
			enabled INTEGER NOT NULL DEFAULT 1
		)`,
		`CREATE TABLE IF NOT EXISTS channels (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			network_id INTEGER NOT NULL REFERENCES networks(id) ON DELETE CASCADE,
			name TEXT NOT NULL,
			key TEXT NOT NULL DEFAULT '',
			UNIQUE(network_id, name)
		)`,
		`CREATE TABLE IF NOT EXISTS messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			network_id INTEGER NOT NULL REFERENCES networks(id) ON DELETE CASCADE,
			target TEXT NOT NULL,
			time TEXT NOT NULL,
			msgid TEXT NOT NULL DEFAULT '',
			command TEXT NOT NULL,
			source TEXT NOT NULL DEFAULT '',
			raw TEXT NOT NULL,
			text TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_target_time ON messages(network_id, target, time)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_msgid ON messages(network_id, msgid)`,
		`CREATE TABLE IF NOT EXISTS read_markers (
			network_id INTEGER NOT NULL REFERENCES networks(id) ON DELETE CASCADE,
			target TEXT NOT NULL,
			time TEXT NOT NULL,
			UNIQUE(network_id, target)
		)`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return fmt.Errorf("migrate: %w\nquery: %s", err, q)
		}
	}
	_, _ = s.db.Exec(`INSERT OR IGNORE INTO auth (id, password_hash, updated_at) VALUES (1, '', ?)`, time.Now().UTC().Format(time.RFC3339Nano))
	_, _ = s.db.Exec(`INSERT OR IGNORE INTO bouncer_meta (key, value) VALUES ('schema_version', '1')`)
	return nil
}

// Network is a configured IRC network.
type Network struct {
	ID            int64
	Name          string
	Host          string
	Port          int
	TLS           bool
	Nick          string
	Username      string
	Realname      string
	Pass          string
	SASLUser      string
	SASLPass      string
	SASLRequired  bool
	Enabled       bool
}

// Channel is an auto-join channel.
type Channel struct {
	ID        int64
	NetworkID int64
	Name      string
	Key       string
}

// Message is a stored chat line.
type Message struct {
	ID        int64
	NetworkID int64
	Target    string
	Time      time.Time
	MsgID     string
	Command   string
	Source    string
	Raw       string
	Text      string
}

// UpsertNetwork inserts or updates a network by name.
func (s *Store) UpsertNetwork(ctx context.Context, n Network) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO networks (name, host, port, tls, nick, username, realname, pass, sasl_user, sasl_pass, sasl_required, enabled)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			host=excluded.host, port=excluded.port, tls=excluded.tls, nick=excluded.nick,
			username=excluded.username, realname=excluded.realname, pass=excluded.pass,
			sasl_user=excluded.sasl_user, sasl_pass=excluded.sasl_pass,
			sasl_required=excluded.sasl_required, enabled=excluded.enabled
	`, n.Name, n.Host, n.Port, boolInt(n.TLS), n.Nick, n.Username, n.Realname, n.Pass,
		n.SASLUser, n.SASLPass, boolInt(n.SASLRequired), boolInt(n.Enabled))
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil || id == 0 {
		err = s.db.QueryRowContext(ctx, `SELECT id FROM networks WHERE name=?`, n.Name).Scan(&id)
	}
	return id, err
}

// ListNetworks returns all networks.
func (s *Store) ListNetworks(ctx context.Context) ([]Network, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, host, port, tls, nick, username, realname, pass, sasl_user, sasl_pass, sasl_required, enabled
		FROM networks ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Network
	for rows.Next() {
		var n Network
		var tls, saslReq, en int
		if err := rows.Scan(&n.ID, &n.Name, &n.Host, &n.Port, &tls, &n.Nick, &n.Username, &n.Realname,
			&n.Pass, &n.SASLUser, &n.SASLPass, &saslReq, &en); err != nil {
			return nil, err
		}
		n.TLS = tls != 0
		n.SASLRequired = saslReq != 0
		n.Enabled = en != 0
		out = append(out, n)
	}
	return out, rows.Err()
}

// NetworkByName returns a network.
func (s *Store) NetworkByName(ctx context.Context, name string) (Network, error) {
	var n Network
	var tls, saslReq, en int
	err := s.db.QueryRowContext(ctx, `
		SELECT id, name, host, port, tls, nick, username, realname, pass, sasl_user, sasl_pass, sasl_required, enabled
		FROM networks WHERE name=?`, name).Scan(
		&n.ID, &n.Name, &n.Host, &n.Port, &tls, &n.Nick, &n.Username, &n.Realname,
		&n.Pass, &n.SASLUser, &n.SASLPass, &saslReq, &en)
	if err != nil {
		return n, err
	}
	n.TLS = tls != 0
	n.SASLRequired = saslReq != 0
	n.Enabled = en != 0
	return n, nil
}

// DeleteNetwork removes a network by name.
func (s *Store) DeleteNetwork(ctx context.Context, name string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM networks WHERE name=?`, name)
	return err
}

// AddChannel adds or updates an auto-join channel (persists key).
func (s *Store) AddChannel(ctx context.Context, networkID int64, name, key string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO channels (network_id, name, key) VALUES (?, ?, ?)
		ON CONFLICT(network_id, name) DO UPDATE SET key=excluded.key`, networkID, name, key)
	return err
}

// RemoveChannel removes an auto-join channel.
func (s *Store) RemoveChannel(ctx context.Context, networkID int64, name string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM channels WHERE network_id=? AND name=?`, networkID, name)
	return err
}

// ListChannels returns channels for a network.
func (s *Store) ListChannels(ctx context.Context, networkID int64) ([]Channel, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, network_id, name, key FROM channels WHERE network_id=?`, networkID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Channel
	for rows.Next() {
		var c Channel
		if err := rows.Scan(&c.ID, &c.NetworkID, &c.Name, &c.Key); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// SetPasswordHash stores argon2id hash (empty clears).
func (s *Store) SetPasswordHash(ctx context.Context, hash string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE auth SET password_hash=?, updated_at=? WHERE id=1`,
		hash, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// PasswordHash returns the stored hash.
func (s *Store) PasswordHash(ctx context.Context) (string, error) {
	var h string
	err := s.db.QueryRowContext(ctx, `SELECT password_hash FROM auth WHERE id=1`).Scan(&h)
	return h, err
}

// AddFingerprint adds an allowed client cert fingerprint (lowercase hex).
func (s *Store) AddFingerprint(ctx context.Context, fp, label string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO cert_fingerprints (fingerprint, label, created_at) VALUES (?, ?, ?)
		ON CONFLICT(fingerprint) DO UPDATE SET label=excluded.label`,
		fp, label, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// RemoveFingerprint deletes a fingerprint.
func (s *Store) RemoveFingerprint(ctx context.Context, fp string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM cert_fingerprints WHERE fingerprint=?`, fp)
	return err
}

// HasFingerprint reports whether fp is allowed.
func (s *Store) HasFingerprint(ctx context.Context, fp string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM cert_fingerprints WHERE fingerprint=?`, fp).Scan(&n)
	return n > 0, err
}

// ListFingerprints returns all fingerprints.
func (s *Store) ListFingerprints(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT fingerprint FROM cert_fingerprints ORDER BY fingerprint`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var fp string
		if err := rows.Scan(&fp); err != nil {
			return nil, err
		}
		out = append(out, fp)
	}
	return out, rows.Err()
}

// InsertMessage stores a chat message.
func (s *Store) InsertMessage(ctx context.Context, m Message) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO messages (network_id, target, time, msgid, command, source, raw, text)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		m.NetworkID, m.Target, formatTime(m.Time), m.MsgID, m.Command, m.Source, m.Raw, m.Text)
	return err
}

// HistoryQuery parameters for CHATHISTORY-style queries.
type HistoryQuery struct {
	NetworkID int64
	Target    string
	Before    *time.Time
	After     *time.Time
	Around    *time.Time // center; Limit split before/after
	Between   bool       // if true, require both After and Before
	Limit     int
	Latest    bool     // if true, return the Limit most recent (optionally before Before)
	Commands  []string // if non-empty, only these IRC commands
}

func historyWhere(q HistoryQuery) (clause string, args []any) {
	clause = "network_id=? AND target=?"
	args = []any{q.NetworkID, q.Target}
	if len(q.Commands) == 0 {
		return clause, args
	}
	clause += " AND command IN ("
	for i, c := range q.Commands {
		if i > 0 {
			clause += ","
		}
		clause += "?"
		args = append(args, c)
	}
	clause += ")"
	return clause, args
}

// QueryMessages returns messages matching q, oldest-first.
func (s *Store) QueryMessages(ctx context.Context, q HistoryQuery) ([]Message, error) {
	if q.Limit <= 0 {
		q.Limit = 100
	}
	where, baseArgs := historyWhere(q)
	cols := `id, network_id, target, time, msgid, command, source, raw, text`
	var rows *sql.Rows
	var err error
	switch {
	case q.Around != nil:
		half := q.Limit / 2
		if half < 1 {
			half = 1
		}
		args := append(append([]any{}, baseArgs...), formatTime(*q.Around), half)
		beforeRows, err1 := s.db.QueryContext(ctx, `
			SELECT `+cols+` FROM (
				SELECT `+cols+`
				FROM messages WHERE `+where+` AND time < ?
				ORDER BY time DESC LIMIT ?
			) ORDER BY time ASC`, args...)
		if err1 != nil {
			return nil, err1
		}
		before, err1 := scanMessages(beforeRows)
		beforeRows.Close()
		if err1 != nil {
			return nil, err1
		}
		rest := q.Limit - len(before)
		if rest < 1 {
			rest = 1
		}
		args = append(append([]any{}, baseArgs...), formatTime(*q.Around), rest)
		afterRows, err1 := s.db.QueryContext(ctx, `
			SELECT `+cols+`
			FROM messages WHERE `+where+` AND time >= ?
			ORDER BY time ASC LIMIT ?`, args...)
		if err1 != nil {
			return nil, err1
		}
		after, err1 := scanMessages(afterRows)
		afterRows.Close()
		if err1 != nil {
			return nil, err1
		}
		return append(before, after...), nil
	case q.Between && q.After != nil && q.Before != nil:
		args := append(append([]any{}, baseArgs...), formatTime(*q.After), formatTime(*q.Before), q.Limit)
		rows, err = s.db.QueryContext(ctx, `
			SELECT `+cols+`
			FROM messages WHERE `+where+` AND time > ? AND time < ?
			ORDER BY time ASC LIMIT ?`, args...)
	case q.Latest:
		if q.Before != nil {
			args := append(append([]any{}, baseArgs...), formatTime(*q.Before), q.Limit)
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+` FROM (
					SELECT `+cols+`
					FROM messages WHERE `+where+` AND time < ?
					ORDER BY time DESC LIMIT ?
				) ORDER BY time ASC`, args...)
		} else {
			args := append(append([]any{}, baseArgs...), q.Limit)
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+` FROM (
					SELECT `+cols+`
					FROM messages WHERE `+where+`
					ORDER BY time DESC LIMIT ?
				) ORDER BY time ASC`, args...)
		}
	case q.Before != nil && q.After == nil:
		args := append(append([]any{}, baseArgs...), formatTime(*q.Before), q.Limit)
		rows, err = s.db.QueryContext(ctx, `
			SELECT `+cols+` FROM (
				SELECT `+cols+`
				FROM messages WHERE `+where+` AND time < ?
				ORDER BY time DESC LIMIT ?
			) ORDER BY time ASC`, args...)
	case q.After != nil:
		args := append(append([]any{}, baseArgs...), formatTime(*q.After), q.Limit)
		rows, err = s.db.QueryContext(ctx, `
			SELECT `+cols+`
			FROM messages WHERE `+where+` AND time > ?
			ORDER BY time ASC LIMIT ?`, args...)
	default:
		args := append(append([]any{}, baseArgs...), q.Limit)
		rows, err = s.db.QueryContext(ctx, `
			SELECT `+cols+` FROM (
				SELECT `+cols+`
				FROM messages WHERE `+where+`
				ORDER BY time DESC LIMIT ?
			) ORDER BY time ASC`, args...)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMessages(rows)
}

func scanMessages(rows *sql.Rows) ([]Message, error) {
	var out []Message
	for rows.Next() {
		var m Message
		var ts string
		if err := rows.Scan(&m.ID, &m.NetworkID, &m.Target, &ts, &m.MsgID, &m.Command, &m.Source, &m.Raw, &m.Text); err != nil {
			return nil, err
		}
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			t, _ = time.Parse(time.RFC3339, ts)
		}
		m.Time = t
		out = append(out, m)
	}
	return out, rows.Err()
}

// DeleteOlderThan removes messages older than t for retention.
func (s *Store) DeleteOlderThan(ctx context.Context, networkID int64, t time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM messages WHERE network_id=? AND time < ?`,
		networkID, formatTime(t))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// GetReadMarker returns the stored last-read @time for target (already casefolded), if any.
func (s *Store) GetReadMarker(ctx context.Context, networkID int64, target string) (ts string, ok bool, err error) {
	err = s.db.QueryRowContext(ctx,
		`SELECT time FROM read_markers WHERE network_id=? AND target=?`,
		networkID, target).Scan(&ts)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return ts, true, nil
}

// SetReadMarkerIfNewer stores ts when it is strictly newer than the existing marker.
// Returns the marker that should be advertised (stored value) and whether it changed.
func (s *Store) SetReadMarkerIfNewer(ctx context.Context, networkID int64, target, ts string) (stored string, updated bool, err error) {
	cur, ok, err := s.GetReadMarker(ctx, networkID, target)
	if err != nil {
		return "", false, err
	}
	if ok {
		cmp, err := compareMessageTimes(cur, ts)
		if err != nil {
			return "", false, err
		}
		if cmp >= 0 {
			return cur, false, nil
		}
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO read_markers (network_id, target, time) VALUES (?, ?, ?)
		ON CONFLICT(network_id, target) DO UPDATE SET time=excluded.time`,
		networkID, target, ts)
	if err != nil {
		return "", false, err
	}
	return ts, true, nil
}

// compareMessageTimes compares two message @time values. Returns -1 if a<b, 0 if equal, 1 if a>b.
func compareMessageTimes(a, b string) (int, error) {
	ta, err := parseMessageTime(a)
	if err != nil {
		return 0, err
	}
	tb, err := parseMessageTime(b)
	if err != nil {
		return 0, err
	}
	switch {
	case ta.Before(tb):
		return -1, nil
	case ta.After(tb):
		return 1, nil
	default:
		return 0, nil
	}
}

func parseMessageTime(s string) (time.Time, error) {
	s = strings.TrimPrefix(s, "timestamp=")
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	// Millisecond server-time without zone variants already covered by RFC3339.
	return time.Time{}, fmt.Errorf("invalid time %q", s)
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// formatTime uses fixed-width fractional seconds so SQLite text ORDER BY is chronological.
func formatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z")
}
