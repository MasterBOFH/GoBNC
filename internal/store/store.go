// Package store provides SQLite persistence for config and chat history.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Store wraps a SQLite database.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) a SQLite database and migrates schema.
// The DB file is created with mode 0600 when missing; existing files are chmod'd to 0600.
func Open(path string) (*Store, error) {
	if err := ensureOwnerOnlyFile(path); err != nil {
		return nil, err
	}
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
	// SQLite may create sidecar files; keep the main DB owner-only after first write.
	_ = os.Chmod(path, 0o600)
	return s, nil
}

// ensureOwnerOnlyFile creates path with 0600 if missing, or chmods an existing file to 0600.
func ensureOwnerOnlyFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("open db file: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("chmod db file: %w", err)
	}
	return nil
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
			sasl INTEGER NOT NULL DEFAULT 0,
			sasl_required INTEGER NOT NULL DEFAULT 0,
			enabled INTEGER NOT NULL DEFAULT 1,
			flood_burst INTEGER NOT NULL DEFAULT 0,
			flood_rate REAL NOT NULL DEFAULT 0,
			alt_nick TEXT NOT NULL DEFAULT '',
			nick_recovery INTEGER NOT NULL DEFAULT 1,
			tls_noverify INTEGER NOT NULL DEFAULT 0,
			tls_cert TEXT NOT NULL DEFAULT '',
			tls_key TEXT NOT NULL DEFAULT '',
			bind_host TEXT NOT NULL DEFAULT ''
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
		`CREATE TABLE IF NOT EXISTS playback_cursors (
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
	// Existing DBs created before flood / playback / nick-recovery columns.
	_, _ = s.db.Exec(`ALTER TABLE networks ADD COLUMN flood_burst INTEGER NOT NULL DEFAULT 0`)
	_, _ = s.db.Exec(`ALTER TABLE networks ADD COLUMN flood_rate REAL NOT NULL DEFAULT 0`)
	_, _ = s.db.Exec(`ALTER TABLE networks ADD COLUMN alt_nick TEXT NOT NULL DEFAULT ''`)
	_, _ = s.db.Exec(`ALTER TABLE networks ADD COLUMN nick_recovery INTEGER NOT NULL DEFAULT 1`)
	_, _ = s.db.Exec(`ALTER TABLE networks ADD COLUMN tls_noverify INTEGER NOT NULL DEFAULT 0`)
	_, _ = s.db.Exec(`ALTER TABLE networks ADD COLUMN tls_cert TEXT NOT NULL DEFAULT ''`)
	_, _ = s.db.Exec(`ALTER TABLE networks ADD COLUMN tls_key TEXT NOT NULL DEFAULT ''`)
	_, _ = s.db.Exec(`ALTER TABLE networks ADD COLUMN bind_host TEXT NOT NULL DEFAULT ''`)
	_, _ = s.db.Exec(`ALTER TABLE networks ADD COLUMN sasl INTEGER NOT NULL DEFAULT 0`)
	// Existing networks with password SASL credentials keep doing SASL.
	_, _ = s.db.Exec(`UPDATE networks SET sasl=1 WHERE sasl_user != '' AND sasl_pass != '' AND sasl=0`)
	_, _ = s.db.Exec(`CREATE TABLE IF NOT EXISTS playback_cursors (
		network_id INTEGER NOT NULL REFERENCES networks(id) ON DELETE CASCADE,
		target TEXT NOT NULL,
		time TEXT NOT NULL,
		UNIQUE(network_id, target)
	)`)
	return nil
}

// Network is a configured IRC network.
type Network struct {
	ID           int64
	Name         string
	Host         string
	Port         int
	TLS          bool
	Nick         string
	Username     string
	Realname     string
	Pass         string
	SASLUser 	 string
	SASLPass 	 string
	// SASL enables bouncer-owned SASL. With user+pass: SCRAM-SHA-256/PLAIN.
	// With neither and a client cert: EXTERNAL. Cert alone does not enable SASL.
	SASL         bool
	SASLRequired bool
	Enabled      bool
	// FloodBurst is max queued send burst in bytes (0 with FloodRate 0 = unlimited).
	FloodBurst int
	// FloodRate is sustained uplink send rate in bytes/sec (0 = pacing disabled).
	FloodRate float64
	// AltNick is the optional second nick tried when Nick is taken (nick recovery).
	AltNick string
	// NickRecovery enables nick ladder + ISON reclaim (default true for new networks).
	NickRecovery bool
	// TLSNoVerify skips uplink TLS certificate and hostname verification (self-signed / mismatched CN).
	TLSNoVerify bool
	// TLSCert / TLSKey are optional uplink client identity paths for this network.
	// Empty inherits gobnc.json tls_client_cert/key; "none" or "-" disables.
	TLSCert string
	TLSKey  string
	// BindHost is the local address for uplink dials on this network.
	// Empty inherits gobnc.json bind_host; "none" or "-" disables.
	BindHost string
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
		INSERT INTO networks (name, host, port, tls, nick, username, realname, pass, sasl_user, sasl_pass, sasl, sasl_required, enabled, flood_burst, flood_rate, alt_nick, nick_recovery, tls_noverify, tls_cert, tls_key, bind_host)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			host=excluded.host, port=excluded.port, tls=excluded.tls, nick=excluded.nick,
			username=excluded.username, realname=excluded.realname, pass=excluded.pass,
			sasl_user=excluded.sasl_user, sasl_pass=excluded.sasl_pass, sasl=excluded.sasl,
			sasl_required=excluded.sasl_required, enabled=excluded.enabled,
			flood_burst=excluded.flood_burst, flood_rate=excluded.flood_rate,
			alt_nick=excluded.alt_nick, nick_recovery=excluded.nick_recovery,
			tls_noverify=excluded.tls_noverify,
			tls_cert=excluded.tls_cert, tls_key=excluded.tls_key,
			bind_host=excluded.bind_host
	`, n.Name, n.Host, n.Port, boolInt(n.TLS), n.Nick, n.Username, n.Realname, n.Pass,
		n.SASLUser, n.SASLPass, boolInt(n.SASL), boolInt(n.SASLRequired), boolInt(n.Enabled),
		n.FloodBurst, n.FloodRate, n.AltNick, boolInt(n.NickRecovery), boolInt(n.TLSNoVerify),
		n.TLSCert, n.TLSKey, n.BindHost)
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
		SELECT id, name, host, port, tls, nick, username, realname, pass, sasl_user, sasl_pass, sasl, sasl_required, enabled, flood_burst, flood_rate, alt_nick, nick_recovery, tls_noverify, tls_cert, tls_key, bind_host
		FROM networks ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Network
	for rows.Next() {
		var n Network
		var tls, saslOn, saslReq, en, nickRec, tlsNoVerify int
		if err := rows.Scan(&n.ID, &n.Name, &n.Host, &n.Port, &tls, &n.Nick, &n.Username, &n.Realname,
			&n.Pass, &n.SASLUser, &n.SASLPass, &saslOn, &saslReq, &en, &n.FloodBurst, &n.FloodRate, &n.AltNick, &nickRec, &tlsNoVerify,
			&n.TLSCert, &n.TLSKey, &n.BindHost); err != nil {
			return nil, err
		}
		n.TLS = tls != 0
		n.SASL = saslOn != 0
		n.SASLRequired = saslReq != 0
		n.Enabled = en != 0
		n.NickRecovery = nickRec != 0
		n.TLSNoVerify = tlsNoVerify != 0
		out = append(out, n)
	}
	return out, rows.Err()
}

// NetworkByName returns a network.
func (s *Store) NetworkByName(ctx context.Context, name string) (Network, error) {
	var n Network
	var tls, saslOn, saslReq, en, nickRec, tlsNoVerify int
	err := s.db.QueryRowContext(ctx, `
		SELECT id, name, host, port, tls, nick, username, realname, pass, sasl_user, sasl_pass, sasl, sasl_required, enabled, flood_burst, flood_rate, alt_nick, nick_recovery, tls_noverify, tls_cert, tls_key, bind_host
		FROM networks WHERE name=?`, name).Scan(
		&n.ID, &n.Name, &n.Host, &n.Port, &tls, &n.Nick, &n.Username, &n.Realname,
		&n.Pass, &n.SASLUser, &n.SASLPass, &saslOn, &saslReq, &en, &n.FloodBurst, &n.FloodRate, &n.AltNick, &nickRec, &tlsNoVerify,
		&n.TLSCert, &n.TLSKey, &n.BindHost)
	if err != nil {
		return n, err
	}
	n.TLS = tls != 0
	n.SASL = saslOn != 0
	n.SASLRequired = saslReq != 0
	n.Enabled = en != 0
	n.NickRecovery = nickRec != 0
	n.TLSNoVerify = tlsNoVerify != 0
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

// SetMessageMsgID sets msgid for a row when it was previously empty (playback backfill).
func (s *Store) SetMessageMsgID(ctx context.Context, id int64, msgid string) error {
	if msgid == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE messages SET msgid=? WHERE id=? AND (msgid='' OR msgid IS NULL)`, id, msgid)
	return err
}

// HistoryQuery parameters for CHATHISTORY-style queries.
type HistoryQuery struct {
	NetworkID int64
	Target    string
	Before    *time.Time
	After     *time.Time
	Around    *time.Time // center; Limit split before/after
	// BeforeBound / AfterBound / AroundBound are exclusive/centered positions from msgid=
	// selectors (time+row id). When set, they take precedence over Before/After/Around.
	BeforeBound *HistoryBound
	AfterBound  *HistoryBound
	AroundBound *HistoryBound
	Between    bool // if true, require both After/AfterBound and Before/BeforeBound
	Limit      int
	Latest     bool     // if true, return the Limit most recent (optionally before Before)
	Commands   []string // if non-empty, only these IRC commands
}

// HistoryBound is a message position in store order (time, then id).
type HistoryBound struct {
	Time time.Time
	ID   int64
}

// MessageByMsgID returns the stored message for msgid on network/target, or nil.
func (s *Store) MessageByMsgID(ctx context.Context, networkID int64, target, msgid string) (*Message, error) {
	if msgid == "" {
		return nil, nil
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT id, network_id, target, time, msgid, command, source, raw, text
		FROM messages WHERE network_id=? AND target=? AND msgid=?
		ORDER BY time ASC, id ASC LIMIT 1`, networkID, target, msgid)
	var m Message
	var ts string
	err := row.Scan(&m.ID, &m.NetworkID, &m.Target, &ts, &m.MsgID, &m.Command, &m.Source, &m.Raw, &m.Text)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		t, _ = time.Parse(time.RFC3339, ts)
	}
	m.Time = t
	return &m, nil
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

func beforeBoundArgs(b HistoryBound) []any {
	ts := formatTime(b.Time)
	return []any{ts, ts, b.ID}
}

func afterBoundArgs(b HistoryBound) []any {
	ts := formatTime(b.Time)
	return []any{ts, ts, b.ID}
}

const (
	sqlBeforeBound = `(time < ? OR (time = ? AND id < ?))`
	sqlAfterBound  = `(time > ? OR (time = ? AND id > ?))`
	sqlFromBound   = `(time > ? OR (time = ? AND id >= ?))` // after-or-equal (AROUND include)
)

// QueryMessages returns messages matching q, oldest-first.
// Limit == 0 defaults to 100. Limit < 0 means no SQL LIMIT (unlimited).
func (s *Store) QueryMessages(ctx context.Context, q HistoryQuery) ([]Message, error) {
	unlimited := q.Limit < 0
	if q.Limit == 0 {
		q.Limit = 100
	}
	where, baseArgs := historyWhere(q)
	cols := `id, network_id, target, time, msgid, command, source, raw, text`
	var rows *sql.Rows
	var err error
	switch {
	case q.AroundBound != nil || q.Around != nil:
		if unlimited {
			q.Limit = 100
		}
		half := q.Limit / 2
		if half < 1 {
			half = 1
		}
		var before []Message
		var after []Message
		if q.AroundBound != nil {
			b := *q.AroundBound
			args := append(append([]any{}, baseArgs...), beforeBoundArgs(b)...)
			args = append(args, half)
			beforeRows, err1 := s.db.QueryContext(ctx, `
				SELECT `+cols+` FROM (
					SELECT `+cols+`
					FROM messages WHERE `+where+` AND `+sqlBeforeBound+`
					ORDER BY time DESC, id DESC LIMIT ?
				) ORDER BY time ASC, id ASC`, args...)
			if err1 != nil {
				return nil, err1
			}
			before, err1 = scanMessages(beforeRows)
			beforeRows.Close()
			if err1 != nil {
				return nil, err1
			}
			rest := q.Limit - len(before)
			if rest < 1 {
				rest = 1
			}
			args = append(append([]any{}, baseArgs...), formatTime(b.Time), formatTime(b.Time), b.ID, rest)
			afterRows, err1 := s.db.QueryContext(ctx, `
				SELECT `+cols+`
				FROM messages WHERE `+where+` AND `+sqlFromBound+`
				ORDER BY time ASC, id ASC LIMIT ?`, args...)
			if err1 != nil {
				return nil, err1
			}
			after, err1 = scanMessages(afterRows)
			afterRows.Close()
			if err1 != nil {
				return nil, err1
			}
		} else {
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
			before, err1 = scanMessages(beforeRows)
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
			after, err1 = scanMessages(afterRows)
			afterRows.Close()
			if err1 != nil {
				return nil, err1
			}
		}
		return append(before, after...), nil
	case q.Between && ((q.AfterBound != nil && q.BeforeBound != nil) || (q.After != nil && q.Before != nil)):
		if q.AfterBound != nil && q.BeforeBound != nil {
			args := append(append([]any{}, baseArgs...), afterBoundArgs(*q.AfterBound)...)
			args = append(args, beforeBoundArgs(*q.BeforeBound)...)
			if unlimited {
				rows, err = s.db.QueryContext(ctx, `
					SELECT `+cols+`
					FROM messages WHERE `+where+` AND `+sqlAfterBound+` AND `+sqlBeforeBound+`
					ORDER BY time ASC, id ASC`, args...)
			} else {
				args = append(args, q.Limit)
				rows, err = s.db.QueryContext(ctx, `
					SELECT `+cols+`
					FROM messages WHERE `+where+` AND `+sqlAfterBound+` AND `+sqlBeforeBound+`
					ORDER BY time ASC, id ASC LIMIT ?`, args...)
			}
		} else if unlimited {
			args := append(append([]any{}, baseArgs...), formatTime(*q.After), formatTime(*q.Before))
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+`
				FROM messages WHERE `+where+` AND time > ? AND time < ?
				ORDER BY time ASC`, args...)
		} else {
			args := append(append([]any{}, baseArgs...), formatTime(*q.After), formatTime(*q.Before), q.Limit)
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+`
				FROM messages WHERE `+where+` AND time > ? AND time < ?
				ORDER BY time ASC LIMIT ?`, args...)
		}
	case q.Latest:
		filter, fargs := latestFilter(q)
		if unlimited {
			args := append(append([]any{}, baseArgs...), fargs...)
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+`
				FROM messages WHERE `+where+filter+`
				ORDER BY time ASC, id ASC`, args...)
		} else {
			args := append(append([]any{}, baseArgs...), fargs...)
			args = append(args, q.Limit)
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+` FROM (
					SELECT `+cols+`
					FROM messages WHERE `+where+filter+`
					ORDER BY time DESC, id DESC LIMIT ?
				) ORDER BY time ASC, id ASC`, args...)
		}
	case q.BeforeBound != nil && q.AfterBound == nil && q.After == nil:
		if unlimited {
			args := append(append([]any{}, baseArgs...), beforeBoundArgs(*q.BeforeBound)...)
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+`
				FROM messages WHERE `+where+` AND `+sqlBeforeBound+`
				ORDER BY time ASC, id ASC`, args...)
		} else {
			args := append(append([]any{}, baseArgs...), beforeBoundArgs(*q.BeforeBound)...)
			args = append(args, q.Limit)
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+` FROM (
					SELECT `+cols+`
					FROM messages WHERE `+where+` AND `+sqlBeforeBound+`
					ORDER BY time DESC, id DESC LIMIT ?
				) ORDER BY time ASC, id ASC`, args...)
		}
	case q.Before != nil && q.After == nil && q.AfterBound == nil:
		if unlimited {
			args := append(append([]any{}, baseArgs...), formatTime(*q.Before))
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+`
				FROM messages WHERE `+where+` AND time < ?
				ORDER BY time ASC`, args...)
		} else {
			args := append(append([]any{}, baseArgs...), formatTime(*q.Before), q.Limit)
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+` FROM (
					SELECT `+cols+`
					FROM messages WHERE `+where+` AND time < ?
					ORDER BY time DESC LIMIT ?
				) ORDER BY time ASC`, args...)
		}
	case q.AfterBound != nil:
		if unlimited {
			args := append(append([]any{}, baseArgs...), afterBoundArgs(*q.AfterBound)...)
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+`
				FROM messages WHERE `+where+` AND `+sqlAfterBound+`
				ORDER BY time ASC, id ASC`, args...)
		} else {
			args := append(append([]any{}, baseArgs...), afterBoundArgs(*q.AfterBound)...)
			args = append(args, q.Limit)
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+`
				FROM messages WHERE `+where+` AND `+sqlAfterBound+`
				ORDER BY time ASC, id ASC LIMIT ?`, args...)
		}
	case q.After != nil:
		if unlimited {
			args := append(append([]any{}, baseArgs...), formatTime(*q.After))
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+`
				FROM messages WHERE `+where+` AND time > ?
				ORDER BY time ASC`, args...)
		} else {
			args := append(append([]any{}, baseArgs...), formatTime(*q.After), q.Limit)
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+`
				FROM messages WHERE `+where+` AND time > ?
				ORDER BY time ASC LIMIT ?`, args...)
		}
	default:
		if unlimited {
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+`
				FROM messages WHERE `+where+`
				ORDER BY time ASC`, baseArgs...)
		} else {
			args := append(append([]any{}, baseArgs...), q.Limit)
			rows, err = s.db.QueryContext(ctx, `
				SELECT `+cols+` FROM (
					SELECT `+cols+`
					FROM messages WHERE `+where+`
					ORDER BY time DESC LIMIT ?
				) ORDER BY time ASC`, args...)
		}
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMessages(rows)
}

// latestFilter returns AND-clause + args for LATEST's optional after-excluding selector.
func latestFilter(q HistoryQuery) (string, []any) {
	if q.AfterBound != nil {
		return " AND " + sqlAfterBound, afterBoundArgs(*q.AfterBound)
	}
	if q.After != nil {
		return " AND time > ?", []any{formatTime(*q.After)}
	}
	return "", nil
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

// GetPlaybackCursor returns the legacy attach-playback watermark for target.
func (s *Store) GetPlaybackCursor(ctx context.Context, networkID int64, target string) (ts string, ok bool, err error) {
	err = s.db.QueryRowContext(ctx,
		`SELECT time FROM playback_cursors WHERE network_id=? AND target=?`,
		networkID, target).Scan(&ts)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return ts, true, nil
}

// SetPlaybackCursorIfNewer advances the legacy playback watermark when ts is newer.
func (s *Store) SetPlaybackCursorIfNewer(ctx context.Context, networkID int64, target, ts string) (stored string, updated bool, err error) {
	cur, ok, err := s.GetPlaybackCursor(ctx, networkID, target)
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
		INSERT INTO playback_cursors (network_id, target, time) VALUES (?, ?, ?)
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
