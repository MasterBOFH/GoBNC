package store

import (
	"context"
	"database/sql"
)

// network_last_nick is the nick each network last had on its uplink (the alternate after a
// collision, a nick changed later), so a stopped network still knows which nick was ours.
var lastNickMigrations = []string{
	`CREATE TABLE IF NOT EXISTS network_last_nick (
		network_id INTEGER PRIMARY KEY REFERENCES networks(id) ON DELETE CASCADE,
		nick TEXT NOT NULL
	)`,
}

// SetLastNick records nick as the one the network last had.
func (s *Store) SetLastNick(ctx context.Context, networkID int64, nick string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO network_last_nick (network_id, nick) VALUES (?, ?)
		ON CONFLICT(network_id) DO UPDATE SET nick=excluded.nick`, networkID, nick)
	return err
}

// LastNick returns the nick the network last had, "" when none was recorded.
func (s *Store) LastNick(ctx context.Context, networkID int64) (string, error) {
	var nick string
	err := s.db.QueryRowContext(ctx, `SELECT nick FROM network_last_nick WHERE network_id=?`, networkID).Scan(&nick)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return nick, err
}
