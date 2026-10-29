package store

import (
	"context"
	"database/sql"
	"time"
)

// MaxMessageID is the newest messages.id stored for networkID (0 when none).
func (s *Store) MaxMessageID(ctx context.Context, networkID int64) (int64, error) {
	var id sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(id) FROM messages WHERE network_id=?`, networkID).Scan(&id); err != nil {
		return 0, err
	}
	return id.Int64, nil
}

// HasTarget reports whether any message is stored for (networkID, target); target is the
// folded name historyTargets stores under.
func (s *Store) HasTarget(ctx context.Context, networkID int64, target string) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM messages WHERE network_id=? AND target=? LIMIT 1`, networkID, target).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// IDAtOrBefore is the newest message id of (networkID, target) with time ≤ t (0 when none).
func (s *Store) IDAtOrBefore(ctx context.Context, networkID int64, target string, t time.Time) (int64, error) {
	var id sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT MAX(id) FROM messages WHERE network_id=? AND target=? AND time<=?`,
		networkID, target, formatTime(t)).Scan(&id)
	return id.Int64, err
}
