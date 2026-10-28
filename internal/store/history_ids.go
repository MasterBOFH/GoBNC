package store

import (
	"context"
	"database/sql"
)

// MaxMessageID is the newest messages.id stored for networkID (0 when none).
func (s *Store) MaxMessageID(ctx context.Context, networkID int64) (int64, error) {
	var id sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(id) FROM messages WHERE network_id=?`, networkID).Scan(&id); err != nil {
		return 0, err
	}
	return id.Int64, nil
}
