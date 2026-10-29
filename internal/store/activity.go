package store

import (
	"context"
	"database/sql"
	"time"
)

// MaxActivityRows caps the unread rows read per buffer; a larger backlog reports this count.
const MaxActivityRows = 500

// Activity is one buffer's unread state: its newest message, and the unread conversation
// lines (texts kept for highlight checks).
type Activity struct {
	LastID   int64
	LastTime time.Time
	Unread   []UnreadLine
}

// UnreadLine is an unread PRIVMSG/NOTICE.
type UnreadLine struct {
	ID      int64
	Command string
	Source  string
	Text    string
}

// BufferActivity reads target's newest message and its unread lines: PRIVMSG/NOTICE with time
// after marker (zero = nothing read) and id above floorID, CTCP other than ACTION left out,
// newest MaxActivityRows at most. Our own lines are for the caller to drop (it knows our nick).
func (s *Store) BufferActivity(ctx context.Context, networkID int64, target string, marker time.Time, floorID int64) (Activity, error) {
	var a Activity
	var id sql.NullInt64
	var ts sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT id, time FROM messages WHERE network_id=? AND target=?
		ORDER BY id DESC LIMIT 1`, networkID, target).Scan(&id, &ts)
	if err == sql.ErrNoRows {
		return a, nil
	}
	if err != nil {
		return a, err
	}
	a.LastID = id.Int64
	a.LastTime, _ = time.Parse(time.RFC3339Nano, ts.String)
	rows, err := s.db.QueryContext(ctx, `SELECT id, command, source, text FROM messages
		WHERE network_id=? AND target=? AND time>? AND id>? AND command IN ('PRIVMSG','NOTICE')
		  AND (substr(text, 1, 1) != char(1) OR substr(text, 1, 7) = char(1) || 'ACTION')
		ORDER BY id DESC LIMIT ?`, networkID, target, formatTime(marker), floorID, MaxActivityRows)
	if err != nil {
		return a, err
	}
	defer rows.Close()
	for rows.Next() {
		var l UnreadLine
		if err := rows.Scan(&l.ID, &l.Command, &l.Source, &l.Text); err != nil {
			return a, err
		}
		a.Unread = append(a.Unread, l)
	}
	return a, rows.Err()
}
