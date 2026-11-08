package store

import (
	"context"
	"database/sql"
	"strings"
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
	return s.exists(ctx, `SELECT 1 FROM messages WHERE network_id=? AND target=? LIMIT 1`, networkID, target)
}

// IDAtOrBefore is the newest message id of (networkID, target) with time ≤ t (0 when none).
// target "*" is the Server buffer (messagesOf), as in the Messages* reads.
func (s *Store) IDAtOrBefore(ctx context.Context, networkID int64, target string, t time.Time) (int64, error) {
	where, args, err := s.messagesOf(ctx, networkID, target)
	if err != nil {
		return 0, err
	}
	var id sql.NullInt64
	err = s.db.QueryRowContext(ctx, `SELECT MAX(id) FROM messages WHERE `+where+` AND time<=?`,
		append(args, formatTime(t))...).Scan(&id)
	return id.Int64, err
}

const idCols = `id, network_id, target, time, msgid, command, source, raw, text`

// queryBuffer runs a query on the rows of the buffer keyed target: query holds "%s" where its
// WHERE clause goes (messagesOf), args follow that clause's own.
func (s *Store) queryBuffer(ctx context.Context, networkID int64, target, query string, args ...any) ([]Message, error) {
	where, wargs, err := s.messagesOf(ctx, networkID, target)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, strings.Replace(query, "%s", where, 1), append(wargs, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMessages(rows)
}

func (s *Store) queryIDPage(ctx context.Context, query string, args ...any) ([]Message, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMessages(rows)
}

func reverseMessages(ms []Message) []Message {
	for i, j := 0, len(ms)-1; i < j; i, j = i+1, j-1 {
		ms[i], ms[j] = ms[j], ms[i]
	}
	return ms
}

// The Messages* queries page one buffer's history by messages.id (the module API's cursor),
// ascending. target is the folded name historyTargets stores under, or "*" for the Server
// buffer: every server-name target of the network together (messagesOf).

// MessagesBefore returns up to limit messages with id < before.
func (s *Store) MessagesBefore(ctx context.Context, networkID int64, target string, before int64, limit int) ([]Message, error) {
	ms, err := s.queryBuffer(ctx, networkID, target, `SELECT `+idCols+` FROM messages WHERE %s AND id<?
		ORDER BY id DESC LIMIT ?`, before, limit)
	return reverseMessages(ms), err
}

// MessagesAfter returns up to limit messages with id > after.
func (s *Store) MessagesAfter(ctx context.Context, networkID int64, target string, after int64, limit int) ([]Message, error) {
	return s.queryBuffer(ctx, networkID, target, `SELECT `+idCols+` FROM messages WHERE %s AND id>?
		ORDER BY id ASC LIMIT ?`, after, limit)
}

// MessagesBetween returns up to limit messages with after < id < before, oldest first: the
// gap fill's page, which starts at the gap's older edge.
func (s *Store) MessagesBetween(ctx context.Context, networkID int64, target string, after, before int64, limit int) ([]Message, error) {
	return s.queryBuffer(ctx, networkID, target, `SELECT `+idCols+` FROM messages WHERE %s AND id>? AND id<?
		ORDER BY id ASC LIMIT ?`, after, before, limit)
}

// MessagesAround returns up to limit messages centred on id: about half before it, the rest
// from it on.
func (s *Store) MessagesAround(ctx context.Context, networkID int64, target string, id int64, limit int) ([]Message, error) {
	half := limit / 2
	before, err := s.MessagesBefore(ctx, networkID, target, id, half)
	if err != nil {
		return nil, err
	}
	from, err := s.MessagesAfter(ctx, networkID, target, id-1, limit-len(before))
	if err != nil {
		return nil, err
	}
	return append(before, from...), nil
}

// MessagesLatest returns the newest limit messages.
func (s *Store) MessagesLatest(ctx context.Context, networkID int64, target string, limit int) ([]Message, error) {
	ms, err := s.queryBuffer(ctx, networkID, target, `SELECT `+idCols+` FROM messages WHERE %s
		ORDER BY id DESC LIMIT ?`, limit)
	return reverseMessages(ms), err
}

// HasMessageBefore reports whether a message with id < id exists.
func (s *Store) HasMessageBefore(ctx context.Context, networkID int64, target string, id int64) (bool, error) {
	ms, err := s.queryBuffer(ctx, networkID, target, `SELECT `+idCols+` FROM messages WHERE %s AND id<? ORDER BY id DESC LIMIT 1`, id)
	return len(ms) > 0, err
}

// HasMessageAfter reports whether a message with id > id exists.
func (s *Store) HasMessageAfter(ctx context.Context, networkID int64, target string, id int64) (bool, error) {
	ms, err := s.queryBuffer(ctx, networkID, target, `SELECT `+idCols+` FROM messages WHERE %s AND id>? ORDER BY id ASC LIMIT 1`, id)
	return len(ms) > 0, err
}

func (s *Store) exists(ctx context.Context, query string, args ...any) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// FirstUnreadID is the oldest message id after the read marker (time > marker; a zero marker
// means none read) and above floorID (a device's closed-at floor); 0 when none.
func (s *Store) FirstUnreadID(ctx context.Context, networkID int64, target string, marker time.Time, floorID int64) (int64, error) {
	where, args, err := s.messagesOf(ctx, networkID, target)
	if err != nil {
		return 0, err
	}
	var id sql.NullInt64
	err = s.db.QueryRowContext(ctx, `SELECT MIN(id) FROM messages WHERE `+where+` AND time>? AND id>?`,
		append(args, formatTime(marker), floorID)...).Scan(&id)
	return id.Int64, err
}

// MessageByID returns one message of networkID by id (nil when none).
func (s *Store) MessageByID(ctx context.Context, networkID, id int64) (*Message, error) {
	ms, err := s.queryIDPage(ctx, `SELECT `+idCols+` FROM messages WHERE network_id=? AND id=?`, networkID, id)
	if err != nil || len(ms) == 0 {
		return nil, err
	}
	return &ms[0], nil
}
