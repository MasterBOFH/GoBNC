package store

import (
	"context"
	"database/sql"
	"strings"
)

// module_kv is a small key/value store per module (storage.* in the module API), namespaced by
// module name.
var moduleKVMigrations = []string{
	`CREATE TABLE IF NOT EXISTS module_kv (
		module TEXT NOT NULL,
		key TEXT NOT NULL,
		value TEXT NOT NULL,
		PRIMARY KEY(module, key)
	)`,
}

// ModuleKVGet returns module's value for key.
func (s *Store) ModuleKVGet(ctx context.Context, module, key string) (value string, found bool, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT value FROM module_kv WHERE module=? AND key=?`, module, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	return value, err == nil, err
}

// ModuleKVSet stores module's value for key.
func (s *Store) ModuleKVSet(ctx context.Context, module, key, value string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO module_kv (module, key, value) VALUES (?, ?, ?)
		ON CONFLICT(module, key) DO UPDATE SET value=excluded.value`, module, key, value)
	return err
}

// ModuleKVDelete removes module's key (no error when absent).
func (s *Store) ModuleKVDelete(ctx context.Context, module, key string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM module_kv WHERE module=? AND key=?`, module, key)
	return err
}

// ModuleKVList returns module's keys starting with prefix, sorted.
func (s *Store) ModuleKVList(ctx context.Context, module, prefix string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key FROM module_kv WHERE module=? ORDER BY key`, module)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := []string{}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, rows.Err()
}
