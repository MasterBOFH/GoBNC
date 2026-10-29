package retecloud

// flushActivity sends one activity frame with absolute counts for every dirty buffer.
func (m *Module) flushActivity(c *wsConn, ls *liteSession) {
	ls.stopTimer()
	entries := make([]liteActivityEntry, 0, len(ls.dirty))
	for key := range ls.dirty {
		if a := ls.activity[key]; a != nil {
			entries = append(entries, liteActivityEntry{Buffer: a.name, Unread: a.unread, Highlights: a.highlights, LastID: a.lastID})
		}
	}
	ls.dirty = map[string]bool{}
	if len(entries) == 0 {
		return
	}
	c.send(ServerMessage{Type: frameActivity, SessionID: sessionID(ls.network), Data: map[string]interface{}{"buffers": entries}})
}
