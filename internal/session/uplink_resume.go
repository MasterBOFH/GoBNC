package session

import (
	"context"
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// Uplink-side draft/resume-0.5 (internal/registration/resume.go has the
// protocol; this file is the persistence half). Registration surfaces the
// issued token as registration.ActionResumeToken for brain.Driver's
// in-memory copy; Session, which sees the same line on Driver.Lines(),
// is what writes it down — in SQLite, next to the other per-network
// facts it already persists (persistChannel), for the reasons on
// store.Store.SetResumeToken. Not the keeper's blob store: that's cleared
// on the very disconnect this token exists to survive.

// resumeTokenFrom returns the token carried by a server→client
// "RESUME TOKEN <token>" line, or "" for any other RESUME form.
func resumeTokenFrom(msg irc.Message) string {
	if !strings.EqualFold(msg.Param(0), "TOKEN") {
		return ""
	}
	return msg.Param(1)
}

// persistResumeToken stores the token the ircd just issued this
// connection. Must be called without s.mu held (a SQLite write).
func (s *Session) persistResumeToken(token string) {
	if s.store == nil || s.Network.ID == 0 {
		return
	}
	if err := s.store.SetResumeToken(context.Background(), s.Network.ID, token); err != nil {
		s.log.Error("persist resume token", "err", err)
	}
}

// clearResumeToken forgets the stored token — for when the server-side
// session it resumes is deliberately ended (QUIT). Must be called without
// s.mu held.
func (s *Session) clearResumeToken() {
	if s.store == nil || s.Network.ID == 0 {
		return
	}
	if err := s.store.SetResumeToken(context.Background(), s.Network.ID, ""); err != nil {
		s.log.Error("clear resume token", "err", err)
	}
}
