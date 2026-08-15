package session

import (
	"encoding/json"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
)

// applyState updates cached session state from an upstream (IRC server) message.
//
// blobIsupport/blobCloak/blobSelfNick collect what the locked dispatch
// below learned that's worth pushing to the keeper's blob store (see
// docs/keeper-design.md) — always collected as plain values here and
// pushed only after s.mu.Unlock(), never while holding it: PushBlob is a
// blocking wire round trip, and holding a lock this broad across one would
// stall every other access to this Session for the duration, the same
// reason persist/remove (channel persistence, below) were already handled
// this way before blob pushes existed at all.
func (s *Session) applyState(msg irc.Message) {
	var persist [][2]string // name, key
	var remove []string
	var blobOnlyJoins []string
	var blobIsupport, blobCloak, blobSelfNick []byte

	s.mu.Lock()
	prevSelfHost := ""
	if s.self != nil {
		prevSelfHost = s.self.Host
	}
	if msg.Source != "" {
		s.touchUserFromPrefixLocked(msg.Source)
	}
	// Any uplink line sourced ":ournick!user@host" is authoritative for our
	// own identity, regardless of which command carries it — enumerating
	// every numeric that might reveal a host change (CHGHOST, RPL_VISIBLEHOST/
	// 396, a SASL RPL_LOGGEDIN mask, whatever an ircd-specific "changed
	// host" numeric happens to be called this week) is exactly the fragile
	// approach this replaces: a self-JOIN echo, a self-echoed PRIVMSG
	// (echo-message), a self-KICK-as-kicker, anything at all works
	// uniformly, with no per-command allowlist to keep up to date. Checked
	// generically here, before the switch below, purely by diffing
	// self.Host across the touchUserFromPrefixLocked call above — that
	// call already updates whichever user the source names, self included,
	// so a change here means this message's source really was self's.
	// CHGHOST is the one command whose *source* is deliberately the old
	// identity (the new one only ever arrives in its params) — its own
	// case below overwrites blobCloak with the correct value in the
	// normal case; on a malformed CHGHOST with no new-host param it
	// leaves this generic (and, in that specific case, still-current)
	// value in place instead, which is a harmless redundant re-push, not
	// a staleness bug.
	if s.self != nil && s.self.Host != "" && s.self.Host != prevSelfHost {
		blobCloak = []byte(s.self.Host)
	}
	switch msg.Command {
	// Commands
	case "JOIN":
		p, b := s.stateJOINLocked(msg)
		persist = append(persist, p...)
		blobOnlyJoins = append(blobOnlyJoins, b...)
	case "PART", "KICK":
		remove = append(remove, s.statePartKickLocked(msg)...)
	case "QUIT":
		s.stateQUITLocked(msg)
	case "TOPIC":
		s.stateTOPICLocked(msg)
	case "NICK":
		blobSelfNick = s.stateNICKLocked(msg)
	case "AWAY":
		s.stateAWAYLocked(msg)
	case "CHGHOST":
		blobCloak = s.stateCHGHOSTLocked(msg)
	case "ACCOUNT":
		s.stateACCOUNTLocked(msg)
	case "MODE":
		persist = append(persist, s.stateMODELocked(msg)...)
	// Numerics
	case "002", "003", "004", "005":
		blobIsupport = s.stateWelcomeNumericLocked(msg)
	case "221":
		s.state221Locked(msg)
	case "301":
		s.state301Locked(msg)
	case "302":
		blobCloak = s.state302Locked(msg)
	case "305":
		s.state305Locked(msg)
	case "306":
		s.state306Locked(msg)
	case "332":
		s.state332Locked(msg)
	case "352":
		s.state352Locked(msg)
	case "353":
		s.state353Locked(msg)
	case "366":
		s.state366Locked(msg)
	case "381":
		s.state381Locked(msg)
	case "396":
		blobCloak = s.state396Locked(msg)
	}
	s.mu.Unlock()

	for _, p := range persist {
		s.persistChannel(p[0], p[1])
	}
	for _, name := range blobOnlyJoins {
		s.pushChannelBlobFromStore(name)
	}
	for _, name := range remove {
		s.persistRemoveChannel(name)
	}
	if blobIsupport != nil {
		s.pushBlob("isupport", keeper.BlobModeAppend, blobIsupport)
	}
	if blobCloak != nil {
		s.pushBlob("cloak", keeper.BlobModeReplace, blobCloak)
	}
	if blobSelfNick != nil {
		s.pushBlob("self-nick", keeper.BlobModeReplace, blobSelfNick)
	}
}

// blobParams JSON-encodes msg.Params for storage as one isupport blob
// entry — see stateWelcomeNumericLocked's 005 case. Encoding failure is
// not possible for a []string (json.Marshal on a slice of plain strings
// never errors), so this deliberately has no error return to check.
func blobParams(params []string) []byte {
	b, _ := json.Marshal(params)
	return b
}
