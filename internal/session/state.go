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
	var blobIsupport, blobCloak, blobSelfNick []byte

	s.mu.Lock()
	if msg.Source != "" {
		s.touchUserFromPrefixLocked(msg.Source)
	}
	switch msg.Command {
	// Commands
	case "JOIN":
		if p := s.stateJOINLocked(msg); p != nil {
			persist = append(persist, p...)
		}
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
	case "381":
		s.state381Locked(msg)
	case "396":
		blobCloak = s.state396Locked(msg)
	}
	s.mu.Unlock()

	for _, p := range persist {
		s.persistChannel(p[0], p[1])
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
