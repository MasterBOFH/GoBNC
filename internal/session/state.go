package session

import "github.com/MasterBOFH/GoBNC/internal/irc"

// applyState updates cached session state from an upstream (IRC server) message.
func (s *Session) applyState(msg irc.Message) {
	var persist [][2]string // name, key
	var remove []string

	s.mu.Lock()
	if msg.Source != "" {
		s.touchUserFromPrefixLocked(msg.Source)
	}
	switch msg.Command {
	// Commands
	case "JOIN":
		s.stateJOINLocked(msg)
	case "PART", "KICK":
		remove = append(remove, s.statePartKickLocked(msg)...)
	case "QUIT":
		s.stateQUITLocked(msg)
	case "TOPIC":
		s.stateTOPICLocked(msg)
	case "NICK":
		s.stateNICKLocked(msg)
	case "AWAY":
		s.stateAWAYLocked(msg)
	case "CHGHOST":
		s.stateCHGHOSTLocked(msg)
	case "ACCOUNT":
		s.stateACCOUNTLocked(msg)
	case "MODE":
		persist = append(persist, s.stateMODELocked(msg)...)
	// Numerics
	case "002", "003", "004", "005":
		s.stateWelcomeNumericLocked(msg)
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
		s.state396Locked(msg)
	}
	s.mu.Unlock()

	for _, p := range persist {
		s.persistChannel(p[0], p[1])
	}
	for _, name := range remove {
		s.persistRemoveChannel(name)
	}
}
