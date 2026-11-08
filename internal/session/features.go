package session

import "github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"

// StateFeatures is the state a Session keeps, or asks the server for, only for modules that
// need it (gobnc-retecloud's lite sync): IRC clients poll for it themselves, so an IRC-only
// bouncer pays nothing for it. internal/server computes the mask from the features configured
// modules are granted (wire.FeatureMemberMeta, ...) and sets it with SetStateFeatures; the
// zero mask is core alone.
type StateFeatures uint32

const (
	// StateMemberMeta: member hosts, accounts and flags — userhost-in-names is requested and
	// GoBNC polls WHOX itself (PollWHO).
	StateMemberMeta StateFeatures = 1 << iota
	// StateSilence: the SILENCE list is queried after registration (RefreshSilence).
	StateSilence
	// StateChannelMeta: channel modes and topics are re-asked after a brain reload.
	StateChannelMeta
)

// StateFeaturesOf is the mask the named module features grant.
func StateFeaturesOf(features []string) StateFeatures {
	var f StateFeatures
	for _, name := range features {
		switch name {
		case wire.FeatureMemberMeta:
			f |= StateMemberMeta
		case wire.FeatureSilence:
			f |= StateSilence
		case wire.FeatureChannelMeta:
			f |= StateChannelMeta
		}
	}
	return f
}

// Has reports whether every feature in want is in the mask.
func (f StateFeatures) Has(want StateFeatures) bool { return f&want == want }

// SetStateFeatures sets which module-only state the Session keeps (see StateFeatures). A change
// applies from the next decision it gates (the next poll, CAP NEW, registration).
func (s *Session) SetStateFeatures(f StateFeatures) { s.features.Store(uint32(f)) }

// StateFeatures returns the mask SetStateFeatures last set.
func (s *Session) StateFeatures() StateFeatures { return StateFeatures(s.features.Load()) }
