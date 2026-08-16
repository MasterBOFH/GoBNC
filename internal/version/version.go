// Package version holds GoBNC's release string and the independent
// brain/keeper compatibility numbers used to decide whether a running
// keeper can serve this binary, should be replaced (gobnc die then start)
// for a non-breaking keeper change, or must be replaced that way before
// this brain can attach.
package version

// Version is the application version (semver). Override at link time with:
//
//	go build -ldflags "-X github.com/MasterBOFH/GoBNC/internal/version.Version=1.2.3"
var Version = "0.1.1"

// BrainVersion is this binary's brain generation. Bump when the brain's
// attach identity should show as newer (brain-only changes that operators
// should see in status). Independent of KeeperVersion: a brain-only bump
// does not ask for a keeper replacement.
const BrainVersion = 1

// KeeperVersion is this binary's keeper generation. Bump on any change to
// keeper behavior or the keeper↔brain protocol surface, including additive
// JSON fields. Leave MinKeeperVersion behind on a non-breaking change so
// an already-running older keeper still accepts this brain.
const KeeperVersion = 1

// MinKeeperVersion is the oldest keeper generation this brain will attach
// to. Bump it (typically to equal KeeperVersion) on a breaking keeper
// change. Pre-versioning keepers omit keeper_version on HelloAck (0); see
// NormalizeKeeperVersion.
const MinKeeperVersion = 1

// Upgrade is the keeper-upgrade advice this binary computes against a
// running keeper's generation.
type Upgrade int

const (
	// UpgradeNone: running keeper is current (or newer).
	UpgradeNone Upgrade = iota
	// UpgradeShould: running keeper is older than this binary but still
	// compatible. Attach succeeds. gobnc die then start to spawn a new
	// keeper (drops uplinks).
	UpgradeShould
	// UpgradeMust: running keeper is below MinKeeperVersion. Attach must
	// fail. gobnc die then start this binary so it can spawn a new keeper.
	UpgradeMust
)

func (u Upgrade) String() string {
	switch u {
	case UpgradeShould:
		return "should"
	case UpgradeMust:
		return "must"
	default:
		return "none"
	}
}

// NormalizeKeeperVersion maps a keeper_version advertised on the wire to
// a generation number. Missing/0 (pre-versioning keepers) is generation 1,
// the implicit version of every keeper that shipped before these fields
// existed — so introducing versioning itself is not a breaking attach.
func NormalizeKeeperVersion(running int) int {
	if running <= 0 {
		return 1
	}
	return running
}

// ClassifyUpgrade is the pure comparison CanUpgrade wraps. running should
// already be normalized. Tests drive this with explicit current/min so a
// bump of the package constants cannot silently empty the table.
func ClassifyUpgrade(running, current, min int) Upgrade {
	if running < min {
		return UpgradeMust
	}
	if running < current {
		return UpgradeShould
	}
	return UpgradeNone
}

// CanUpgrade returns keeper-upgrade advice for a running keeper generation
// (the raw HelloAck.KeeperVersion, including 0 for an unversioned keeper)
// against this binary's KeeperVersion / MinKeeperVersion.
func CanUpgrade(runningKeeper int) Upgrade {
	return ClassifyUpgrade(NormalizeKeeperVersion(runningKeeper), KeeperVersion, MinKeeperVersion)
}

// QuitMessage is the default uplink QUIT reason on process shutdown.
func QuitMessage() string {
	return "GoBNC " + Version
}
