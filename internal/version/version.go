// Package version holds the GoBNC release version string.
package version

// Version is the application version (semver). Override at link time with:
//
//	go build -ldflags "-X github.com/MasterBOFH/GoBNC/internal/version.Version=1.2.3"
var Version = "0.1.0"

// QuitMessage is the default uplink QUIT reason on process shutdown.
func QuitMessage() string {
	return "GoBNC " + Version
}
