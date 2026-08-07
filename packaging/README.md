# GoBNC packaging

Init/service units and CI package builders for release assets.

| Path | What it produces |
|------|------------------|
| `linux/build-deb.sh` + `nfpm.yaml` | `.deb` with binary + systemd unit + `gobnc.json.example` |
| `macos/build-pkg.sh` | unsigned macOS `.pkg` with binary + LaunchDaemon + example config |
| `freebsd/build-tarball.sh` | `.tar.gz` with binary + `rc.d/gobnc` + example config |
| `systemd/gobnc.service` | Linux systemd unit (`-foreground`) |
| `freebsd/rc.d/gobnc` | FreeBSD rc script (`daemon(8)` + `-foreground`) |

Release workflow (tag `v*`):

1. Build linux/freebsd/darwin binaries
2. Build `.deb` (amd64, arm64) via nfpm
3. Build Darwin `.pkg` (amd64, arm64) via `pkgbuild`
4. Bundle FreeBSD install tarballs with rc.d
5. Attach everything to the GitHub Release

Supervised installs always use `-foreground`; self-daemonization is for interactive `gobnc serve` only.
