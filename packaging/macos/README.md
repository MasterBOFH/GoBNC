# macOS packages

Release CI (macos runner) builds unsigned `.pkg` installers:

- `gobnc_${version}_darwin_amd64.pkg`
- `gobnc_${version}_darwin_arm64.pkg`

Each package installs:

| Path | Content |
|------|---------|
| `/usr/local/bin/gobnc` | binary |
| `/usr/local/etc/gobnc/gobnc.json.example` | copy of repo `gobnc.json.example` |
| `/Library/LaunchDaemons/org.gobnc.bouncer.plist` | launchd (`-foreground`) |

`postinstall` creates state/log dirs and copies the example to `gobnc.json` if missing.
Load with:

```bash
sudo launchctl load -w /Library/LaunchDaemons/org.gobnc.bouncer.plist
```

Packages are unsigned (ad-hoc). Sign/notarize separately if distributing outside GitHub Releases.

Local build on macOS:

```bash
VERSION=1.2.3 ./packaging/macos/build-pkg.sh
```
