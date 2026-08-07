# Debian packages

Release CI builds `.deb` files with [nfpm](https://github.com/goreleaser/nfpm):

- `gobnc_${version}_linux_amd64.deb`
- `gobnc_${version}_linux_arm64.deb`

Each package installs:

| Path | Content |
|------|---------|
| `/usr/bin/gobnc` | binary |
| `/lib/systemd/system/gobnc.service` | systemd unit |
| `/etc/gobnc/gobnc.json.example` | copy of repo `gobnc.json.example` |

`postinstall` creates the `gobnc` user/group and state dirs, and copies the example
to `/etc/gobnc/gobnc.json` only if that file does not already exist.

Local build (after linux binaries exist in `dist/`):

```bash
# install nfpm, then:
VERSION=1.2.3 ./packaging/linux/build-deb.sh
```
