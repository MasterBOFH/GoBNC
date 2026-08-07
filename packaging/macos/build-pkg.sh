#!/usr/bin/env bash
# Build unsigned macOS .pkg installers for amd64 and arm64.
# Usage: VERSION=1.2.3 ./packaging/macos/build-pkg.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
ver="${VERSION:?VERSION required (no leading v)}"
mkdir -p dist

if ! command -v pkgbuild >/dev/null 2>&1; then
  echo "pkgbuild not found (macOS only)" >&2
  exit 1
fi

for goarch in amd64 arm64; do
  bin="dist/gobnc_${ver}_darwin_${goarch}"
  if [[ ! -x "$bin" && ! -f "$bin" ]]; then
    echo "missing binary $bin (build it first)" >&2
    exit 1
  fi
  chmod +x "$bin"
  root="$(mktemp -d)"
  scripts="$(mktemp -d)"
  mkdir -p "$root/usr/local/bin" \
           "$root/usr/local/etc/gobnc" \
           "$root/Library/LaunchDaemons"
  cp "$bin" "$root/usr/local/bin/gobnc"
  chmod 755 "$root/usr/local/bin/gobnc"
  cp gobnc.json.example "$root/usr/local/etc/gobnc/gobnc.json.example"
  chmod 644 "$root/usr/local/etc/gobnc/gobnc.json.example"
  cp packaging/macos/org.gobnc.bouncer.plist "$root/Library/LaunchDaemons/org.gobnc.bouncer.plist"
  chmod 644 "$root/Library/LaunchDaemons/org.gobnc.bouncer.plist"
  cp packaging/macos/scripts/postinstall "$scripts/postinstall"
  chmod 755 "$scripts/postinstall"

  out="dist/gobnc_${ver}_darwin_${goarch}.pkg"
  echo "building $out"
  pkgbuild \
    --root "$root" \
    --scripts "$scripts" \
    --identifier org.gobnc.bouncer \
    --version "$ver" \
    --install-location / \
    "$out"
  rm -rf "$root" "$scripts"
done
