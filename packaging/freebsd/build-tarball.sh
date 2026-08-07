#!/usr/bin/env bash
# Bundle FreeBSD binary + rc.d script (native .pkg needs a FreeBSD builder).
# Usage: VERSION=1.2.3 ./packaging/freebsd/build-tarball.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
ver="${VERSION:?VERSION required (no leading v)}"
mkdir -p dist

for goarch in amd64 arm64; do
  bin="dist/gobnc_${ver}_freebsd_${goarch}"
  if [[ ! -f "$bin" ]]; then
    echo "missing binary $bin (build it first)" >&2
    exit 1
  fi
  stage="$(mktemp -d)"
  mkdir -p "$stage/usr/local/bin" "$stage/usr/local/etc/rc.d" "$stage/usr/local/etc/gobnc"
  cp "$bin" "$stage/usr/local/bin/gobnc"
  chmod 755 "$stage/usr/local/bin/gobnc"
  cp packaging/freebsd/rc.d/gobnc "$stage/usr/local/etc/rc.d/gobnc"
  chmod 555 "$stage/usr/local/etc/rc.d/gobnc"
  cp gobnc.json.example "$stage/usr/local/etc/gobnc/gobnc.json.example"
  chmod 644 "$stage/usr/local/etc/gobnc/gobnc.json.example"
  out="dist/gobnc_${ver}_freebsd_${goarch}.tar.gz"
  echo "building $out"
  tar -C "$stage" -czf "$out" usr
  rm -rf "$stage"
done
