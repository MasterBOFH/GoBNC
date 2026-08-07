#!/usr/bin/env bash
# Build .deb packages with nfpm for linux amd64 and arm64.
# Usage: VERSION=1.2.3 ./packaging/linux/build-deb.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
ver="${VERSION:?VERSION required (no leading v)}"
mkdir -p dist

if ! command -v nfpm >/dev/null 2>&1; then
  echo "nfpm not found; install from https://github.com/goreleaser/nfpm/releases" >&2
  exit 1
fi

for goarch in amd64 arm64; do
  case "$goarch" in
    amd64) debarch=amd64 ;;
    arm64) debarch=arm64 ;;
  esac
  bin="dist/gobnc_${ver}_linux_${goarch}"
  if [[ ! -x "$bin" ]]; then
    echo "missing binary $bin (build it first)" >&2
    exit 1
  fi
  cfg="$(mktemp)"
  sed -e "s/\${ARCH}/${debarch}/g" \
      -e "s/\${VERSION}/${ver}/g" \
      -e "s/\${GOARCH}/${goarch}/g" \
      packaging/nfpm.yaml >"$cfg"
  out="dist/gobnc_${ver}_linux_${goarch}.deb"
  echo "building $out"
  nfpm package --packager deb --config "$cfg" --target "$out"
  rm -f "$cfg"
done
