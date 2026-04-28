#!/usr/bin/env bash
set -euo pipefail

REPO="KristjanPikhof/Auto-Commit-Daemon"
INSTALL_DIR="${ACD_INSTALL_DIR:-$HOME/.local/bin}"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$OS" in
  darwin|linux) ;;
  *) echo "unsupported os: $OS" >&2; exit 1 ;;
esac

ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64)  ARCH=amd64 ;;
  arm64|aarch64) ARCH=arm64 ;;
  *) echo "unsupported arch: $ARCH" >&2; exit 1 ;;
esac

VERSION="${ACD_VERSION:-}"
if [ -z "$VERSION" ]; then
  VERSION="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
    | grep '"tag_name"' | head -1 | sed -E 's/.*"([^"]+)".*/\1/')"
fi
[ -n "$VERSION" ] || { echo "could not resolve latest acd version" >&2; exit 1; }

# Tag carries leading "v" (e.g. v2026-04-28); goreleaser archive names omit it.
VERSION_NUM="${VERSION#v}"

URL="https://github.com/$REPO/releases/download/$VERSION/acd_${VERSION_NUM}_${OS}_${ARCH}.tar.gz"
SUMS_URL="https://github.com/$REPO/releases/download/$VERSION/checksums.txt"

mkdir -p "$INSTALL_DIR"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "Downloading $URL"
curl -fsSL "$URL" -o "$TMP/acd.tar.gz"
curl -fsSL "$SUMS_URL" -o "$TMP/checksums.txt"

# Verify checksum (works on both macOS and Linux)
if command -v sha256sum >/dev/null 2>&1; then
  ( cd "$TMP" && grep "acd_${VERSION}_${OS}_${ARCH}.tar.gz" checksums.txt | sha256sum -c - )
elif command -v shasum >/dev/null 2>&1; then
  ( cd "$TMP" && grep "acd_${VERSION}_${OS}_${ARCH}.tar.gz" checksums.txt | shasum -a 256 -c - )
else
  echo "warning: no sha256 verifier found; skipping checksum check" >&2
fi

tar -xzf "$TMP/acd.tar.gz" -C "$TMP"
install -m 0755 "$TMP/acd" "$INSTALL_DIR/acd"

echo
echo "Installed acd $VERSION to $INSTALL_DIR/acd"
echo
echo "Next:"
echo "  1) Make sure $INSTALL_DIR is on your PATH"
echo "  2) Run: acd init <claude-code|codex|opencode|pi|shell>"
echo "  3) Follow the printed snippet to wire up your harness"
