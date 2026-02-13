#!/usr/bin/env bash
# Install lakebench binary from GitHub Releases.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/PureStorage-OpenConnect/lakebench-k8s/main/install.sh | bash
#
# Environment variables:
#   INSTALL_DIR  -- Installation directory (default: /usr/local/bin)
#   VERSION      -- Specific version to install (default: latest)

set -euo pipefail

REPO="PureStorage-OpenConnect/lakebench-k8s"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

# Detect OS and architecture
RAW_OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$RAW_OS" in
  linux)   OS="linux" ;;
  darwin)  OS="macos" ;;
  *)
    echo "Error: unsupported OS: $RAW_OS" >&2
    exit 1
    ;;
esac

case "$ARCH" in
  x86_64)  ARCH="amd64" ;;
  aarch64) ARCH="arm64" ;;
  arm64)   ARCH="arm64" ;;
  *)
    echo "Error: unsupported architecture: $ARCH" >&2
    exit 1
    ;;
esac

BINARY="lakebench-${OS}-${ARCH}"

# Determine version
if [ -n "${VERSION:-}" ]; then
  TAG="v${VERSION#v}"
else
  echo "Detecting latest version..."
  TAG=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | cut -d'"' -f4)
  if [ -z "$TAG" ]; then
    echo "Error: could not determine latest version" >&2
    exit 1
  fi
fi

URL="https://github.com/${REPO}/releases/download/${TAG}/${BINARY}"

echo "Downloading lakebench ${TAG} for ${OS}/${ARCH}..."
if ! curl -fsSL "$URL" -o "${INSTALL_DIR}/lakebench"; then
  echo "Error: download failed. Check that ${BINARY} exists in release ${TAG}" >&2
  echo "Available binaries: https://github.com/${REPO}/releases/tag/${TAG}" >&2
  exit 1
fi

chmod +x "${INSTALL_DIR}/lakebench"
echo "Installed lakebench ${TAG} to ${INSTALL_DIR}/lakebench"

# Verify
if command -v lakebench &>/dev/null; then
  lakebench version
fi
