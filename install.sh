#!/usr/bin/env bash
# Install lakebench binary from GitHub Releases.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/PureStorage-OpenConnect/lakebench-k8s/main/install.sh | bash
#
# Environment variables:
#   INSTALL_DIR  -- Installation directory (default: /usr/local/bin)
#   VERSION      -- Specific version to install (default: latest)
#
# The binary is downloaded to a temporary directory and checked against the
# SHA256SUMS file of the same release before it is moved into INSTALL_DIR,
# so a failed, truncated or corrupted download never leaves a lakebench
# there. SHA256SUMS comes from the same release as the binary: it detects a
# bad download, not a tampered release. Releases before 1.7.0 publish no
# SHA256SUMS and cannot be installed with this script.
#
# Everything runs inside main(), called on the last line: if the pipe from
# curl is cut short, bash is left with an unfinished function and runs
# nothing.

set -euo pipefail

REPO="PureStorage-OpenConnect/lakebench-k8s"
# The binaries release.yml publishes; the release also carries SHA256SUMS.
AVAILABLE="lakebench-linux-amd64 lakebench-macos-amd64 lakebench-macos-arm64"

# Removed on every exit, including a failed download or a bad checksum.
LB_TMP=""
LB_STAGED=""

cleanup() {
  if [ -n "$LB_STAGED" ]; then rm -f "$LB_STAGED"; fi
  if [ -n "$LB_TMP" ]; then rm -rf "$LB_TMP"; fi
}

die() {
  echo "Error: $*" >&2
  exit 1
}

main() {
  local install_dir="${INSTALL_DIR:-/usr/local/bin}"
  local raw_os arch os binary tag base hasher expected actual

  raw_os=$(uname -s | tr '[:upper:]' '[:lower:]')
  arch=$(uname -m)

  case "$raw_os" in
    linux)  os="linux" ;;
    darwin) os="macos" ;;
    *) die "unsupported OS: $raw_os (available binaries: $AVAILABLE)" ;;
  esac

  case "$arch" in
    x86_64 | amd64)  arch="amd64" ;;
    aarch64 | arm64) arch="arm64" ;;
    *) die "unsupported architecture: $arch (available binaries: $AVAILABLE)" ;;
  esac

  binary="lakebench-${os}-${arch}"
  case " $AVAILABLE " in
    *" $binary "*) ;;
    *)
      die "no ${binary} binary is published (available binaries: $AVAILABLE);" \
        "install from PyPI instead: pipx install lakebench-k8s"
      ;;
  esac

  if command -v sha256sum >/dev/null 2>&1; then
    hasher="sha256sum"
  elif command -v shasum >/dev/null 2>&1; then
    hasher="shasum -a 256"
  else
    die "neither sha256sum nor shasum is installed; cannot verify the download"
  fi

  [ -d "$install_dir" ] || die "INSTALL_DIR ${install_dir} does not exist"
  [ -w "$install_dir" ] \
    || die "cannot write to ${install_dir}; rerun with sudo or set INSTALL_DIR to a writable directory"

  if [ -n "${VERSION:-}" ]; then
    tag="v${VERSION#v}"
  else
    echo "Detecting latest version..."
    tag=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
      | grep '"tag_name"' | cut -d'"' -f4) || true
    [ -n "$tag" ] || die "could not determine latest version"
  fi

  # LB_INSTALL_BASE_URL is for tests only: it points the downloads at a
  # local server instead of GitHub.
  base="${LB_INSTALL_BASE_URL:-https://github.com/${REPO}/releases/download}/${tag}"

  trap cleanup EXIT
  LB_TMP=$(mktemp -d "${TMPDIR:-/tmp}/lakebench-install.XXXXXX")

  echo "Downloading lakebench ${tag} for ${os}/${arch}..."
  if ! curl -fsSL "${base}/${binary}" -o "${LB_TMP}/${binary}"; then
    echo "Error: download failed. Check that ${binary} exists in release ${tag}" >&2
    echo "Available binaries: https://github.com/${REPO}/releases/tag/${tag}" >&2
    exit 1
  fi
  if ! curl -fsSL "${base}/SHA256SUMS" -o "${LB_TMP}/SHA256SUMS"; then
    die "could not download SHA256SUMS for release ${tag}; releases before 1.7.0" \
      "publish none. Download ${binary} from https://github.com/${REPO}/releases/tag/${tag}" \
      "and check it yourself, or install from PyPI: pipx install lakebench-k8s"
  fi

  # A line is "<sha256>  <name>", or "<sha256> *<name>" in binary mode.
  expected=$(awk -v f="$binary" \
    '{ n = $2; sub(/^\*/, "", n); if (n == f) { print tolower($1); exit } }' \
    "${LB_TMP}/SHA256SUMS")
  case "$expected" in
    "" | *[!0-9a-f]*) die "SHA256SUMS for ${tag} has no valid entry for ${binary}" ;;
  esac
  [ "${#expected}" -eq 64 ] || die "SHA256SUMS for ${tag} has no valid entry for ${binary}"
  actual=$($hasher "${LB_TMP}/${binary}" | awk '{ print $1 }')
  if [ "$actual" != "$expected" ]; then
    die "checksum mismatch for ${binary} (expected ${expected}, got ${actual});" \
      "nothing was installed"
  fi

  # Stage inside INSTALL_DIR, then rename over the old binary, so the
  # replacement is atomic and an interrupted copy is removed on exit.
  LB_STAGED="${install_dir}/.lakebench.new.$$"
  cp "${LB_TMP}/${binary}" "$LB_STAGED"
  # 755 whatever the umask, so a binary installed under sudo runs for everyone.
  chmod 755 "$LB_STAGED"
  mv -f "$LB_STAGED" "${install_dir}/lakebench"
  LB_STAGED=""
  echo "Installed lakebench ${tag} to ${install_dir}/lakebench"

  # Verify the binary just installed, not whichever lakebench is first on PATH.
  "${install_dir}/lakebench" version
}

main
