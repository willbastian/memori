#!/usr/bin/env bash

set -euo pipefail

REPO="willbastian/memori"
VERSION=""
INSTALL_DIR="${MEMORI_INSTALL_DIR:-$HOME/.local/bin}"
BASE_URL="${MEMORI_INSTALL_BASE_URL:-}"

usage() {
  cat >&2 <<'EOF'
usage: install_release.sh [--version <tag>] [--dir <install-dir>] [--base-url <url-or-path>]

Installs the memori release binary for the current OS and architecture.

Defaults:
  --version   latest GitHub release
  --dir       $HOME/.local/bin
  --base-url  https://github.com/willbastian/memori/releases/download/<tag>
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --dir)
      INSTALL_DIR="${2:-}"
      shift 2
      ;;
    --base-url)
      BASE_URL="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need_cmd tar

if [[ -z "${VERSION}" ]]; then
  need_cmd curl
  VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"
  if [[ -z "${VERSION}" ]]; then
    echo "failed to resolve latest release tag" >&2
    exit 1
  fi
fi

uname_s="$(uname -s)"
uname_m="$(uname -m)"

case "${uname_s}" in
  Darwin) GOOS="darwin" ;;
  Linux) GOOS="linux" ;;
  *)
    echo "unsupported OS: ${uname_s}" >&2
    exit 1
    ;;
esac

case "${uname_m}" in
  x86_64|amd64) GOARCH="amd64" ;;
  arm64|aarch64) GOARCH="arm64" ;;
  *)
    echo "unsupported architecture: ${uname_m}" >&2
    exit 1
    ;;
esac

ASSET="memori_${VERSION}_${GOOS}_${GOARCH}.tar.gz"
if [[ -z "${BASE_URL}" ]]; then
  BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"
fi

TMPDIR_ROOT="$(mktemp -d)"
trap 'rm -rf "${TMPDIR_ROOT}"' EXIT
ARCHIVE_PATH="${TMPDIR_ROOT}/${ASSET}"

download_asset() {
  local source="$1"
  local target="$2"

  if [[ -d "${source}" ]]; then
    cp "${source}/${ASSET}" "${target}"
    return
  fi
  if [[ "${source}" == file://* ]]; then
    cp "${source#file://}/${ASSET}" "${target}"
    return
  fi

  need_cmd curl
  curl -fsSL "${source}/${ASSET}" -o "${target}"
}

download_asset "${BASE_URL}" "${ARCHIVE_PATH}"

tar -C "${TMPDIR_ROOT}" -xzf "${ARCHIVE_PATH}"
STAGE_DIR="${TMPDIR_ROOT}/memori_${VERSION}_${GOOS}_${GOARCH}"
BIN_PATH="${STAGE_DIR}/bin/memori"

if [[ ! -f "${BIN_PATH}" ]]; then
  echo "downloaded archive did not contain ${BIN_PATH}" >&2
  exit 1
fi

mkdir -p "${INSTALL_DIR}"
install -m 0755 "${BIN_PATH}" "${INSTALL_DIR}/memori"

echo "Installed memori ${VERSION} to ${INSTALL_DIR}/memori"
if [[ ":$PATH:" != *":${INSTALL_DIR}:"* ]]; then
  echo "Add ${INSTALL_DIR} to your PATH to run memori directly."
fi
