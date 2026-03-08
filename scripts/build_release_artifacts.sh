#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <version> [output-dir]" >&2
  exit 1
fi

VERSION="$1"
OUTDIR="${2:-dist}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_COMMIT="$(git -C "${ROOT_DIR}" rev-parse --short HEAD 2>/dev/null || echo unknown)"
BUILD_DATE="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

TARGETS=(
  "darwin amd64"
  "darwin arm64"
  "linux amd64"
  "linux arm64"
)

if command -v sha256sum >/dev/null 2>&1; then
  CHECKSUM_CMD=(sha256sum)
else
  CHECKSUM_CMD=(shasum -a 256)
fi

mkdir -p "${OUTDIR}"
rm -f "${OUTDIR}/SHA256SUMS.txt"

for target in "${TARGETS[@]}"; do
  read -r GOOS GOARCH <<<"${target}"
  STAGE_DIR="${OUTDIR}/memori_${VERSION}_${GOOS}_${GOARCH}"
  BIN_DIR="${STAGE_DIR}/bin"
  ARCHIVE_PATH="${OUTDIR}/memori_${VERSION}_${GOOS}_${GOARCH}.tar.gz"

  rm -rf "${STAGE_DIR}"
  mkdir -p "${BIN_DIR}"

  (
    cd "${ROOT_DIR}"
    CGO_ENABLED=0 GOOS="${GOOS}" GOARCH="${GOARCH}" go build \
      -trimpath \
      -ldflags "-X github.com/willbastian/memori/internal/cli.buildVersion=${VERSION} -X github.com/willbastian/memori/internal/cli.buildCommit=${BUILD_COMMIT} -X github.com/willbastian/memori/internal/cli.buildDate=${BUILD_DATE}" \
      -o "${BIN_DIR}/memori" \
      ./cmd/memori
  )

  cp "${ROOT_DIR}/README.md" "${STAGE_DIR}/README.md"
  if [[ -d "${ROOT_DIR}/assets" ]]; then
    cp -R "${ROOT_DIR}/assets" "${STAGE_DIR}/assets"
  fi

  tar -C "${OUTDIR}" -czf "${ARCHIVE_PATH}" "$(basename "${STAGE_DIR}")"
  (
    cd "${OUTDIR}"
    "${CHECKSUM_CMD[@]}" "$(basename "${ARCHIVE_PATH}")"
  ) >> "${OUTDIR}/SHA256SUMS.txt"
  rm -rf "${STAGE_DIR}"
done
