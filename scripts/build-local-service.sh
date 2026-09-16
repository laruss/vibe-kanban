#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${VIBE_KANBAN_PACKAGE_DIR:-${REPO_ROOT}/dist/local-service}"
TARGET_DIR="${CARGO_TARGET_DIR:-${REPO_ROOT}/target}"
VERSION="$(node -p "require('${REPO_ROOT}/package.json').version")"
ARCH="$(uname -m)"

case "${ARCH}" in
  arm64|aarch64) ARCH="arm64" ;;
  x86_64) ARCH="x64" ;;
  *)
    echo "Unsupported macOS architecture: ${ARCH}" >&2
    exit 1
    ;;
esac

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This package targets macOS; run it on the target Mac." >&2
  exit 1
fi

export VK_SHARED_API_BASE="${VK_SHARED_API_BASE:-https://api.vibekanban.com}"
export VITE_VK_SHARED_API_BASE="${VITE_VK_SHARED_API_BASE:-${VK_SHARED_API_BASE}}"

cd "${REPO_ROOT}"
pnpm --filter @vibe/local-web run build
cargo build --release --bin server

STAGING_DIR="$(mktemp -d "${TMPDIR:-/tmp}/vibe-kanban-package.XXXXXX")"
trap 'rm -rf "${STAGING_DIR}"' EXIT
PACKAGE_NAME="vibe-kanban-${VERSION}-macos-${ARCH}"
PACKAGE_ROOT="${STAGING_DIR}/${PACKAGE_NAME}"
mkdir -p "${PACKAGE_ROOT}"
install -m 0755 "${TARGET_DIR}/release/server" "${PACKAGE_ROOT}/vibe-kanban"
install -m 0644 \
  "${REPO_ROOT}/scripts/macos/ai.bloop.vibe-kanban.local.plist.template" \
  "${PACKAGE_ROOT}/ai.bloop.vibe-kanban.local.plist.template"

mkdir -p "${OUTPUT_DIR}"
tar -czf "${OUTPUT_DIR}/${PACKAGE_NAME}.tar.gz" -C "${STAGING_DIR}" "${PACKAGE_NAME}"
(
  cd "${OUTPUT_DIR}"
  shasum -a 256 "${PACKAGE_NAME}.tar.gz" > "${PACKAGE_NAME}.tar.gz.sha256"
)

echo "Created ${OUTPUT_DIR}/${PACKAGE_NAME}.tar.gz"
echo "Created ${OUTPUT_DIR}/${PACKAGE_NAME}.tar.gz.sha256"
