#!/usr/bin/env bash
# Linux unprivileged private mount namespace. No owner mount is altered.
set -euo pipefail
: "${ATLAS_TEMP_ROOT:?Set a new dedicated temporary directory}"
: "${ATLAS_PERMANENT_ROOT:?Set a separate permanent evidence/model directory}"
ATLAS_TEMP_CAPACITY_BYTES=${ATLAS_TEMP_CAPACITY_BYTES:-3000000000}
ATLAS_EXTERNAL_TEMPORARY_BYTES=${ATLAS_EXTERNAL_TEMPORARY_BYTES:-0}
export ATLAS_TEMP_CAPACITY_BYTES ATLAS_EXTERNAL_TEMPORARY_BYTES
if [ "$((ATLAS_TEMP_CAPACITY_BYTES + ATLAS_EXTERNAL_TEMPORARY_BYTES + 4096))" -ge 5000000000 ]; then
  echo 'Temporary capacity plus external temporary reservation must stay below 5 GB' >&2
  exit 2
fi
mkdir -p "$ATLAS_TEMP_ROOT" "$ATLAS_PERMANENT_ROOT"
# Refuse mounting over existing data; never hide or delete owner files.
if [ -n "$(ls -A "$ATLAS_TEMP_ROOT")" ]; then
  echo 'Temporary mountpoint must be empty' >&2
  exit 2
fi
exec unshare -Ur -m bash -c '
set -euo pipefail
mount -t tmpfs -o "size=$ATLAS_TEMP_CAPACITY_BYTES,nr_inodes=200000" atlas-temp "$ATLAS_TEMP_ROOT"
export TMPDIR="$ATLAS_TEMP_ROOT/tmp" TMP="$ATLAS_TEMP_ROOT/tmp" TEMP="$ATLAS_TEMP_ROOT/tmp"
export PIP_CACHE_DIR="$ATLAS_TEMP_ROOT/pip" UV_CACHE_DIR="$ATLAS_TEMP_ROOT/uv" XDG_CACHE_HOME="$ATLAS_TEMP_ROOT/cache"
export PYTHONDONTWRITEBYTECODE=1
mkdir -p "$TMPDIR" "$PIP_CACHE_DIR" "$UV_CACHE_DIR" "$XDG_CACHE_HOME"
exec "$@"
' atlas-bounded "$@"
