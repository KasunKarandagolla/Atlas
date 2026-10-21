#!/usr/bin/env bash
# Invoke inside atlas_red_bounded_run.sh with explicit permanent/temp paths.
set -euo pipefail
: "${ATLAS_PERMANENT_ROOT:?}"
: "${ATLAS_TEMP_ROOT:?}"
ATLAS_SOURCE_ROOT=$(cd "$(dirname "$0")/.." && pwd)
python3.12 -c 'import os, shutil; assert shutil.disk_usage(os.environ["ATLAS_PERMANENT_ROOT"]).free > 8000000000, "Insufficient physical headroom for isolated runtime, staging and OS reserve"'
ATLAS_MODEL_ENV="$ATLAS_PERMANENT_ROOT/env/foundation"
if [ ! -x "$ATLAS_MODEL_ENV/bin/python" ]; then
  python3.12 -m venv "$ATLAS_MODEL_ENV"
fi
# Exact CPU-only torch and transitive package hashes; primary ATLAS env untouched.
"$ATLAS_MODEL_ENV/bin/python" -m pip install --require-hashes --no-cache-dir \
  --extra-index-url https://download.pytorch.org/whl/cpu \
  -r "$ATLAS_SOURCE_ROOT/configs/research/foundation-linux-py312.lock"
"$ATLAS_MODEL_ENV/bin/python" -m pip check
ATLAS_KRONOS_SOURCE="$ATLAS_PERMANENT_ROOT/kronos-source"
if [ ! -e "$ATLAS_KRONOS_SOURCE" ]; then
  git clone --no-checkout https://github.com/shiyu-coder/Kronos.git "$ATLAS_KRONOS_SOURCE"
  git -C "$ATLAS_KRONOS_SOURCE" checkout --detach 67b630e67f6a18c9e9be918d9b4337c960db1e9a
fi
if [ "$(git -C "$ATLAS_KRONOS_SOURCE" rev-parse HEAD)" != 67b630e67f6a18c9e9be918d9b4337c960db1e9a ] || \
   [ -n "$(git -C "$ATLAS_KRONOS_SOURCE" status --porcelain)" ]; then
  echo 'Existing Kronos source is changed or has the wrong revision; preserved and refused.' >&2
  exit 2
fi
exec "$ATLAS_MODEL_ENV/bin/python" "$ATLAS_SOURCE_ROOT/scripts/atlas_red_provision_models.py" \
  --permanent-root "$ATLAS_PERMANENT_ROOT" --temporary-root "$ATLAS_TEMP_ROOT" \
  --kronos-source "$ATLAS_KRONOS_SOURCE" "$@"
