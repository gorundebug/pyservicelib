#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
schema=${SERVICE_API_SPEC:-"$root/../servicelib/api/serviceapi.yaml"}
models="$root/src/pyservicelib_gorundebug/api/models"
mode=${1:-generate}
case "$mode" in
  generate|--check) ;;
  *) printf 'Usage: %s [--check]\n' "$0" >&2; exit 2 ;;
esac
work=$(mktemp -d "${TMPDIR:-/tmp}/pyservicelib-api.XXXXXX")
trap 'rm -rf "$work"' EXIT

docker run --rm --user "$(id -u):$(id -g)" \
  -v "$schema:/local/serviceapi.yaml:ro" -v "$work:/local/output" \
  "${OPENAPI_GENERATOR_IMAGE:-openapitools/openapi-generator-cli:v7.24.0}" generate \
  -g python --skip-validate-spec -i /local/serviceapi.yaml \
  --additional-properties packageName=. --global-property models -o /local/output

test -f "$work/models/type.py"
touch "$work/models/__init__.py"
if [[ "$mode" == --check ]]; then
  diff -ru --exclude=__pycache__ "$models" "$work/models"
else
  # Do not discard the current models unless generation has succeeded.
  rm -rf "$models"
  mkdir -p "$models"
  cp -R "$work/models/." "$models/"
  touch "$root/src/pyservicelib_gorundebug/api/__init__.py"
fi
