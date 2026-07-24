#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd -- "${SCRIPT_DIR}/../../../.." && pwd)"
DASHBOARDS_DIR="${SERVICELIB_GRAFANA_DASHBOARDS:-${WORKSPACE_DIR}/servicelib/grafana/dashboards}"
SERVICE_NAME="$(basename "$(dirname "${SCRIPT_DIR}")")"
IMAGE="servicelib-dashboards-${SERVICE_NAME}"

docker build \
  --build-context "dashboard_sources=${DASHBOARDS_DIR}" \
  --build-arg "SERVICE_NAME=${SERVICE_NAME}" \
  -f "${SCRIPT_DIR}/Dockerfile" \
  -t "${IMAGE}" \
  "${SCRIPT_DIR}"

mkdir -p "${SCRIPT_DIR}/dist"
docker run --rm -v "${SCRIPT_DIR}/dist:/output" "${IMAGE}"
