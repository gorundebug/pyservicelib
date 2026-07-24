#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
PYTHON="${PROJECT_DIR}/.venv/bin/python"

export PYTHONPATH="${PROJECT_DIR}/src:${SCRIPT_DIR}/model/src:${SCRIPT_DIR}/order_service_api/src:${SCRIPT_DIR}/inventory_service_api/src:${SCRIPT_DIR}/orderservice/src:${SCRIPT_DIR}/inventoryservice/src"

case "${1:-test}" in
  generate)
    # Regenerate HTTP models and protobuf/gRPC transport code from source specs.
    PYTHON="${PYTHON}" bash "${SCRIPT_DIR}/order_service_api/generate.sh"
    PYTHON="${PYTHON}" bash "${SCRIPT_DIR}/inventory_service_api/generate.sh"
    ;;
  test)
    # Run shared checks and user-owned per-function tests for both services.
    # Separate processes preserve the independent `tests` package namespace
    # that each generated service has in its own repository.
    "${PYTHON}" -m pytest "${SCRIPT_DIR}/tests" -q
    "${PYTHON}" -m pytest "${SCRIPT_DIR}/orderservice/tests" -q
    "${PYTHON}" -m pytest "${SCRIPT_DIR}/inventoryservice/tests" -q
    ;;
  typecheck)
    # Strictly type-check both services and their shared handwritten models.
    "${PYTHON}" -m mypy \
      --config-file "${SCRIPT_DIR}/mypy.ini" \
      "${SCRIPT_DIR}/model/src" \
      "${SCRIPT_DIR}/order_service_api/src" \
      "${SCRIPT_DIR}/orderservice/src" \
      "${SCRIPT_DIR}/inventoryservice/src"
    ;;
  integration)
    # Build both independent services and run the complete HTTP -> gRPC scenario.
    "${SCRIPT_DIR}/integration_test.sh"
    ;;
  dashboards)
    # Render the shared servicelib dashboards for each concrete service name.
    bash "${SCRIPT_DIR}/orderservice/grafana/generate.sh"
    bash "${SCRIPT_DIR}/inventoryservice/grafana/generate.sh"
    ;;
  observe)
    # Generate dashboards and run the services with Prometheus and Grafana.
    "${BASH_SOURCE[0]}" dashboards
    docker compose \
      -f "${SCRIPT_DIR}/docker-compose.yml" \
      --profile observability \
      up --build
    ;;
  up)
    # Start the two-service example and leave it running.
    docker compose -f "${SCRIPT_DIR}/docker-compose.yml" up --build
    ;;
  down)
    # Stop both the base example and its optional observability profile.
    docker compose \
      -f "${SCRIPT_DIR}/docker-compose.yml" \
      --profile observability \
      down
    ;;
  *)
    echo "usage: $0 {generate|test|typecheck|integration|dashboards|observe|up|down}" >&2
    exit 2
    ;;
esac
