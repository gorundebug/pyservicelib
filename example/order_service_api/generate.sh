#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SPEC="${SCRIPT_DIR}/openapi/orderserviceapi/processorder.yaml"
OUTPUT="${SCRIPT_DIR}/src/order_service_api/generated/models.py"

"${PYTHON:-python3}" -m datamodel_code_generator \
  --input "${SPEC}" \
  --input-file-type openapi \
  --output "${OUTPUT}" \
  --output-model-type pydantic_v2.BaseModel \
  --target-python-version 3.14 \
  --snake-case-field \
  --alias-generator to_camel \
  --use-standard-collections \
  --use-union-operator \
  --field-constraints \
  --allow-population-by-field-name \
  --disable-timestamp \
  --formatters builtin \
  --custom-file-header \
  '# Code generated from processorder.yaml. DO NOT EDIT.'
