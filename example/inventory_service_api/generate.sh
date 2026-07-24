#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
OUT="${SCRIPT_DIR}/src/inventory_service_api/generated"

"${PYTHON:-python3}" -m grpc_tools.protoc \
  -I "${SCRIPT_DIR}/proto" \
  --python_out="${OUT}" \
  --pyi_out="${OUT}" \
  --grpc_python_out="${OUT}" \
  "${SCRIPT_DIR}/proto/processorderitem.proto"

# grpcio-tools emits an absolute sibling import; packages require a relative one.
sed -i.bak \
  's/^import processorderitem_pb2 as processorderitem__pb2$/from . import processorderitem_pb2 as processorderitem__pb2/' \
  "${OUT}/processorderitem_pb2_grpc.py"
rm -f "${OUT}/processorderitem_pb2_grpc.py.bak"
