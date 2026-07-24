#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"

docker compose -f "${COMPOSE_FILE}" up -d --build --force-recreate
cleanup() {
  docker compose -f "${COMPOSE_FILE}" down
}
trap cleanup EXIT

docker compose -f "${COMPOSE_FILE}" exec -T orderservice python - <<'PY'
import json
import time
import urllib.error
import urllib.request

payload = {
    "customerId": "customer-1",
    "items": [
        {"itemId": "item-1", "sku": "BOOK", "quantity": 2},
        {"itemId": "item-2", "sku": "PHONE", "quantity": 2},
    ],
}

for attempt in range(60):
    try:
        request = urllib.request.Request(
            "http://127.0.0.1:9091/v1/processorder",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            result = json.load(response)
        break
    except (OSError, urllib.error.URLError):
        if attempt == 59:
            raise
        time.sleep(0.25)

assert result["status"] == "PARTIALLY_CONFIRMED", result
assert len(result["confirmedItems"]) == 2, result
assert result["confirmedItems"][0]["reserved"] is True, result
assert result["confirmedItems"][1]["reserved"] is False, result
assert result["totalAmount"] == 1623.0, result
print(json.dumps(result, indent=2))
PY
