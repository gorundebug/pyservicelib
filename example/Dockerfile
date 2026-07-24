FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /workspace

COPY pyproject.toml requirements.txt README.md ./
COPY src ./src
COPY example ./example

RUN python -m pip install --no-cache-dir -e . \
    -e ./example/model \
    -e ./example/order_service_api \
    -e ./example/inventory_service_api \
    -e ./example/orderservice \
    -e ./example/inventoryservice
