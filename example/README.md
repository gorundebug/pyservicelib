# Python servicelib example

This is the Python counterpart of the generated Go and C++ examples. It keeps
the same independent-module boundary:

- `model` owns messages shared by both services;
- `order_service_api` owns the OpenAPI source and generated HTTP models;
- `inventory_service_api` owns the proto source and generated protobuf/gRPC code;
- `orderservice` owns the HTTP entry point and order stream graph;
- `inventoryservice` owns the unary gRPC entry point and inventory stream graph.

The runtime path is:

`POST /v1/processorder -> Input -> Split -> FlatMap -> gRPC SinkWithResult`
`-> Map -> Merge -> HTTP result callback`.

The second split branch uses `Delay -> Map` to emit a soft-timeout result.

Commands:

```bash
./example/run.sh generate     # regenerate OpenAPI and protobuf/gRPC files
./example/run.sh test         # local unit/config tests
./example/run.sh typecheck    # strict mypy check for both services
./example/run.sh integration  # Docker build plus end-to-end request
./example/run.sh dashboards   # render Grafana dashboards for both services
./example/run.sh up           # run only the two application services
./example/run.sh observe      # run services, Prometheus and Grafana
./example/run.sh down
```

With `observe`, Prometheus is available at
[`http://localhost:9090`](http://localhost:9090) and Grafana at
[`http://localhost:3000`](http://localhost:3000), using `admin` / `admin`.
Prometheus scrapes each service's generated `/metrics` handler. Grafana
provisions one folder per service from dashboards rendered from the canonical
`servicelib/grafana` Jsonnet sources.

Development-only checker dependencies are listed in
`example/requirements-dev.txt`. The type checker runs in strict mode. The only
targeted relaxation permits the generated gRPC adapter to inherit from
`grpcio-tools`' untyped generated servicer base; it does not disable checking
of the adapter body.

Generated transport code lives only under:

- `order_service_api/src/order_service_api/generated`;
- `inventory_service_api/src/inventory_service_api/generated`.

Business handlers and operator functions live under each service's
`src/<service>/internal/functions`.

The `generate` command runs both source generators:

- OpenAPI → strict Pydantic v2 HTTP models through
  `datamodel-code-generator`;
- protobuf → Python, gRPC and `.pyi` type stubs through `grpcio-tools`.

## Generated and user-owned application code

Python uses `_generated.py` instead of Go's `.generated.go`, because a dot
would make the module name unusable in a normal Python import:

```text
internal/app/
  service_generated.py       generated graph, makers and lifecycle
  http_service_generated.py  generated HTTP adapter (order service)
  grpc_service_generated.py  generated gRPC adapter (inventory service)
  service.py                 user-owned Service subclass and dependencies
```

`GeneratedService` may be regenerated in full. The user-owned `Service`
inherits it and keeps custom maker/function registration and lifecycle hooks in
`custom_makers_init`, `custom_functions_init`, `on_start` and `on_stop`. A
generator must create `service.py` only when it is absent. The lifecycle hooks
intentionally do not use the names `start`, `stop` or `service_init`: those are
runtime protocol methods on Python `ServiceApp`.

Configuration has the same ownership boundary:

```text
internal/
  config_generated.py  generated typed topology facade
  config.py            user-owned Config and CustomConfig
```

The generic runtime retains graph lists for traversal, while generated
application code uses typed resources such as
`cfg.named.streams.process_order`. Resource IDs and wrapper types are emitted
by the generator; display-name string lookup does not leak into service code.
