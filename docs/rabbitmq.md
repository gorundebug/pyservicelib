# RabbitMQ AMQP 1.0 (Python)

The adapter uses **rabbitmq-amqp-python-client 1.0.1**, the official stable
RabbitMQ Python AMQP 1.0 SDK, with **python-qpid-proton 0.40.0** locked as its
native dependency. It does not use AMQP 0.9.1, aio-pika, or the prerelease 2.x API.

## Public contracts

Import `RabbitMQDataConnectorConfig` and `RabbitMQEndpointConfig` from
`pyservicelib_gorundebug.runtime.config`. Source and sink factories are in
`datasource.rabbitmq` and `datasink.rabbitmq`:

- `make_rabbitmq_endpoint_consumer(stream, handler)` preserves the existing
  typed source or plain sink contract, according to its package.
- `datasink.rabbitmq.make_rabbitmq_endpoint_consumer_with_result(stream, handler)`
  routes R through the ordinary result continuation, independently of E.

Source handlers implement concurrency, on_error, begin_request, consume_message,
get_message_id, and end_request as in the existing Python native adapter contract.
Concurrency is 1..min(64, consumerCredits). Register result callbacks before
`await sc.collect(value)`: the graph may return a result synchronously.
`ConsumerMessage.message` is an owned native Message snapshot. `accept()`,
`release()`, and `reject()` explicitly settle that delivery. `ResultContext.done()`
only ends the request's result wait; it does not settle or replenish credit.
Handles expire at request completion or connection generation replacement.
BeginRequest failure skips ConsumeMessage and EndRequest; ConsumeMessage errors
reach EndRequest. Transport errors never manufacture a value of E.

Sink handlers implement get_stream_id, begin_request, consume_message, and
end_request. Set `SinkMessage.message` to an official SDK Message with a bytes
body and `inferred=True`. `send(callback)` provides bounded asynchronous worker
admission. The callback receives Delivery and maps it to R. `await send_sync()`
explicitly waits on the same path; call `out(result)` to emit a mapped result.
Pre-admission validation/backpressure raises an exception to the handler and
EndRequest. Async delivery callbacks may outlive EndRequest and retain their
captured state until completion. No result or acknowledgment is fabricated.

Delivery.outcome distinguishes `accepted`, `unroutable`, `rejected`, `modified`,
`unknown`, and `not-sent` (expiry before native transmission). Delivery.native_state
retains the numeric AMQP disposition; reason retains native broker rejection
information where supplied. Live Proton Delivery objects never cross owners.
Cancellation after admission is conservatively unknown; it does not imply broker
rejection. The adapter never automatically replays an unknown publication.

## Ownership, bounds and shutdown

The stable SDK's asyncio facade delegates blocking work to threads. Its public
Connection does not expose dial/operation timeouts. Each endpoint therefore owns
one spawned process containing the official Connection, management, links,
reactor and live delivery handles. Public SDK operations are confined to that
owner. There are no private SDK mutations or upstream patches. The asynchronous
service side exchanges bounded commands/results and native encoded snapshots.
Native publishing is serialized within each endpoint's worker; this is not a
claim that SDK 1.x exposes a native nonblocking publisher API. Separate endpoints
provide independent owners. Spawn-based applications must use their normal
`if __name__ == '__main__'` entrypoint guard.

Consumer credit and publisher pending limits default to 256 and accept 1..4096.
Source count credit replenishes only on explicit settlement; admitted native
encoded bytes are additionally capped at 8MiB. A message is capped at 1MiB after
native decoding, so the broker's frame/message limits still matter to transient
native allocation. Sink publication/callback count and byte bounds remain held
through callback completion. Commands and results use bounded IPC queues. No
unbounded executor queue or task per unadmitted message is created.

Start respects its context and separate connection/topology deadlines. Stop
closes admission, lets admitted handlers/results/settlements/publications drain,
and terminates a blocked native owner at its deadline. Source prefetched but
unadmitted deliveries remain unsettled for broker redelivery. EndRequest waits
for admitted application work; a handler ignoring cancellation can outlive Stop,
but EndRequest does not race it. A finite supervisor cleanup reaps terminated
processes. Sink callbacks ignoring cancellation may likewise outlive Stop.

Recovery rotates configured servers, with up to 60 recovery attempts separated
by 250ms, under lifecycle cancellation. A new owner has a new delivery generation.
Native automatic recovery is disabled because its sleep/retry behavior is not
context-bound. Reconnect rebuilds resources only under the same explicit creation
flags and does not replay publications. An idle publisher detects a disconnected
transport on its next operation; unknown publications remain unknown.

## Configuration and provisioning

The shared fields match Go: servers, virtualHost, connectTimeoutMs (default2000),
operationTimeoutMs (default5000), username/password, and TLS fields. Authentication
is runtime configuration. URLs accept amqp/amqps without credentials, path, query,
fragment or invalid ports. Token and credentialsFile are unsupported. SDK 1.x has
no public TLS server-name override: a supplied tlsServerName must match the URL
hostname. Its virtual-host URI handling supports URI-unreserved characters and
slash; unsupported names fail explicitly rather than route to an encoded name.
TLS verification remains enabled; CA and client keypair paths are supported.

Source requires queueName; sink requires queueName or exchangeName. Queue types
are quorum/classic, exchanges direct/topic/fanout. createQueue, createExchange,
and createBinding are independent opt-ins, all false by default. An existing
queue is inspected and any specified queueType must match. Missing queues are
created only with permission, defaulting to durable quorum. ExchangeType requires
createExchange because the native management API cannot inspect an existing
exchange's type read-only. Existing exchanges are verified through sender attach.
Queue/exchange/binding resources are never removed by adapter shutdown.

AMQP application properties propagate case-insensitive x-stream-id, x-trace,
traceparent, tracestate and baggage. Unrelated properties and native message
properties/annotations remain intact. A publication snapshot does not mutate
caller headers or alias later caller changes.

## Native dependencies and verification

Proton is a C dependency. Install a C compiler, pkg-config and OpenSSL development
headers before building it. Without OpenSSL discovery, its official source build
can silently select an SSL stub; the native TLS acceptance test catches that.
On macOS with Homebrew OpenSSL, build with:

```sh
PKG_CONFIG_PATH=/opt/homebrew/opt/openssl@3/lib/pkgconfig \
python -m pip install --force-reinstall --no-deps --no-cache-dir \
  --no-binary=python-qpid-proton python-qpid-proton==0.40.0
```

Run from pyservicelib with the existing dedicated RabbitMQ 4.x test broker:

```sh
RABBITMQ_TEST_URL=amqp://127.0.0.1:15672 \
.venv/bin/python -m pytest tests/test_rabbitmq_contract.py tests/test_rabbitmq_broker.py
```

The fixture uses adapter/adapter-test credentials. It creates unique test queues
and exchanges and removes those resources. It never restarts or pauses the broker.
Fault tests only terminate/suspend adapter-owned processes. TLS tests use a local
proxy and temporary certificate to the same broker. Native tests skip when no
broker URL is provided. The TLS test requires the openssl executable.

References: [official SDK](https://github.com/rabbitmq/rabbitmq-amqp-python-client),
[stable release](https://pypi.org/project/rabbitmq-amqp-python-client/1.0.1/),
[Proton native build requirements](https://github.com/apache/qpid-proton/blob/main/INSTALL.md).
