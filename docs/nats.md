# Python native NATS and JetStream profile

This profile uses the official [nats.py](https://github.com/nats-io/nats.py/tree/v2.12.0)
client, pinned to `nats-py[nkeys]==2.12.0` in requirements.txt and uv.lock.
It implements the repository's `servicegen/docs/message-brokers-contract.md`
through the existing public DataSource/DataSink lifecycle. It does not use a
Custom source, hidden application consumer, community adapter or upstream patch.
Generator/template support is a separate work item.

## Public entry points

- `datasource.nats.make_nats_endpoint_consumer(stream, handler)` registers a
  native source and endpoint in the service environment.
- `datasink.nats.make_nats_endpoint_consumer(stream, handler)` maps delivery
  values to the plain sink's typed error route, matching the existing Kafka API.
- `datasink.nats.make_nats_endpoint_consumer_with_result(stream, handler)` maps
  delivery values to the explicit typed result continuation.
- `runtime.config` exports NatsDataConnectorConfig, JetStreamDataConnectorConfig,
  NatsEndpointConfig and JetStreamEndpointConfig. The ordinary runtime config
  loader also accepts the regenerated API fields.

The connector kind selects Core (NATS=7) or JetStream=8. The implementation
identifier is `nats.py`; serialized architecture bindings use
`implementations.python`. Existing known implementation strings still decode
as their typed enum; the runtime configuration accepts open implementation
strings for pack-selected clients. Public DataSource/DataSink interfaces and
existing Kafka/Temporal behavior have not been changed.

## Configuration and resources

Explicit `servers` are required. Supported URLs are `nats://` and `tls://`, with
no embedded credentials, path, query or fragment, and explicit ports in 1..65535.
Connect and operation timeouts are integer milliseconds in 1..9223372036854,
defaulting to 2000 and 5000; conversion to seconds happens at the SDK boundary.
No implicit localhost. Authentication is one of username/password, token, or
credentialsFile (mounted JWT/NKey credentials). Malformed/oversized credential
files are rejected before the SDK's synchronous marker parser. Credentials
are runtime inputs, never logged by this adapter.

TLS requires tlsEnabled=true, including tls:// URLs. CA, server name and paired
client certificate/key fields configure a verifying SSLContext. This profile
uses the official `tls_handshake_first=True` option to prevent plaintext
downgrade; the server must enable/support TLS-first. An ordinary INFO-first TLS
server is not supported by this strict profile. No insecure verification bypass.

Core endpoints use `subject` and optional source-only `queueGroup`. Core sinks
require a concrete subject. JetStream endpoints require `streamName`; sources
also require `durableName` and use durable pull consumers. Queue groups and
consumer declarations on sinks are rejected. Subject coverage and consumer
compatibility are checked before admission and after reconnect.

`createStream=false` and `createConsumer=false` are defaults. Existing-only
mode never creates a resource. Explicit creation creates a missing FILE/1 stream
and an EXPLICIT/ALL/INSTANT durable pull consumer. Existing resources are never
updated or deleted. Existing consumers must have the requested single filter,
explicit ACK, pull mode, durable identity, ALL delivery and INSTANT replay;
ack wait/redelivery limits otherwise remain the existing broker configuration.

nats.py 2.12's add_consumer is create-or-update. To avoid its lookup/create race,
this adapter uses **the official client's public request method** for the native
JetStream CONSUMER.CREATE management request with `action: create`. It uses public
ConsumerConfig/ConsumerInfo/APIError types, no private SDK methods. Creation
requires server >=2.11 conservatively; existing-only binding does not impose that
creation gate. Concurrent incompatible creation fails instead of reconciling.
Only the default JetStream API prefix is supported.

## Source contract

`begin_request -> consume_message -> end_request` uses inherited request
cancellation/deadline and a per-request stream ID. A begin failure calls on_error
and metrics, without consume/end. A successful begin leads to one end invocation.
Consume failures and async `ResultContext.cancel(error)` preserve the error for
end_request. Its StreamContext exposes typed collect/error_collect operations.
Transport failures without message state call the handler's on_error.

Result-bearing requests remain pending until `result.done()` or cancellation.
Returning from consume, collecting input and removing a result callback do not
complete the request. Done is idempotent and **never ACK**. Callbacks may be
synchronous or async. Completion is synchronized with an active callback before
finalization; observed Done wins over cancellation, not over a consume error.
Callbacks have a finite operation-timeout budget. Late results cannot invoke a
finalized request's callback. Duplicate active correlation IDs are rejected.

ConsumerMessage preserves bytes, subject, reply and the native client's header
representation. **Python header values are scalar strings** (`dict[str, str]`):
with pinned nats-py 2.16.0, repeated wire values of the same name collapse to the
last value before the Service Architect handler receives the message. The public
publisher also accepts one string per name. Do not use repeated header values for
a cross-language contract involving Python; encode application multiplicity in
the payload or distinct names. The SDK does not reconstruct discarded values or
patch the native parser. `test_native_repeated_header_representation_limit`
checks this actual wire boundary for Core and JetStream, alongside binary bytes
and correlation. Other runtimes can expose a richer native header collection.
It exposes
**no ACK method** for Core. JetStreamMessage adds native metadata and explicit
ack, ack_sync, nak, in_progress and term. ack only requests native settlement;
ack_sync waits for a server response. No automatic ACK, retry/requeue policy,
Kafka checkpoint abstraction, transactions or exactly-once claim. Handles expire
when their request finalizes or the connection epoch changes. Do not retain a
message to settle it after EndRequest. Server-side duplicate/redelivery races
remain native JetStream semantics; local fencing is not a distributed lease.

Official reconnect is enabled (60 attempts, SDK default retry interval). On
disconnect, active source request owners are cancelled, old queued delivery epochs
are rejected, and the receive task stops. On reconnect, the adapter validates
existing resources and rebinds the subscription without provisioning; pending
buffers from the old subscription are not reused. Core messages during an outage
cannot be recovered; JetStream recovery follows the durable consumer's ACK wait
and delivery policy.

## Sink contract

Set `message.data` (bytes) and optional string headers. The modeled subject is
fixed. `message.send(map_delivery)` performs bounded, immediate queue admission;
it neither waits for publication nor synthesizes a successful delivery result.
EndRequest follows ConsumeMessage return. A later native outcome is mapped into
the typed route with the original caller ContextVars. Handler-owned state used
by later callbacks must remain valid independently of EndRequest.

`Delivery(publication, error)` distinguishes failure from publication:

- Core Send: `Publication('accepted')` means the native client accepted publish.
  It does not confirm network flush, subscribers or durability.
- Core SendSync: `Publication('flushed')` follows the native flush.
- JetStream Send/SendSync: `Publication('jetstream', PubAck)` follows the official
  publish_async future. Both use this same native async path; Sync awaits it.

SendSync returns Delivery and does not automatically emit it; call out explicitly.
Skip emits a local result without publishing. A message may send/skip once.
Before admission, cancellation/full queues/disconnection reject explicitly.
Cancellation after transmission cannot withdraw a message. Caller cancellation
can stop a synchronous wait while the delivery remains owned; later mapped async
results are retained. A publication timeout may mean unknown delivery. There is
no adapter-level republish. Native pending bytes are checked before transmission.

## Bounds, context and shutdown

Each source endpoint has 64 queued deliveries plus one receive in progress and
1..64 active handlers (zero/unlimited is rejected). A source request permits 64
result callbacks. Native subscriptions have 64-message/8-MiB pending limits.
JetStream pulls one message at a time. Core native overflow is an observable
SlowConsumer error and closes admission; it is not an acknowledged/drop-success
mode. The adapter cannot recover ephemeral messages lost before admission.

Each sink has 64 queued publications and four delivery workers. Native outgoing
pending_size is 8 MiB and the flusher queue has 64 entries. JetStream async
max-pending is 64; the native reply subscription uses nats.py's own finite default
buffers (not the source's 8-MiB limit). Payloads are at most 1 MiB and application
headers at most 64 KiB. Servers advertising max_payload above 8 MiB fail startup
or reconnect validation. These are logical bounds, not measured peak RSS; Python
object overhead, native parser/socket buffers and broker storage are additional.

Source Stop first stops receive admission/unsubscribes, then drains already
queued/active requests with their context and connection still usable for ACK.
Only deadline expiry cancels incomplete requests; it never acknowledges them.
Sink Stop first rejects new sends, then drains admitted native outcomes and
result callbacks before flush/close. Deadline expiry reports unfinished work,
cancels active operations and resolves unsent sync waiters with failure. Queued
async work that cannot execute its result route before the deadline is reported
through the logger; it is never reported as successful publication.

Finalizers/callbacks and native close are bounded. The void Stop API reports
unfinished work through logs. Python cancellation is cooperative: application
callbacks that suppress cancellation or block the event loop can defeat deadlines.
A native close timeout is logged and its task cancelled; complete OS-level socket
cleanup is not claimed in that exceptional case. No detached cleanup task is
intentionally left running.

Headers x-stream-id, x-trace, traceparent, tracestate and baggage are extracted
case-insensitively. The incoming stream ID is retained (or generated when absent).
W3C tracing/baggage use the configured OpenTelemetry propagator. Outgoing current
context replaces matching trace/correlation headers case-insensitively and
preserves caller business headers. Native protocol bytes are never JSON-wrapped;
only the native management API uses its specified JSON representation.

## Verification and limits

Acceptance runs use isolated official nats-server 2.15.0 processes and temporary
storage, nats.py 2.12.0, CPython 3.14.6, macOS arm64. Tests cover real Core and
JetStream source/sink traffic, byte/header/correlation preservation, Done without
ACK and redelivery, explicit confirmed ACK, typed success/failure results, source
begin/consume/async errors, resource flags and non-mutation, broker restart and
resumption, two Core queue consumers, bounded queues, overflow, cancellation,
and graceful/deadline drain. TLS-first with verified CA/server name and rejection
of a wrong hostname is exercised. Focused existing Kafka/config tests are also run.

Username/password and token broker authentication, JWT/NKey authentication, mTLS,
broker ACLs, clustered replication/failover,
sustained-load/RSS, every settlement operation and cross-language/generated
project interoperability are **not yet broker-verified**. No request/reply,
JetStream transactions, custom API domains or configurable resource reconciliation
are advertised. Native deprecation warnings on Python 3.14 remain unpatched.

Run from pyservicelib:

```sh
PYTHONPATH=src NATS_SERVER_BINARY=/path/to/nats-server .venv/bin/python -m pytest \
  tests/test_nats_contract.py tests/test_nats_broker.py \
  tests/test_config_parity.py tests/test_kafka_source_lifecycle.py \
  tests/test_kafka_sink_result.py tests/test_schedule_config_types.py
```

Final focused execution: **92 passed, zero skipped** (27 real-broker cases,
33 NATS configuration/contract cases, 32 existing Kafka/config/schedule cases).
Mypy passes for all six new source modules with --check-untyped-defs and
--follow-imports=silent; git diff --check passes. The parent's opt-in
TestPythonNATSGeneration gate also passes all four Core/JetStream/order cases
with Go 1.26.4, compiling Python and importing generated named config, Service
and endpoint modules. That gate checks configuration/imports, not a running
generated service against the broker. Canonical NatsPy='nats.py' bindings were
regenerated using build_api.sh after the shared schema owner added the enum.
The regeneration also carries current shared RabbitMQ schema declarations;
this change does not implement or advertise a RabbitMQ Python adapter.

Named configuration adapters must explicitly preserve the runtime-only
properties bag, which the generic base model excludes from dumps:

```python
NatsDataConnectorConfig.model_validate({
    **config.model_dump(exclude_none=True),
    "properties": config.properties,
})
```

Use the same pattern for JetStream and endpoint named adapters. Auth/TLS fields
are regular typed fields and survive this conversion. Default generated source
concurrency must be positive (1 is the conservative default), and JetStream
handler annotations should use JetStreamMessage to expose settlement methods.

Without a local server binary, broker tests explicitly skip and must not be
counted as acceptance evidence.
