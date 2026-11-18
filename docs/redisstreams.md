# Redis Streams native Python SDK

The source and sink use official **redis-py 8.1.0** (`redis.asyncio`), pinned in
requirements.txt and uv.lock. Supported transport is one standalone primary,
Redis 7.2 or newer. Startup validates INFO server and ROLE. Cluster, Sentinel,
replica, multiple-server and unsupported broker options fail explicitly. There
are no upstream patches, custom transport implementations or generic interface
extensions.

## Integration and native records

Import from `pyservicelib_gorundebug.datasource.redisstreams`:

- `make_redisstreams_endpoint_consumer[S,T,R,E](stream, handler)`
- `ConsumerMessage`, `ResultContext`, `EndpointHandler`, `Entry`, and
  `DeletedPendingEntriesError`.

Import from `pyservicelib_gorundebug.datasink.redisstreams`:

- `make_redisstreams_endpoint_consumer[S,T,E](stream, handler)`
- `make_redisstreams_endpoint_consumer_with_result[S,T,R,E](stream, handler)`
- `SinkMessage`, `EndpointHandler`, and `Delivery`.

Typed configuration constructors `RedisStreamsDataConnectorConfig` and
`RedisStreamsEndpointConfig` are exported by runtime.config. Connector type is
RedisStreams (10), implementation `redis-py`, protocol identity `redisstreams`.
The existing DataSource/DataSink Start/Stop interfaces and typed graph routes
are unchanged. Handler signatures follow the Python native broker adapters.
Source handlers provide concurrency(sc), an integer 1..min(64, readCount).

ConsumerMessage.message is an immutable Entry containing native string `id`,
ordered `fields: tuple[tuple[bytes,bytes], ...]`, `reclaimed`, and retained `size`.
ConsumerMessage.data aliases those fields and headers exposes transport metadata.
SinkMessage.fields accepts a list/tuple of string or bytes-like field/value pairs.
Publication snapshots copy mutable buffers. Duplicate names, ordering and binary
values survive transport; no dictionary conversion or implicit JSON codec occurs.

The adapter uses redis-py's public ConnectionPool, send_command and read_response
APIs to avoid lossy convenience response callbacks. It does not access private
client state or override parsers. Native pools are bounded; application code does
not need to manage Redis connections.

## Completion, settlement and outcomes

Source `await msg.ack()` explicitly invokes XACK and returns its native affected
PEL count. It can be used once per local handle. `result.done()`, handler return
and EndRequest never ACK. XACK does not delete the stream entry. Handles expire
when their request or connection epoch ends. This is local lifecycle fencing,
**not Redis owner fencing**: XACK can acknowledge an ID reclaimed by another
consumer. Reclaim cannot undo another worker's business effects. Set claimIdleMs
above normal processing latency and make effects idempotent.

Register result callbacks before `await sc.collect(...)`, since a result can
arrive synchronously. Begin failure skips Consume/End; successful Begin leads to
one EndRequest after actual handler and result work. A result-bearing source
waits for Done or cancellation. `result.cancel(error)` preserves an asynchronous
failure for EndRequest. Transport failures reach on_error without synthesizing E
or acknowledging input. Completed results win over cancellation after callback
synchronization. Correlation retains inherited stream ID; internal request
identity separately prevents concurrent same-ID requests from stealing callbacks.

Sink `msg.send(callback)` performs bounded local admission and returns immediately;
the official asynchronous XADD is awaited by owned adapter work. The callback may
be async and its return value follows the existing typed result route.
`await msg.send_sync()` awaits the same native outcome without implicit result
emission; `await msg.out(value)` and `await msg.skip(value)` are explicit local
result operations. Local invalid/capacity/cancelled admission raises
AdmissionRejected before sending and is passed to EndRequest.

| Delivery.outcome | Meaning |
| --- | --- |
| published | Redis returned the XADD entry ID, exposed as Delivery.id |
| rejected | Native error reply or NOMKSTREAM nil; append was not confirmed successful |
| not-sent | Owner unavailable/cancelled before native submission |
| unknown | Timeout, cancellation or disconnect after possible transmission |

Delivery.error carries failure detail. Unknown XADD is never automatically
replayed. Command retry is disabled with the official async Retry(NoBackoff(), 0).
A returned ID proves primary acceptance, not disk/replica durability, consumption,
business completion or exactly-once effects. Canonical outcome mapping is
EntryID = delivery.id and Accepted = delivery.outcome == "published"; EventID and
Reason are application mappings.

EndRequest follows ConsumeMessage return, **not publication completion**. Async
callbacks can outlive it; captured handler state must remain valid. Capacity is
held until callback/result work ends. An unsent message expires when its handler
returns; it cannot borrow a later request's admission. Context variables propagate cancellation,
deadlines and correlation across async publication/result callbacks. Native fields
carry case-insensitive x-stream-id, x-trace, traceparent, tracestate and baggage.
Caller fields remain intact except matching transport fields replaced by current
context metadata in the snapshot.

## Configuration

One redis:// or rediss:// URL requires a hostname; an omitted port defaults to
6379. Explicit ports are 1..65535. URL credentials, paths (including /), database,
queries, fragments, whitespace and controls are rejected. redisDatabase defaults
0; runtime username/password supports ACL and password-only default-user auth.
Token and credentialsFile are unsupported. Put credentials in runtime bindings,
not topology URLs. Connect/operation timeouts default 2000/5000ms and require
integers 1..2147483647.

TLS verifies trust and hostname. Custom CA and paired client certificate/key
files are supported. **Public-client limitation:** redis-py cannot override TLS
server name separately from the connection hostname; a differing tlsServerName
is rejected. Use a matching URL hostname. No verification bypass is provided.

Both roles require exact literal streamKey. Source requires consumerGroup;
consumerName defaults to a unique endpoint-instance identity. createConsumerGroup
is false/absent by default. Opt-in XGROUP CREATE uses MKSTREAM and groupStartId
`0-0` (default) or `$` only. Supplying groupStartId requires creation opt-in.
BUSYGROUP preserves the existing position; other errors fail startup.

Source defaults: readCount 64 (1..4096), readBlockMs 1000 (1..60000), claimIdleMs
60000 and claimIntervalMs 1000 (1..2147483647). Source application concurrency is
also bounded by the handler's concurrency setting. Sink publisherMaxInFlight
is 256 by default (1..4096). Sink createStream defaults false, using XADD
NOMKSTREAM; true explicitly permits stream creation. Source-only fields on sinks
and sink-only fields on sources are rejected, including explicit false flags.
Runtime API/YAML projection preserves explicit false and absent options.

No implicit retention trim, TTL, deletion, DLQ, group reset or business retry is
introduced. ACLs need INFO/ROLE, TYPE for sink startup, XINFO GROUPS/PING for
source startup, XREADGROUP/XAUTOCLAIM/XACK for receive/settlement, XADD for
publication, and XGROUP CREATE only when opted in.

## Admission, recovery and shutdown

Each source owns a dedicated one-connection blocking-read pool and a separate
bounded control pool, so BLOCK cannot delay ACK or reclaim. New and reclaimed
entries use the same admission bound. XAUTOCLAIM retains its cursor even across
empty pages; deleted PEL IDs are exposed as DeletedPendingEntriesError.ids via
on_error. A lost XREADGROUP reply may already have created PEL work; reclaim
recovers it. Processing is concurrent; ordering is not promised.

Retained entry size is capped at 1MiB and each endpoint's admission at 8MiB.
Oversized source records are reported and left pending, never fabricated as an
empty graph payload. Sink count/byte capacity remains held through callbacks.
**Public-client limitation:** Redis COUNT limits records, not bytes. Native reply
decoding occurs before admission checks, so a huge input can exceed these retained
work limits transiently. This is not a total-process heap guarantee.

A failed operation invalidates the connection epoch and reports the failure.
Bounded recovery replaces native pools with at most 60 attempts, 250ms apart,
using fresh connect deadlines and the owner's lifetime cancellation. Startup
validation runs again without resetting existing groups. There is no replay of
ambiguous XADD. A permanently exhausted owner stops rather than spinning forever.

Stop first closes admission and cancels the blocking read, while the ACK/control
connection remains usable during drain. The supplied Stop context bounds waiting
for admitted source handlers and sink publications/callbacks. At expiry, request
contexts/tasks are cancelled and native connections are disconnected using the
public nowait API. Application code that ignores cancellation may outlive Stop;
EndRequest never races an active source handler. At most bounded admitted work
and one transport reporter remain owned in that case. No process, worker thread,
unbounded queue or immortal reconnect task is required by the adapter.

## Acceptance

```sh
REDIS_TEST_URL=redis://127.0.0.1:16379 .venv/bin/python -m pytest -q \
  tests/test_redisstreams_config.py tests/test_redisstreams_broker.py
```

The native fixture uses adapter-test, unique keys and databases 0/1, cleaning only
its recorded keys. It never stops, restarts or reconfigures the shared broker.
Faults use owned TCP/TLS proxies. OpenSSL creates disposable TLS test certificates.
Native tests skip when REDIS_TEST_URL is absent. Generated canonical service
acceptance remains a separate parent-owned generator gate.

Primary sources: [official Redis Python client](https://redis.io/docs/latest/develop/clients/redis-py/),
[redis-py 8.1.0 release](https://github.com/redis/redis-py/releases/tag/v8.1.0),
[async usage](https://redis.io/docs/latest/develop/clients/redis-py/async/),
[retry configuration](https://redis.io/docs/latest/develop/clients/redis-py/produsage/),
[XACK](https://redis.io/docs/latest/commands/xack/),
[XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/).

Local acceptance evidence (2026-11-17): **153 tests passed**, zero failures/skips
in the combined Redis/config/native-adapter/request-context gate. Redis-specific
coverage is **40 configuration tests and 27 native scenarios**. Tests ran on
Python 3.14.6 with the shared Redis 7.4 broker. Native scenarios include a full
source→async XADD→correlated result→XACK path, binary duplicate fields, empty
reclaim pages/deleted IDs, lost read and write replies, owner cancellation,
backpressure, cancellation-resistant drain, mTLS/trust/hostname verification,
idle control-connection reconnection fencing during normal work and drain,
asynchronous identity/callback failure propagation, and retained-message admission.
The latter uses redis-py's public register_connect_callback notification and
CLIENT KILL against only the exact test-owned connection ID.

Scoped Ruff lint/format checks, source compilation and SDK wheel/sdist builds
passed. Wheel metadata includes redis==8.1.0 and both adapter packages. Existing
NATS/RabbitMQ/config/context tests were included; this is not a claim that the
entire repository suite or generated canonical Python service gate has run.
