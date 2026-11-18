# Python Pulsar native adapter

The SDK uses **Apache `pulsar-client==3.13.0`** and its public `pulsar.asyncio`
Client/Producer/Consumer APIs. No native private attribute, monkey patch, custom
callback bridge, reactor, or SA worker thread is used. The official package's
asyncio implementation owns its callback-to-future dispatch. Its async NACK
currently delegates the nonblocking native NACK call through its own `to_thread`;
this is upstream behavior, not an adapter executor.

Reviewed primary sources: [Apache installation/version guidance](https://pulsar.apache.org/docs/client-libraries/python-setup/),
[Apache Python usage and explicit acknowledgments](https://pulsar.apache.org/docs/client-libraries/python-use/),
and the installed 3.13.0 public `pulsar/asyncio.py` and `pulsar/__init__.py` source.

## Runtime contract

`PulsarDataConnectorConfig` requires one `pulsar://` or `pulsar+ssl://` server URL
and separate `adminUrl`. No inline credentials/path/query/fragment. Defaults are
2s connect and 5s operation timeouts (runtime zero selects the default). Token or
TLS certificate/key authentication is supported, not username/password or
credentialsFile. TLS follows the URL; hostname verification is explicitly on,
insecure TLS is off. CA and paired certificate/key paths are supported. A TLS
binary URL requires HTTPS administration. There is no separate server-name
override in this profile. Tokens stay out of runtime-to-architecture projection.

`PulsarEndpointConfig` takes one fully qualified `persistent://tenant/namespace/topic`.
Both `createTopic` and `createSubscription` are opt-in. Partitions0..1000 apply
only to explicit topic creation; no expansion/reconciliation. Source fields:
subscriptionName, subscriptionType Shared/Exclusive/Failover (defaultShared),
subscriptionInitialPosition Earliest/Latest (creation only; defaultEarliest),
receiverQueueSize1..4096 (default256), negativeAckRedeliveryDelayMs
(default60000). Sink publisherMaxInFlight1..4096 defaults256. Opposite-role fields
are rejected. No pattern/multi-topic subscription, KeyShared, custom schema,
transaction, replication, scheduling, or batching controls are exposed.

Administrative access is a startup prerequisite. Before connecting, the adapter
requires explicit namespace overrides `allowAutoTopicCreation=false` and, for
sources, `allowAutoSubscriptionCreation=false`. Null/inherited policies are not
accepted as proof. The adapter never changes them or creates tenants/namespaces.
Operators must keep these policies enforced to cover deletion races. Missing
resources are created only under their individual flags; conflicts trigger a
re-read, not blind success. Existing cursors are never reset. HTTP requests and
responses are bounded; redirects are rejected, avoiding cross-origin credential
forwarding. Broker error response bodies are not included in exceptions.

Source maker: `datasource.pulsar.make_pulsar_endpoint_consumer(stream, handler)`.
Sink makers: `datasink.pulsar.make_pulsar_endpoint_consumer` and
`make_pulsar_endpoint_consumer_with_result`. Existing public DataSource/DataSink
and typed StreamContext result/error interfaces are unchanged.

A ConsumerMessage exposes the official native `message` (MessageId, bytes,
properties, key, publish/event time and redelivery count), plus `data`/`headers`.
`await ack()` is individual ACK; `await nack()` requests native redelivery.
Completion follows the official API, not an invented durable broker receipt.
Handler return, Done and EndRequest never ACK. Handles retire with their request
and logical consumer session. The native Pulsar consumer maintains its identity
across its reconnects; no physical-socket epoch or exclusive business-side-effect
fencing is claimed. Register result callbacks before emitting downstream work.

A sink fills `message.message = ProducerMessage(data, properties, key, event_timestamp)`.
`message.send(lambda delivery: typed_result)` admits an async publication;
callbacks may also be async and retain their admission slot through completion.
`await message.send_sync()` explicitly waits. Payload/properties are snapshotted
before async submission. The original request's EndRequest can precede delivery;
callback state must remain valid. Retained handles reject new send/send_sync after
ConsumeMessage returns, including while an earlier callback remains active.

Delivery outcomes:

- **NotSent:** before native handoff (invalid payload, expired/cancelled context,
  wrong role, stopped/capacity), or definitive native local queue/validation failure.
- **Accepted:** official producer confirmation with native MessageId.
- **Rejected:** explicit native authentication/authorization/schema/fencing/termination rejection.
- **Unknown:** timeout, cancellation or connection/other ambiguous failure after
  native send begins. Native AlreadyClosed is conservatively Unknown because
  shutdown can terminate an already pending publication.

The original failure is retained in Delivery.error. Immediate graph admission
raises AdmissionRejected (`outcome='NotSent'`). SA does not retry sends. Apache's
native client can reconnect/retransmit pending publications; this adapter cannot
promise zero native retransmission or exactly-once effects. A confirmed ID is not
proof of consumer processing, disk/replica durability or business success.

Sources have one outstanding async receive and bounded concurrent handlers
(1..min(64, receiverQueueSize)). Both per-partition and cross-partition native
receiver/publisher limits are configured. Publication batching is disabled;
batch-index individual ACK is enabled for externally batched messages. Retained
application payloads are capped at1MiB each and8MiB per endpoint; these bounds do
not cap all native allocator/decoder memory. No additional unbounded SA queue.

Stop first closes admission, then drains handlers/callbacks within its Context.
At deadline it cancels local waits and reports unfinished cleanup. Native client
close starts once and remains owned by the session; a later Stop can observe the
same task. Timeout does not mean native cleanup succeeded. Cancellation of receive
may leave an already dispatched native delivery pending; closing/replacing the
durable consumer redelivers unacknowledged data. Shutdown never unsubscribes.
Python user callbacks must cooperate with the event loop; CPU-blocking user code
cannot be preempted by asyncio deadlines.

Properties retain caller fields and propagate case-insensitive x-stream-id,
x-trace and W3C tracing through the existing context facilities. There is no JSON
transport envelope.

## Focused acceptance

```sh
PULSAR_TEST_URL=pulsar://127.0.0.1:16650 \
PULSAR_TEST_ADMIN_URL=http://127.0.0.1:18080 \
.venv/bin/python -m pytest -q tests/test_pulsar_config.py \
  tests/test_pulsar_outcomes.py tests/test_pulsar_broker.py tests/test_pulsar_api_schema.py
```

Native tests create a unique public/py-UUID namespace, explicitly disable both
auto-creation policies there, and remove only owned topics/namespace. They never
restart/reconfigure the shared broker. Fault/TLS fixtures forward real broker
traffic; only lookup addresses are rewritten to keep clients on the proxy, and
the ambiguity test withholds a native SEND_RECEIPT after persistence.

Coverage includes partitioned/non-partitioned provisioning; explicit ACK versus
Done; correlation and tracing properties; binary payload/key/event time; NACK and
pending redelivery; Shared/Exclusive/Failover and Latest creation/cursor reuse;
multiple consumers and individual out-of-order ACK; callback-held capacity,
retired handles and bounded/repeated Stop; reply loss and reconnect; TLS/mTLS
publication and rejection of wrong host, untrusted CA and missing identity.
Call-count tests prove zero native sends before admission rejection and one after
an ambiguous failure. The TLS fixture tests SDK identity options, not a broker TLS
deployment. Token ACL integration is now verified with the separate isolated official fixture
described below. Generated services/images and cross-architecture
binary packaging remain separate gates; no generator capability is enabled here.

Recorded on2026-11-17: 68 focused Pulsar checks (28 config/projection,18 outcome,
14 native,8 binding compatibility) pass. The last affected outcome/native run
passes32 tests; the unchanged configuration/binding checks passed in the preceding
full run. Another119 adjacent Redis/NATS/RabbitMQ/nested-config checks pass.
Scoped Ruff/format and wheel/sdist builds pass; wheel metadata pins
pulsar-client==3.13.0 and includes both adapter packages. These are local Python3.14
macOS arm64 results against the existing Pulsar4.1.2 broker, not generated-image
or cross-platform certification.

## Separate runtime admin credentials

`PulsarDataConnectorConfig.admin_token: str = ""` (input alias `adminToken`) is
REST-only. Nonempty values override the admin Bearer header; empty/absent keeps
legacy `token` fallback. Native authentication always uses `token`. A rejected
admin request never retries with native credentials. Use a tenant-admin token for
preflight and a scoped producer/consumer token for native traffic. No policy
validation or explicit creation check is waived. The process still holds both
secrets. `adminToken` plus TLS client identity and CR/LF are rejected; CA-only TLS
with tokens remains supported.

The field is excluded from repr/model serialization and canonical API projection;
the Session's validated runtime snapshot explicitly retains it. Generator/env
binding remains separate. The isolated cross-language fixture in sibling
`tsservicelib/test/native-probes/pulsar-auth.test.mjs` passes 28 checks including
Python native separation plus 29 configuration tests and Go race acceptance.
It verifies scoped denials, failed admin authentication without fallback, no
credential values in errors/client/broker logs, and bounded shutdown. The fixture
is removed after each run without touching the shared broker.
