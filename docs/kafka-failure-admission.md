# Kafka source failure and assignment boundaries

A failed BeginRequest, ConsumeMessage, result callback, or cancelled request parks
only its assigned partition. Queued successors are discarded and subsequent
fetches cannot reopen that lane. Independent partitions continue. A fresh group
assignment reopens the partition; this does not introduce automatic handler
retry or force a group rebalance. Operational recovery may therefore require
reassignment/restart. EndRequest runs exactly once after successful BeginRequest,
including cancellation during ConsumeMessage. BeginRequest failure has no state
to end. Result callback exceptions still propagate unchanged to existing result
stream error routing, while waking the owning request to fence its partition.

Both ordinary and checkpoint-restored subscriptions install the native rebalance
listener. Revocation closes admission and fences retained mark/commit handles
before draining lanes and removing pending marks. Reassignment establishes a new
epoch. Ordinary assignments do not seek or manufacture checkpoint offsets.
Retained marks become no-ops; stale explicit commits fail before native admission.
Revocation during a native commit rejects its completion as stale, but cannot undo
a broker commit already issued. Explicit ACK before a later processing failure
likewise cannot be undone. Done alone never commits. Kafka offsets remain
cumulative; this is not an individual-record acknowledgement protocol.

Focused local gate:

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -p no:cacheprovider \
  tests/test_kafka_failure_admission.py tests/test_kafka_source_scheduling.py \
  tests/test_kafka_source_lifecycle.py tests/test_kafka_checkpoint.py \
  tests/test_kafka_source_startup.py -q
```

The local gate exercises the actual scheduler, request lifecycle and rebalance
listener with fake native transport calls. Native acceptance is separately
reproducible with an isolated, uniquely named Apache Kafka 4.1.2 container:

```sh
.venv/bin/python tests/run_kafka_failure_native.py
```

The runner creates no volumes, exposes only a dynamically chosen loopback port,
uses UUID topics/groups, and removes its own broker in `finally`. It runs six
native cases plus the focused local lifecycle block. No shared broker is changed.
To use an externally managed disposable broker, set `SA_TEST_KAFKA_BROKERS` and run
`tests/test_kafka_failure_native.py` directly with pytest.

Native cases cover synchronous failure and asynchronous result callback
cancellation in ordinary/restore modes: p0 has a verified nonempty native fetch
cache, delivers only offset 0 and stays committed at 0, while p1 advances to 8.
Additional traffic and multiple periodic commit intervals cannot reopen p0.
Two real group members then exercise ordinary/restore revocation and reassignment:
stale marks leave the managed offset map unchanged; old explicit commits reject;
newly assigned handles process and commit subsequent traffic. Actual broker
committed offsets are checked, rather than relying on local counters alone.

This does not promise rollback of commits already submitted before failure or
revocation, nor cover broker crashes/network partitions or every group assignor.

Verified locally with aiokafka 0.14.0 / Apache Kafka 4.1.2: **58 passed in
30.19s** (6 native + 52 focused local), recorded in
`/private/tmp/sa-python-kafka-b01-native.log`. The final run included the explicit
pre-admission native fetch-cache observation and stale-mark map assertions.
All runner-owned containers were removed. No production correction was needed
following this native acceptance.
