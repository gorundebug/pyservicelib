"""Run only Python Kafka B01 gates using a uniquely owned Apache Kafka container.

Run from pyservicelib: .venv/bin/python tests/run_kafka_failure_native.py
No volumes, shared broker changes, authentication, or host credential reads.
"""
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
name = 'sa-python-kafka-b01-' + uuid.uuid4().hex[:12]
image = 'apache/kafka:4.1.2'
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]


def docker(*args, **kwargs):
    return subprocess.run(['docker', *args], check=True, timeout=30, **kwargs)


try:
    docker('run', '-d', '--name', name, '--cpus=2', '--memory=1g',
           '-p', f'127.0.0.1:{port}:9092',
           '-e', 'KAFKA_NODE_ID=1', '-e', 'KAFKA_PROCESS_ROLES=broker,controller',
           '-e', 'KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093',
           '-e', f'KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{port}',
           '-e', 'KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093',
           '-e', 'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER',
           '-e', 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
           '-e', 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1',
           '-e', 'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0',
           '-e', 'KAFKA_HEAP_OPTS=-Xms256m -Xmx512m', image)
    print(f'Owned broker {name}; image {image}; loopback port {port}', flush=True)
    # Probe the public native client from the host, including advertised address.
    probe = '''import asyncio
from aiokafka.admin import AIOKafkaAdminClient
async def main():
    client = AIOKafkaAdminClient(bootstrap_servers="127.0.0.1:PORT", request_timeout_ms=1500)
    try:
        await client.start()
    finally:
        await client.close()
asyncio.run(main())
'''.replace('PORT', str(port))
    until = time.monotonic() + 60
    while True:
        result = subprocess.run([sys.executable, '-c', probe], cwd=ROOT,
                                capture_output=True, timeout=8)
        if result.returncode == 0:
            break
        if time.monotonic() >= until:
            raise RuntimeError('isolated broker did not become ready: ' + result.stderr.decode())
        time.sleep(1)
    env = dict(os.environ, SA_TEST_KAFKA_BROKERS=f'127.0.0.1:{port}', PYTHONDONTWRITEBYTECODE='1')
    result = subprocess.run([sys.executable, '-m', 'pytest', '-p', 'no:cacheprovider', '-q',
        'tests/test_kafka_failure_native.py', 'tests/test_kafka_failure_admission.py',
        'tests/test_kafka_source_scheduling.py', 'tests/test_kafka_source_lifecycle.py',
        'tests/test_kafka_checkpoint.py', 'tests/test_kafka_source_startup.py'],
        cwd=ROOT, env=env, timeout=300)
    raise SystemExit(result.returncode)
finally:
    # Retain diagnostics without retaining the disposable container or volumes.
    with open('/private/tmp/' + name + '.broker.log', 'w') as log:
        subprocess.run(['docker', 'logs', name], stdout=log, stderr=subprocess.STDOUT, timeout=15)
    subprocess.run(['docker', 'rm', '-f', '-v', name], timeout=30, check=True)
