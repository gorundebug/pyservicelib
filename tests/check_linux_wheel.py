"""Run with the Python executable of a clean Linux venv containing the SDK wheel."""
import importlib
import importlib.metadata
import json
from pathlib import Path
import platform
import sys
import sysconfig


assert sys.platform == "linux", sys.platform
assert sys.prefix != sys.base_prefix, "use an isolated venv"
installed = Path(sysconfig.get_paths()["purelib"]).resolve()
entries = []
for broker in ("nats", "pulsar", "redisstreams", "rabbitmq"):
    for direction in ("datasource", "datasink"):
        name = f"pyservicelib_gorundebug.{direction}.{broker}"
        module = importlib.import_module(name)
        assert Path(module.__file__).resolve().is_relative_to(installed), module.__file__
        entries.append(name)

versions = {}
for distribution, expected in {
    "nats-py": "2.12.0",
    "pulsar-client": "3.13.0",
    "redis": "8.1.0",
    "rabbitmq-amqp-python-client": "1.0.1",
}.items():
    actual = importlib.metadata.version(distribution)
    assert actual == expected, (distribution, actual, expected)
    versions[distribution] = actual

import pulsar

client = pulsar.Client("pulsar://127.0.0.1:6650", operation_timeout_seconds=1)
client.close()
print(json.dumps({
    "status": "passed", "sdk": importlib.metadata.version("pyservicelib_gorundebug"),
    "python": platform.python_version(), "machine": platform.machine(),
    "libc": platform.libc_ver(), "entries": entries, "versions": versions,
    "nativePulsarConstructAndClose": True,
}, indent=2))
