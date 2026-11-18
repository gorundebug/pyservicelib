# Clean Linux wheel acceptance

2026-11-18: SDK 0.2.181 installs into a fresh venv on Linux aarch64,
Python 3.12.16, glibc 2.36. No system site packages or source checkout are used.
All dependencies are resolved through the configured local PyPI proxy.
`pip check` passes, all eight broker modules resolve inside the venv, pinned
broker versions match, and the official Pulsar native client constructs/closes.

Evidence: `/private/tmp/sa-python-clean-linux-wheel-fixed.log`.
The first run, `/private/tmp/sa-python-clean-linux-wheel.log`, failed because
Proton's source build could not find `gcc`; it is not counted as acceptance.
Both generated Python image templates now install
`build-essential libssl-dev libffi-dev` alongside git/CA certificates.
The dependency is currently unconditional in the SDK, so these build packages
are needed even for a graph without a RabbitMQ connector. Generated service and
workspace Dockerfile checks pass in `sa-python-native-build-generation.log`.

Wheel: `/private/tmp/sa-broker-sdk-packages/pyservicelib_gorundebug-0.2.181-py3-none-any.whl`.
SHA256: `5c6e838ca7c7c70877c242110716c82e2fd95a4cc901aebdc59bfbdebc83df4d`.
Build log: `/private/tmp/sa-python-linux-wheel-build.log`.

To repeat, use a disposable Linux Python 3.12 container with those native build
packages and the configured APT/PyPI mirrors. Mount the wheel directory and
`check_linux_wheel.py` read-only, then run:

```sh
python -m venv /tmp/wheel-check
/tmp/wheel-check/bin/python -m pip install --timeout 20 --retries 1 \
  /artifacts/pyservicelib_gorundebug-0.2.181-py3-none-any.whl
/tmp/wheel-check/bin/python -m pip check
/tmp/wheel-check/bin/python /check_linux_wheel.py
```

The retained test used local `servicelib-standalone-python:3.12`, a fresh venv,
APT `apt-debian`/`apt-debian-security` and PyPI `pypi-proxy` Nexus repositories.
Only the disposable container was modified. The upstream
[Proton installation guide](https://github.com/apache/qpid-proton/blob/main/INSTALL.md)
documents the native compiler/OpenSSL prerequisites.

This is clean package installation and native loading, not execution of the
complete generated Docker build, live broker acceptance, AMD64/musl support or
a lock of every transitive Python dependency. Resolved versions are retained in
the successful pip log; existing broad SDK dependency ranges remain unchanged.
