#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import time
from typing import Optional

from opentelemetry.sdk.resources import Resource
from opentelemetry._logs import SeverityNumber
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry._logs._internal import LogRecord
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, SimpleLogRecordProcessor

from ...environment.log import Logger, LogsEngine, Config, Field, FieldType


def _field_to_attr(f: Field):
    """Convert a Field to (key, value) for OTel log attributes."""
    v = f._val
    if f.type == FieldType.STRING:
        return f.key, str(v) if v is not None else ''
    if f.type == FieldType.INT64:
        return f.key, int(v) if v is not None else 0
    if f.type == FieldType.FLOAT64:
        return f.key, float(v) if v is not None else 0.0
    if f.type == FieldType.BOOL:
        return f.key, bool(v)
    if f.type == FieldType.ERROR:
        return f.key, str(v) if v is not None else ''
    return f.key, str(v) if v is not None else ''


class _OtelLogger(Logger):
    __slots__ = ('_logger',)

    def __init__(self, logger) -> None:
        self._logger = logger

    def _emit(self, severity: SeverityNumber, msg: str, fields: tuple[Field, ...]) -> None:
        attrs = {k: v for k, v in (_field_to_attr(f) for f in fields)} if fields else {}
        ts = time.time_ns()
        record = LogRecord(
            timestamp=ts,
            observed_timestamp=ts,
            severity_number=severity,
            severity_text=severity.name,
            body=msg,
            attributes=attrs if attrs else None,
        )
        self._logger.emit(record)

    def debug(self, msg: str, *fields: Field) -> None:
        self._emit(SeverityNumber.DEBUG, msg, fields)

    def info(self, msg: str, *fields: Field) -> None:
        self._emit(SeverityNumber.INFO, msg, fields)

    def warn(self, msg: str, *fields: Field) -> None:
        self._emit(SeverityNumber.WARN, msg, fields)

    def error(self, msg: str, *fields: Field) -> None:
        self._emit(SeverityNumber.ERROR, msg, fields)


class OtelLogsEngine(LogsEngine):

    def __init__(self, provider: LoggerProvider, service_name: str) -> None:
        self._provider = provider
        self._logger = _OtelLogger(provider.get_logger(service_name))

    def default_logger(self, cfg: Optional[Config] = None) -> Logger:
        return self._logger

    async def shutdown(self) -> None:
        self._provider.shutdown()


def create_stdout_logs_engine(service_name: str) -> LogsEngine:
    """Create a LogsEngine that writes structured log records to stdout."""
    from opentelemetry.sdk._logs.export import ConsoleLogRecordExporter
    resource = Resource({'service.name': service_name})
    provider = LoggerProvider(resource=resource)
    provider.add_log_record_processor(SimpleLogRecordProcessor(ConsoleLogRecordExporter()))
    return OtelLogsEngine(provider, service_name)


def create_otlp_logs_engine(service_name: str, endpoint: str = 'localhost:4317',
                             insecure: bool = True) -> LogsEngine:
    """Create a LogsEngine that exports log records via OTLP gRPC."""
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
    resource = Resource({'service.name': service_name})
    provider = LoggerProvider(resource=resource)
    exporter = OTLPLogExporter(endpoint=endpoint, insecure=insecure)
    provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
    return OtelLogsEngine(provider, service_name)
