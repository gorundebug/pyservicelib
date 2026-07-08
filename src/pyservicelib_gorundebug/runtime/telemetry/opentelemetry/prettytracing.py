#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import sys
from io import TextIOBase
from typing import Optional, Sequence

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.trace import StatusCode


class PrettySpanExporter(SpanExporter):
    """Prints completed spans in a human-readable single-line format.

    Each span occupies one line:

        [1782d516][HotelSearch     ]  23:01:20.203    120ms  grpc.output    endpoint="Search Rooms"
        [1782d516][HotelSearch     ]  23:01:20.365     12ms  grpc.output  ✖ endpoint="Search Rooms"  error="deadline exceeded"
    """

    def __init__(self, out: Optional[TextIOBase] = None) -> None:
        self._out = out or sys.stdout

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        if not spans:
            return SpanExportResult.SUCCESS
        sorted_spans = sorted(spans, key=lambda s: s.start_time or 0)
        for s in sorted_spans:
            self._print_span(s)
        return SpanExportResult.SUCCESS

    def _print_span(self, s: ReadableSpan) -> None:
        sc = s.context
        trace_id = ''
        if sc is not None and sc.is_valid:
            trace_id = format(sc.trace_id, '032x')[:8]

        svc_name = ''
        if s.resource is not None:
            for attr in s.resource.attributes.items():
                if attr[0] == 'service.name':
                    svc_name = str(attr[1])
                    break

        start_ns = s.start_time or 0
        end_ns = s.end_time or start_ns
        dur_ns = end_ns - start_ns

        from datetime import datetime, timezone
        start_dt = datetime.fromtimestamp(start_ns / 1e9, tz=timezone.utc)
        start_str = start_dt.strftime('%H:%M:%S.') + f'{start_dt.microsecond // 1000:03d}'

        status_mark = '✖ ' if (s.status is not None and s.status.status_code == StatusCode.ERROR) else '  '

        parts = [
            f'[{trace_id}]',
            f'[{svc_name:<16}]',
            f'  {start_str}',
            f'  {_pretty_duration(dur_ns):>8}',
            f'  {s.name:<28}',
            status_mark,
        ]

        attrs = s.attributes or {}
        for k, v in attrs.items():
            parts.append(f' {k}={str(v)!r}')

        events = s.events or []
        if events:
            ev_parts = []
            for ev in events:
                ev_attrs = ev.attributes or {}
                if not ev_attrs:
                    ev_parts.append(ev.name)
                else:
                    inner = '  '.join(f'{k}={str(v)!r}' for k, v in ev_attrs.items())
                    ev_parts.append(f'{ev.name}({inner})')
            parts.append('  » ' + ' '.join(ev_parts))

        print(''.join(parts), file=self._out)

    def shutdown(self) -> None:
        pass


def _pretty_duration(ns: int) -> str:
    ms = ns // 1_000_000
    us = ns // 1_000
    if ns < 1_000_000:
        return f'{us}µs'
    if ns < 1_000_000_000:
        return f'{ms}ms'
    return f'{ns / 1e9:.3f}s'
