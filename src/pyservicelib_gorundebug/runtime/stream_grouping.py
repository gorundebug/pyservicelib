"""Bounded definition labels for concrete stream observability, never instances."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .common import Stream


def stream_grouping(stream: Stream) -> tuple[str, str]:
    config = stream.config
    return config.pipeline or "", config.component or ""
