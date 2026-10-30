"""Canonical process-local interval syntax shared with Service Architect."""

import re
from datetime import timedelta


def parse_local_schedule_interval(expression: str) -> timedelta | None:
    if not expression.startswith("@every"):
        return None
    match = re.fullmatch(r"@every ([1-9][0-9]{0,7})ms", expression)
    if match is None or not 10 <= int(match.group(1)) <= 86_400_000:
        raise ValueError("local interval requires @every <10..86400000>ms")
    return timedelta(milliseconds=int(match.group(1)))
