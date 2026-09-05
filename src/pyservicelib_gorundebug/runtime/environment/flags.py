#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See the LICENSE file for details.

import os


def flag_enabled(name: str) -> bool:
    """Apply the canonical boolean environment-variable contract."""
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}
