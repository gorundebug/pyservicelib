"""Process failure boundary for detached runtime callbacks, not workflow tasks."""

import os
import sys
import traceback
from typing import NoReturn


def terminate_background_failure(error: BaseException) -> NoReturn:
    """A detached callback has no caller that can own its unexpected failure."""
    try:
        print("servicelib: unhandled background callback", file=sys.stderr, flush=True)
        traceback.print_exception(error, file=sys.stderr)
        sys.stderr.flush()
    except BaseException:
        # A broken diagnostic sink must not turn a fatal failure into success.
        pass
    finally:
        os._exit(2)
