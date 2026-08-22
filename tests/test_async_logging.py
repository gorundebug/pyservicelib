import logging

from pyservicelib_gorundebug.runtime.logging.asynclog.asynclog import AsyncLogger


def test_default_async_logger_does_not_reconfigure_root_logger():
    root = logging.getLogger()
    original_level = root.level
    original_handlers = tuple(root.handlers)

    logger = AsyncLogger()

    assert logger._log.name == "pyservicelib"
    assert logger._log.propagate is False
    assert root.level == original_level
    assert tuple(root.handlers) == original_handlers
