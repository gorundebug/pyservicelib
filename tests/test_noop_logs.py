from pyservicelib_gorundebug.runtime.environment.log import NoopLogsEngine


def test_noop_logs_engine() -> None:
    engine = NoopLogsEngine()
    logger = engine.default_logger()
    logger.debug("debug")
    logger.info("info")
    logger.warn("warn")
    logger.error("error")
