import asyncio
import signal

from pyservicelib_gorundebug.runtime.config import ConfigSettings
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.serviceapp import ServiceAppLoader

from .internal.app import Dependency, Service
from .internal.config import Config


async def run() -> None:
    service = await ServiceAppLoader[Service, Config]().load(
        "Order Service",
        Dependency(),
        ConfigSettings(),
    )
    await service.start_service(Context())

    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopped.set)
    await stopped.wait()
    await service.stop_service(Context())


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
