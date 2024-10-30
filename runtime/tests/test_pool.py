#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.


from typing import List
import os
from pathlib import Path
from datetime import timedelta, datetime

from pyservicelib.runtime.pool import make_delay_pool
from pyservicelib.runtime.serviceapp import  ServiceAppLoader
from pyservicelib.runtime.context import default_context
from pyservicelib.runtime.config import  ConfigSettings
from pyservicelib.runtime.tests.mockservice import MockService, MockServiceConfig, MockServiceDependency


async def test_delay_pool():
   os.chdir(Path(__file__).parent)
   delays: List[int] = [1000, 5000, 1200, 3000, 1500, 4000, 1350, 900, 100, 500, 500, 500, 500, 500, 500, 500]
   recorded_delays: List[int] = []

   service = await ServiceAppLoader[MockService, MockServiceConfig]().init(
      "MockService", MockServiceDependency(), ConfigSettings())
   ctx = default_context()

   delay_pool = make_delay_pool(service)
   await delay_pool.start(ctx)

   async def task_with_delay(value: int, start_time: datetime):
      time_difference = int((datetime.now() - start_time).total_seconds() * 1000)
      assert abs(time_difference - value) <= 5
      recorded_delays.append(value)

   for delay in delays:
      await delay_pool.add_task(timedelta(milliseconds=delay), task_with_delay, delay, datetime.now())

   await delay_pool.stop(ctx)
   delays.sort()
   assert recorded_delays == delays

   await service.stop(ctx)
   await service.release()

