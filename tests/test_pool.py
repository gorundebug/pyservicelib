#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.
import asyncio
import pytest
import os
from pathlib import Path
from datetime import timedelta, datetime

from ..pyservicelib.runtime.pool import make_delay_pool
from ..pyservicelib.runtime.pool.threadpool import AsyncThreadPoolExecutor, AsyncFuture
from ..pyservicelib.runtime.serviceapp import ServiceAppLoader
from ..pyservicelib.runtime.context import default_context
from ..pyservicelib.runtime.config import  ConfigSettings
from .mockservice import MockService, MockServiceConfig, MockServiceDependency


@pytest.mark.asyncio
async def test_delay_pool():
   os.chdir(Path(__file__).parent)
   delays: list[int] = [1000, 5000, 1200, 3000, 1500, 4000, 1350, 900, 100, 500, 500, 500, 500, 500, 500, 500]
   recorded_delays: list[int] = []

   service = await ServiceAppLoader[MockService, MockServiceConfig]().load(
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

@pytest.mark.asyncio
async def test_async_pool():
   pool = AsyncThreadPoolExecutor(5)

   delays: list[float] = [5, 5, 5, 5, 5, 2, 2, 2, 2, 2]
   counter = len(delays)

   async def task(value: float):
      nonlocal counter
      await asyncio.sleep(value)
      counter -= 1
      return value

   tasks: list[AsyncFuture] = []

   start_time = datetime.now()

   for delay in delays:
      tasks.append(pool.add_task(task, delay))

   results = await asyncio.gather(*[future.result() for future in tasks])

   end_time = datetime.now()
   time_difference = 7000 - int((end_time - start_time).total_seconds() * 1000)

   assert all(result in delays for result in results)
   assert counter == 0

   assert abs(time_difference) <= 50

   pool.shutdown()

@pytest.mark.asyncio
async def test_async_pool_add_task_without_block():
   pool = AsyncThreadPoolExecutor(5)

   delays: list[float] = [5, 5, 5, 5, 5, 2, 2, 2, 2, 2]
   counter = len(delays)

   async def task(value: float):
      nonlocal counter
      await asyncio.sleep(value)
      counter -= 1
      return value

   tasks: list[AsyncFuture] = []

   for delay in delays:
      tasks.append(pool.add_task(task, delay))

   start_time = datetime.now()

   results = await asyncio.gather(*[future.result() for future in tasks])

   end_time = datetime.now()
   time_difference = 7000 - int((end_time - start_time).total_seconds() * 1000)

   assert all(result in delays for result in results)
   assert counter == 0

   assert abs(time_difference) <= 50

   pool.shutdown()

@pytest.mark.asyncio
async def test_async_pool_shutdown():
   pool = AsyncThreadPoolExecutor(5)

   delays: list[float] = [5, 5, 5, 5, 5, 2, 2, 2, 2, 2]
   counter = len(delays)

   async def task(value: float):
      nonlocal counter
      await asyncio.sleep(value)
      counter -= 1
      return value

   tasks: list[AsyncFuture] = []

   start_time = datetime.now()

   for delay in delays:
      tasks.append(pool.add_task(task, delay))

   pool.shutdown()
   results = await asyncio.gather(*[future.result() for future in tasks])

   end_time = datetime.now()
   time_difference = 7000 - int((end_time - start_time).total_seconds() * 1000)

   assert all(result in delays for result in results)
   assert counter == 0

   assert abs(time_difference) <= 50