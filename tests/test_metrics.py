#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

import pytest

from pyservicelib_gorundebug.runtime.environment.metrics import CounterOpts
from pyservicelib_gorundebug.runtime.telemetry import MetricsEngineFactory, MetricsEngineType

@pytest.mark.asyncio
async def test_metrics():
    engine = await MetricsEngineFactory.create_metrics_engine(MetricsEngineType.PROMETHEUS)
    metrics = engine.metrics

    counter = metrics.counter(CounterOpts(name="test",
                                          documentation="test metric",
                                          const_labels={"service": "test"}
                                          ))
    counter.inc()

    counter_vec = metrics.counter_vec(CounterOpts(name="testvec",
                                          documentation="test vec metric",
                                          const_labels={"service": "test", "service1": "test2"}
                                          ), ("value1", "value2"))

    value_counter = counter_vec.with_label_values(1, 2)
    value_counter.inc()

    value_counter = counter_vec.with_label_values(1, 2)
    value_counter.inc()
