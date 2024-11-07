#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.

from enum import Enum

from pyservicelib.runtime.environment.metrics import MetricsEngine
from pyservicelib.runtime.telemetry.prometheus.prometheus import PrometheusMetricsEngine


class MetricsEngineType(str, Enum):

    PROMETHEUS = 'PROMETHEUS'


class MetricsEngineFactory:

    @classmethod
    async def create_metrics_engine(cls, engine_type: MetricsEngineType) -> MetricsEngine:
        if engine_type == MetricsEngineType.PROMETHEUS:
            return await PrometheusMetricsEngine.engine()
        raise ValueError(f"Unsupported metrics engine type {engine_type}")