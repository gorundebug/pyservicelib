"""User-owned service extensions. The generator never overwrites this file."""

from pyservicelib_gorundebug.api.models.environment import Environment
from pyservicelib_gorundebug.runtime.environment import (
    ServiceDependency,
    ServiceEnvironment,
)
from pyservicelib_gorundebug.runtime.environment.log import LogsEngine
from pyservicelib_gorundebug.runtime.environment.metrics import MetricsEngine
from pyservicelib_gorundebug.runtime.environment.tracing import TracingEngine
from pyservicelib_gorundebug.runtime.context import Context
from pyservicelib_gorundebug.runtime.telemetry import (
    create_otlp_logs_engine,
    create_otlp_metrics_engine,
    create_otlp_tracing_engine,
    create_pretty_tracing_engine,
    create_prometheus_metrics_engine,
)

from .service_generated import GeneratedService


class Dependency(ServiceDependency):
    async def get_logs_engine(self, env: ServiceEnvironment) -> LogsEngine | None:
        if _uses_otlp(env):
            return create_otlp_logs_engine(env.service_config.name)
        # None selects servicelib's human-readable asynchronous local logger.
        return None

    async def get_metrics_engine(self, env: ServiceEnvironment) -> MetricsEngine | None:
        if _uses_otlp(env):
            return create_otlp_metrics_engine()
        return create_prometheus_metrics_engine()

    async def get_tracing_engine(
        self,
        env: ServiceEnvironment,
    ) -> TracingEngine | None:
        if _uses_otlp(env):
            return create_otlp_tracing_engine(env.service_config.name)
        return create_pretty_tracing_engine(
            env.service_config.name,
            context_sampler=True,
        )


class Service(GeneratedService):
    async def custom_makers_init(self, ctx: Context) -> None:
        del ctx

    async def custom_functions_init(self, ctx: Context) -> None:
        del ctx

    async def on_start(self, ctx: Context) -> None:
        del ctx

    async def on_stop(self, ctx: Context) -> None:
        del ctx


def _uses_otlp(env: ServiceEnvironment) -> bool:
    return env.service_config.environment in {
        Environment.Staging,
        Environment.Production,
    }
