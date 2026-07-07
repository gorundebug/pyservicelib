#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import TYPE_CHECKING, cast

from ..api.models.stream_app import StreamApp
from ..api.models.stream import Stream as ApiStream
from ..api.models.pool import Pool
from ..api.models.link import Link
from ..api.models.call_semantics import CallSemantics
from .config.config import ServiceConfig, StreamConfig
from .config.config_to_api import (
    service_config_to_api,
    stream_config_to_api,
    data_connector_config_to_api,
    endpoint_config_to_api,
)

if TYPE_CHECKING:
    from .serviceapp import ServiceApp


def runtime_to_stream_app(app: "ServiceApp") -> StreamApp:
    config = app.config

    services_from_config = [service_config_to_api(cast(ServiceConfig, svc))
                            for svc in config.services]

    pools: list[Pool] = []
    for pool_name in app._task_pools:
        pool_cfg = config.get_pool_by_name(pool_name)
        if pool_cfg is not None:
            pools.append(Pool(name=pool_name, executorsCount=pool_cfg.executors_count))
    for pool_name in app._priority_task_pools:
        pool_cfg = config.get_pool_by_name(pool_name)
        if pool_cfg is not None:
            pools.append(Pool(name=pool_name, executorsCount=pool_cfg.executors_count))

    registered_streams: set[int] = set(app._streams.keys())

    streams: list[ApiStream] = []
    for service_stream in app._streams.values():
        s = stream_config_to_api(cast(StreamConfig, service_stream.config))
        streams.append(s)

    data_connectors = []
    endpoints = []
    for ds in app._dataSources.values():
        data_connectors.append(data_connector_config_to_api(ds.data_connector))
        for ep in ds.endpoints:
            ep_api = endpoint_config_to_api(ep.config)
            endpoints.append(ep_api)

    for ds in app._dataSinks.values():
        data_connectors.append(data_connector_config_to_api(ds.data_connector))
        for ep in ds.endpoints:
            ep_api = endpoint_config_to_api(ep.config)
            endpoints.append(ep_api)

    default_cs = CallSemantics.Inherited
    svc_cfg = app.service_config
    if svc_cfg is not None and svc_cfg.default_call_semantics is not None:
        default_cs = svc_cfg.default_call_semantics

    links: list[Link] = []
    for li in app._runtime_links:
        if li.from_id not in registered_streams or li.to_id not in registered_streams:
            continue
        cs = li.call_semantics
        if cs == CallSemantics.Inherited or cs == default_cs:
            continue
        link = Link(
            var_from=li.from_id,
            to=li.to_id,
            callSemantics=cs,
        )
        links.append(link)

    types = list(config.types)
    modules = config.modules

    return StreamApp(
        settings=config.settings,
        services=services_from_config,
        pools=pools,
        streams=streams,
        types=list(config.types),  # type: ignore[arg-type]
        modules=config.modules,
        dataConnectors=data_connectors,
        endpoints=endpoints,
        links=links,
    )
