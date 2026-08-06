#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Optional, cast
from ...api.models.stream_app import StreamApp
from ...api.models.service import Service
from ...api.models.stream import Stream
from ...api.models.data_connector import DataConnector
from ...api.models.data_connector_implementation import DataConnectorImplementation
from ...api.models.data_connector_type import DataConnectorType
from ...api.models.endpoint import Endpoint
from ...api.models.link import Link
from ...api.models.type import Type
from ...api.models.pool import Pool
from ...api.models.call_semantics import CallSemantics
from ...api.models.programming_language import ProgrammingLanguage
from .config import ServiceAppConfig, ServiceConfig, StreamConfig, DataConnectorConfig
from .config import EndpointConfig, LinkConfig, TypeConfig, PoolConfig


def config_to_stream_app(config: ServiceAppConfig) -> StreamApp:
    return StreamApp(
        settings=config.settings,
        services=[service_config_to_api(cast(ServiceConfig, s)) for s in config.services],
        streams=[stream_config_to_api(cast(StreamConfig, s)) for s in config.streams],
        links=[link_config_to_api(cast(LinkConfig, l)) for l in config.links],
        types=[type_config_to_api(cast(TypeConfig, t)) for t in config.types],
        dataConnectors=[data_connector_config_to_api(cast(DataConnectorConfig, dc))
                        for dc in config.data_connectors],
        endpoints=[endpoint_config_to_api(cast(EndpointConfig, ep)) for ep in config.endpoints],
        pools=[pool_config_to_api(cast(PoolConfig, p)) for p in config.pools],
        modules=config.modules,
    )


def service_config_to_api(s: ServiceConfig) -> Service:
    return Service(
        id=s.id,
        name=s.name,
        programmingLanguage=ProgrammingLanguage.Python,
        modulePath=s.module_path,
        golangVersion=s.golang_version,
        defaultCallSemantics=s.default_call_semantics if s.default_call_semantics is not None else CallSemantics.Inherited,
        httpHost=s.http_host,
        httpPort=s.http_port,
        statusHandler=s.status_handler,
        metricsHandler=s.metrics_handler,
        grpcHost=s.grpc_host,
        grpcPort=s.grpc_port,
        defaultGrpcTimeout=s.default_grpc_timeout,
        color=s.color,
        shutdownTimeout=s.shutdown_timeout,
        logLevel=s.log_level,
        environment=s.environment,
    )


def stream_config_to_api(sc: StreamConfig) -> Stream:
    return Stream(
        id=sc.id,
        name=sc.name,
        idSource=sc.id_source,
        idSources=sc.id_sources,
        type=sc.type,
        valueType=sc.value_type,
        keyType=sc.key_type,
        idService=sc.id_service,
        idEndpoint=sc.id_endpoint,
        joinType=sc.join_type,
        joinStorage=sc.join_storage,
        pattern=sc.pattern,
        xPos=sc.x_pos,
        yPos=sc.y_pos,
        functionName=sc.function_name,
        functionPackage=sc.function_package,
        publicFunction=sc.public_function,
        functionDescription=sc.function_description,
        functionInitializerGroup=sc.function_initializer_group,
        functionModule=sc.function_module,
        ttl=sc.ttl,
        renewTTL=sc.renew_ttl,
        duration=sc.duration,
        pipeline=sc.pipeline,
    )


def data_connector_config_to_api(dc: DataConnectorConfig) -> DataConnector:
    impl = dc.implementation
    if impl is None:
        impl_val = DataConnectorImplementation.Undefined
    else:
        impl_val = DataConnectorImplementation(impl)
    return DataConnector(
        id=dc.id,
        name=dc.name,
        type=dc.type,
        implementation=impl_val,
        host=dc.host or None,
        port=dc.port or None,
        address=dc.address or None,
        connectionsCount=(dc.connections_count or 1)
        if dc.type == DataConnectorType.gRPC else None,
        brokers=dc.brokers or None,
        version=dc.version or None,
        dialTimeout=dc.dial_timeout or None,
        usePartitioner=dc.use_partitioner or None,
        **{"async": dc.var_async or None},
        useDedicatedListener=dc.use_dedicated_listener or None,
        module=dc.module or None,
    )


def endpoint_config_to_api(ep: EndpointConfig) -> Endpoint:
    return Endpoint(
        id=ep.id,
        name=ep.name,
        idDataConnector=ep.id_data_connector,
        httpMethodType=ep.http_method_type,
        grpcMethodType=ep.grpc_method_type,
        path=ep.path or None,
        topic=ep.topic or None,
        consumerGroup=ep.consumer_group or None,
        createTopic=ep.create_topic or None,
        partitions=ep.partitions or None,
        replicationFactor=ep.replication_factor or None,
        methodName=ep.method_name or None,
        functionName=ep.function_name or None,
        functionPackage=ep.function_package or None,
        publicFunction=ep.public_function or None,
        functionDescription=ep.function_description or None,
        functionInitializerGroup=ep.function_initializer_group or None,
        functionModule=ep.function_module or None,
    )


def link_config_to_api(l: LinkConfig) -> Link:
    cs = l.call_semantics if l.call_semantics is not None else CallSemantics.Inherited
    return Link.model_validate({
        "from": l.var_from,
        "to": l.to,
        "callSemantics": cs,
        "async": l.var_async,
        "poolName": l.pool_name,
        "priority": l.priority,
    })


def type_config_to_api(t: TypeConfig) -> Type:
    return Type(
        name=t.name,
        type=t.type,
        typeDefinitionLang1=t.type_definition_lang1 or None,
        typeDefinitionLang2=t.type_definition_lang2 or None,
        typeImportLang1=t.type_import_lang1 or None,
        typeImportLang2=t.type_import_lang2 or None,
        valueType=t.value_type or None,
        keyType=t.key_type or None,
        package=t.package or None,
        module=t.module or None,
        definitionFormat=t.definition_format,
        publicType=t.public_type or None,
        transferByValue=t.transfer_by_value or None,
        useAlias=t.use_alias or None,
    )


def pool_config_to_api(p: PoolConfig) -> Pool:
    return Pool(
        name=p.name,
        executorsCount=p.executors_count,
        queueCapacity=p.queue_capacity or None,
    )
