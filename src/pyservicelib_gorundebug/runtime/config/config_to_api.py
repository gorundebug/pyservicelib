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
        startupHandler=s.startup_handler,
        readinessHandler=s.readiness_handler,
        livenessHandler=s.liveness_handler,
        kubernetesWorkloadType=s.kubernetes_workload_type,
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
        component=sc.component,
    )


def data_connector_config_to_api(dc: DataConnectorConfig) -> DataConnector:
    bindings = dict(dc.implementations) if dc.implementations is not None else None
    implementation = None
    if dc.type == DataConnectorType.Custom:
        implementation = dc.implementation
    elif dc.implementation:
        if bindings is None:
            bindings = {}
        bindings["python"] = getattr(dc.implementation, "value", dc.implementation)
    return DataConnector.model_validate({
        "id": dc.id,
        "name": dc.name,
        "type": dc.type,
        "implementation": implementation,
        "implementations": bindings,
        "host": dc.host or None,
        "port": dc.port or None,
        "address": dc.address or None,
        "connectionsCount": (dc.connections_count or 1)
        if dc.type == DataConnectorType.gRPC else None,
        "brokers": dc.brokers or None,
        "adminUrl": dc.admin_url if dc.type == DataConnectorType.Pulsar else None,
        "servers": list(dc.servers) if dc.servers is not None else None,
        "virtualHost": dc.virtual_host if dc.type not in (DataConnectorType.RedisStreams, DataConnectorType.Pulsar) else None,
        "redisDatabase": dc.redis_database if dc.type == DataConnectorType.RedisStreams else None,
        "connectTimeoutMs": (dc.connect_timeout_ms or 2000) if dc.type == DataConnectorType.Pulsar else dc.connect_timeout_ms if dc.type.value in (7, 8, 9, 10) else None,
        "operationTimeoutMs": (dc.operation_timeout_ms or 5000) if dc.type == DataConnectorType.Pulsar else dc.operation_timeout_ms if dc.type.value in (7, 8, 9, 10) else None,
        "token": dc.token if dc.type != DataConnectorType.Pulsar else None,
        "credentialsFile": dc.credentials_file,
        "version": dc.version or None,
        "dialTimeout": dc.dial_timeout or None,
        "usePartitioner": dc.use_partitioner or None,
        "async": dc.var_async or None,
        "securityProtocol": dc.security_protocol or None,
        "saslMechanism": dc.sasl_mechanism or None,
        "username": dc.username or None,
        "password": dc.password or None,
        "useDedicatedListener": dc.use_dedicated_listener or None,
        "module": dc.module or None,
        "namespace": dc.namespace or None,
        "identity": dc.identity or None,
        "apiKey": dc.api_key or None,
        "tlsEnabled": dc.tls_enabled if dc.type.value in (7, 8, 9, 10, 11) else dc.tls_enabled or None,
        "tlsServerName": dc.tls_server_name or None,
        "tlsCaFile": dc.tls_ca_file or None,
        "tlsCertFile": dc.tls_cert_file or None,
        "tlsKeyFile": dc.tls_key_file or None,
        "workerStopTimeout": dc.worker_stop_timeout or None,
    })


def endpoint_config_to_api(ep: EndpointConfig) -> Endpoint:
    return Endpoint(
        id=ep.id,
        name=ep.name,
        idDataConnector=ep.id_data_connector,
        httpMethodType=ep.http_method_type,
        grpcMethodType=ep.grpc_method_type,
        path=ep.path or None,
        topic=ep.topic or None,
        queueName=ep.queue_name,
        exchangeName=ep.exchange_name,
        routingKey=ep.routing_key,
        queueType=ep.queue_type,
        exchangeType=ep.exchange_type,
        createQueue=ep.create_queue if "create_queue" in ep.model_fields_set else None,
        createExchange=ep.create_exchange if "create_exchange" in ep.model_fields_set else None,
        createBinding=ep.create_binding if "create_binding" in ep.model_fields_set else None,
        consumerCredits=ep.consumer_credits if 'consumer_credits' in ep.model_fields_set else None,
        publisherMaxInFlight=ep.publisher_max_in_flight if 'publisher_max_in_flight' in ep.model_fields_set else None,
        streamKey=ep.stream_key,
        consumerName=ep.consumer_name,
        createConsumerGroup=ep.create_consumer_group if "create_consumer_group" in ep.model_fields_set else None,
        groupStartId=ep.group_start_id if "group_start_id" in ep.model_fields_set else None,
        readCount=ep.read_count if "read_count" in ep.model_fields_set else None,
        readBlockMs=ep.read_block_ms if "read_block_ms" in ep.model_fields_set else None,
        claimIdleMs=ep.claim_idle_ms if "claim_idle_ms" in ep.model_fields_set else None,
        claimIntervalMs=ep.claim_interval_ms if "claim_interval_ms" in ep.model_fields_set else None,
        subject=ep.subject,
        queueGroup=ep.queue_group,
        streamName=ep.stream_name,
        durableName=ep.durable_name,
        createStream=ep.create_stream if ep.subject or (ep.stream_key and "create_stream" in ep.model_fields_set) else None,
        createConsumer=ep.create_consumer if ep.subject else None,
        consumerGroup=ep.consumer_group or None,
        subscriptionName=ep.subscription_name,
        subscriptionType=ep.subscription_type,
        createSubscription=ep.create_subscription if 'create_subscription' in ep.model_fields_set else None,
        subscriptionInitialPosition=ep.subscription_initial_position,
        receiverQueueSize=ep.receiver_queue_size,
        negativeAckRedeliveryDelayMs=ep.negative_ack_redelivery_delay_ms,
        createTopic=ep.create_topic if 'create_topic' in ep.model_fields_set else None,
        partitions=ep.partitions or None,
        replicationFactor=ep.replication_factor or None,
        methodName=ep.method_name or None,
            functionName=ep.function_name,
        functionPackage=ep.function_package or None,
        publicFunction=ep.public_function or None,
        functionDescription=ep.function_description or None,
        functionInitializerGroup=ep.function_initializer_group or None,
        functionModule=ep.function_module or None,
        enabled=ep.enabled if "enabled" in ep.model_fields_set or ep.queue_name or ep.exchange_name or ep.subject or ep.stream_key else getattr(ep, "enabled", False) or None,
        tracingEnabled=ep.tracing_enabled if "tracing_enabled" in ep.model_fields_set or ep.queue_name or ep.exchange_name or ep.subject or ep.stream_key else ep.tracing_enabled or None,
        taskQueue=getattr(ep, "task_queue", "") or None,
        temporalExecutionType=getattr(ep, "temporal_execution_type", None),
        maxConcurrentActivities=getattr(ep, "max_concurrent_activities", 0) or None,
        maxConcurrentWorkflowTasks=getattr(ep, "max_concurrent_workflow_tasks", 0) or None,
        schedule=getattr(ep, "schedule", "") or None,
        scheduleId=getattr(ep, "schedule_id", "") or None,
        timezone=getattr(ep, "timezone", "") or None,
        overlapPolicy=getattr(ep, "overlap_policy", None),
        missedRunPolicy=getattr(ep, "missed_run_policy", None),
        workflowExecutionTimeout=getattr(ep, "workflow_execution_timeout", 0) or None,
        activityStartToCloseTimeout=getattr(ep, "activity_start_to_close_timeout", 0) or None,
        activityHeartbeatTimeout=getattr(ep, "activity_heartbeat_timeout", 0) or None,
        maximumAttempts=getattr(ep, "maximum_attempts", 0) or None,
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
        bindings=t.bindings,
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
