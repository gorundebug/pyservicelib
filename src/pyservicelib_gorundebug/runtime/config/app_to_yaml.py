#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

from typing import Any
import yaml

from ...api.models.stream_app import StreamApp
from ...api.models.transformation_type import TransformationType
from ...api.models.call_semantics import CallSemantics


def _split_words_for_key(s: str) -> list[str]:
    words: list[str] = []
    current: list[str] = []
    runes = list(s)
    n = len(runes)
    i = 0
    while i < n:
        ch = runes[i]
        if ch in (' ', '\t', '\n', '_', '-'):
            if current:
                words.append(''.join(current))
                current = []
            i += 1
            continue
        if current and ch.isupper():
            prev = current[-1]
            if not prev.isupper():
                words.append(''.join(current))
                current = []
            elif i + 1 < n and runes[i + 1].islower():
                words.append(''.join(current))
                current = []
        current.append(ch)
        i += 1
    if current:
        words.append(''.join(current))
    return words


def to_camel_case_first_lower(text: str) -> str:
    words = _split_words_for_key(text)
    if not words:
        return ''
    result = [words[0].lower()]
    for w in words[1:]:
        result.append(w[0].upper() + w[1:].lower() if len(w) > 1 else w.upper())
    return ''.join(result)


def app_to_yaml(app: StreamApp) -> bytes:
    stream_key: dict[int, str] = {}
    for s in app.streams:
        stream_key[s.id] = to_camel_case_first_lower(s.name)

    ep_key: dict[int, str] = {}
    dc_endpoints: dict[int, list] = {}
    for ep in app.endpoints:
        ep_key[ep.id] = to_camel_case_first_lower(ep.name)
        dc_endpoints.setdefault(ep.id_data_connector, []).append(ep)

    type_key: dict[str, str] = {}
    for t in app.types:
        type_key[t.name] = to_camel_case_first_lower(t.name)

    consumers: dict[int, list[int]] = {}
    for s in app.streams:
        if s.id_source != 0:
            consumers.setdefault(s.id_source, []).append(s.id)
        if s.id_sources:
            for src in s.id_sources:
                consumers.setdefault(src, []).append(s.id)

    error_stream_key: dict[int, str] = {}
    for s in app.streams:
        if s.type == TransformationType.Error and s.id_source != 0:
            error_stream_key[s.id_source] = stream_key[s.id]

    doc: dict[str, Any] = {}

    # settings
    settings_node: dict[str, Any] = {"name": app.settings.name}
    if app.settings.module_version:
        settings_node["moduleVersion"] = app.settings.module_version
    if app.settings.repo_path:
        settings_node["repoPath"] = app.settings.repo_path
    doc["settings"] = settings_node

    # modules
    if app.modules:
        modules_node: dict[str, Any] = {}
        for m in app.modules:
            m_node: dict[str, Any] = {"name": m.name, "modulePath": m.module_path}
            if m.golang_version:
                m_node["golangVersion"] = m.golang_version
            modules_node[to_camel_case_first_lower(m.name)] = m_node
        doc["modules"] = modules_node

    # pools
    if app.pools:
        pools_node: dict[str, Any] = {}
        for p in app.pools:
            pool_node: dict[str, Any] = {
                "name": p.name,
                "executorsCount": p.executors_count,
            }
            if p.queue_capacity:
                pool_node["queueCapacity"] = p.queue_capacity
            pools_node[to_camel_case_first_lower(p.name)] = pool_node
        doc["pools"] = pools_node

    # types
    if app.types:
        types_node: dict[str, Any] = {}
        for t in app.types:
            t_node: dict[str, Any] = {}
            key = to_camel_case_first_lower(t.name)
            if t.name != key:
                t_node["name"] = t.name
            if t.type is not None and str(t.type) != "":
                t_node["type"] = str(t.type)
            if t.definition_format is not None:
                t_node["definitionFormat"] = t.definition_format.name
            if t.module:
                t_node["module"] = t.module
            if t.package:
                t_node["package"] = t.package
            if t.transfer_by_value is not None:
                t_node["transferByValue"] = t.transfer_by_value
            if t.use_alias is not None:
                t_node["useAlias"] = t.use_alias
            if t.public_type is not None:
                t_node["publicType"] = t.public_type
            if t.description:
                t_node["description"] = t.description
            if t.value_type:
                t_node["valueType"] = t.value_type
            if t.key_type:
                t_node["keyType"] = t.key_type
            if t.type_definition_lang1:
                t_node["typeDefinitionLang1"] = t.type_definition_lang1
            if t.type_definition_lang2:
                t_node["typeDefinitionLang2"] = t.type_definition_lang2
            if t.type_import_lang1:
                t_node["typeImportLang1"] = t.type_import_lang1
            if t.type_import_lang2:
                t_node["typeImportLang2"] = t.type_import_lang2
            types_node[key] = t_node
        doc["types"] = types_node

    # dataConnectors with nested endpoints
    if app.data_connectors:
        dc_node: dict[str, Any] = {}
        for dc in app.data_connectors:
            dc_obj: dict[str, Any] = {
                "name": dc.name,
                "type": dc.type.name,
            }
            if dc.implementation is not None and str(dc.implementation) != "":
                dc_obj["implementation"] = str(dc.implementation)
            if dc.host:
                dc_obj["host"] = dc.host
            if dc.port:
                dc_obj["port"] = dc.port
            if dc.address:
                dc_obj["address"] = dc.address
            if dc.brokers:
                dc_obj["brokers"] = dc.brokers
            if dc.version:
                dc_obj["version"] = dc.version
            if dc.dial_timeout:
                dc_obj["dialTimeout"] = dc.dial_timeout
            if dc.use_partitioner:
                dc_obj["usePartitioner"] = dc.use_partitioner
            if dc.var_async:
                dc_obj["async"] = dc.var_async
            if dc.security_protocol:
                dc_obj["securityProtocol"] = dc.security_protocol.value
            if dc.sasl_mechanism:
                dc_obj["saslMechanism"] = dc.sasl_mechanism.value
            if dc.username:
                dc_obj["username"] = dc.username
            if dc.password:
                dc_obj["password"] = dc.password
            if dc.use_dedicated_listener:
                dc_obj["useDedicatedListener"] = dc.use_dedicated_listener
            if dc.module:
                dc_obj["module"] = dc.module
            if dc.namespace:
                dc_obj["namespace"] = dc.namespace
            if dc.identity:
                dc_obj["identity"] = dc.identity
            if dc.api_key:
                dc_obj["apiKey"] = dc.api_key
            if dc.tls_enabled:
                dc_obj["tlsEnabled"] = dc.tls_enabled
            if dc.tls_server_name:
                dc_obj["tlsServerName"] = dc.tls_server_name
            if dc.tls_ca_file:
                dc_obj["tlsCaFile"] = dc.tls_ca_file
            if dc.tls_cert_file:
                dc_obj["tlsCertFile"] = dc.tls_cert_file
            if dc.tls_key_file:
                dc_obj["tlsKeyFile"] = dc.tls_key_file
            if dc.worker_stop_timeout:
                dc_obj["workerStopTimeout"] = dc.worker_stop_timeout
            eps = dc_endpoints.get(dc.id)
            if eps:
                eps_node: dict[str, Any] = {}
                for ep in eps:
                    ep_obj: dict[str, Any] = {"name": ep.name}
                    if ep.grpc_method_type is not None:
                        ep_obj["grpcMethodType"] = ep.grpc_method_type.name
                    if ep.http_method_type is not None:
                        ep_obj["httpMethodType"] = str(ep.http_method_type.value)
                    if ep.path:
                        ep_obj["path"] = ep.path
                    if ep.topic:
                        ep_obj["topic"] = ep.topic
                    if ep.partitions:
                        ep_obj["partitions"] = ep.partitions
                    if ep.create_topic:
                        ep_obj["createTopic"] = ep.create_topic
                    if ep.replication_factor:
                        ep_obj["replicationFactor"] = ep.replication_factor
                    if ep.consumer_group:
                        ep_obj["consumerGroup"] = ep.consumer_group
                    if ep.method_name:
                        ep_obj["methodName"] = ep.method_name
                    if ep.function_name:
                        ep_obj["functionName"] = ep.function_name
                    if ep.function_package:
                        ep_obj["functionPackage"] = ep.function_package
                    if ep.public_function:
                        ep_obj["publicFunction"] = ep.public_function
                    if ep.function_description:
                        ep_obj["functionDescription"] = ep.function_description
                    if ep.function_initializer_group:
                        ep_obj["functionInitializerGroup"] = ep.function_initializer_group
                    if ep.function_module:
                        ep_obj["functionModule"] = ep.function_module
                    if hasattr(ep, "task_queue"):
                        ep_obj["enabled"] = ep.enabled
                        ep_obj["tracingEnabled"] = ep.tracing_enabled
                        ep_obj["taskQueue"] = ep.task_queue
                        ep_obj["temporalExecutionType"] = ep.temporal_execution_type.value
                        if ep.max_concurrent_activities:
                            ep_obj["maxConcurrentActivities"] = ep.max_concurrent_activities
                        if ep.max_concurrent_workflow_tasks:
                            ep_obj["maxConcurrentWorkflowTasks"] = ep.max_concurrent_workflow_tasks
                        ep_obj["schedule"] = ep.schedule
                        ep_obj["scheduleId"] = ep.schedule_id
                        ep_obj["timezone"] = ep.timezone
                        ep_obj["overlapPolicy"] = ep.overlap_policy.value
                        ep_obj["missedRunPolicy"] = ep.missed_run_policy.value
                        ep_obj["workflowExecutionTimeout"] = ep.workflow_execution_timeout
                        ep_obj["activityStartToCloseTimeout"] = ep.activity_start_to_close_timeout
                        ep_obj["activityHeartbeatTimeout"] = ep.activity_heartbeat_timeout
                        ep_obj["maximumAttempts"] = ep.maximum_attempts
                    eps_node[ep_key[ep.id]] = ep_obj
                dc_obj["endpoints"] = eps_node
            dc_node[to_camel_case_first_lower(dc.name)] = dc_obj
        doc["dataConnectors"] = dc_node

    # services with pipelines and links
    streams_by_svc_pipeline: dict[int, dict[str, list]] = {}
    for s in app.streams:
        pipe = s.pipeline or ""
        svc_pipes = streams_by_svc_pipeline.setdefault(s.id_service, {})
        svc_pipes.setdefault(pipe, []).append(s)

    stream_svc: dict[int, int] = {s.id: s.id_service for s in app.streams}
    links_by_svc: dict[int, list] = {}
    for l in app.links:
        svc_id = stream_svc.get(l.var_from, 0)
        links_by_svc.setdefault(svc_id, []).append(l)

    svcs_node: dict[str, Any] = {}
    for svc in app.services:
        svc_obj: dict[str, Any] = {
            "name": svc.name,
            "defaultCallSemantics": svc.default_call_semantics.name,
            "programmingLanguage": svc.programming_language.name,
            "modulePath": svc.module_path,
            "httpHost": svc.http_host,
            "httpPort": svc.http_port,
            "grpcHost": svc.grpc_host,
            "grpcPort": svc.grpc_port,
            "shutdownTimeout": svc.shutdown_timeout,
            "environment": svc.environment.name if svc.environment is not None else "",
            "color": svc.color,
            "statusHandler": svc.status_handler,
            "metricsHandler": svc.metrics_handler,
            "startupHandler": svc.startup_handler,
            "readinessHandler": svc.readiness_handler,
            "livenessHandler": svc.liveness_handler,
            "kubernetesWorkloadType": svc.kubernetes_workload_type.value,
        }
        if svc.golang_version:
            svc_obj["golangVersion"] = svc.golang_version
        if svc.default_grpc_timeout:
            svc_obj["defaultGrpcTimeout"] = svc.default_grpc_timeout
        if svc.log_level is not None:
            svc_obj["logLevel"] = svc.log_level.name

        pipelines_node: dict[str, Any] = {}
        if svc.id in streams_by_svc_pipeline:
            pipes = streams_by_svc_pipeline[svc.id]
            for pipe_name in sorted(pipes.keys()):
                pipe_streams = pipes[pipe_name]
                pipe_node: dict[str, Any] = {}
                for s in pipe_streams:
                    s_node: dict[str, Any] = {
                        "type": s.type.name,
                        "name": s.name,
                        "xPos": s.x_pos,
                        "yPos": s.y_pos,
                    }
                    if s.id_source != 0:
                        s_node["source"] = stream_key.get(s.id_source, str(s.id_source))
                    if s.id_sources:
                        s_node["sources"] = [stream_key.get(src, str(src)) for src in s.id_sources]
                    if s.id_endpoint is not None:
                        s_node["endpoint"] = ep_key.get(s.id_endpoint, str(s.id_endpoint))
                    if s.value_type:
                        vt_key = type_key.get(s.value_type, s.value_type)
                        s_node["valueType"] = vt_key
                    if s.key_type:
                        kt_key = type_key.get(s.key_type, s.key_type)
                        s_node["keyType"] = kt_key
                    if s.id in error_stream_key:
                        s_node["errorStream"] = error_stream_key[s.id]
                    if s.join_type is not None:
                        s_node["joinType"] = s.join_type.name
                    if s.join_storage is not None:
                        s_node["joinStorage"] = s.join_storage.name
                    if s.pattern is not None:
                        s_node["pattern"] = s.pattern.name
                    if s.function_name:
                        s_node["functionName"] = s.function_name
                    if s.function_package:
                        s_node["functionPackage"] = s.function_package
                    if s.public_function:
                        s_node["publicFunction"] = s.public_function
                    if s.function_description:
                        s_node["functionDescription"] = s.function_description
                    if s.function_initializer_group:
                        s_node["functionInitializerGroup"] = s.function_initializer_group
                    if s.function_module:
                        s_node["functionModule"] = s.function_module
                    if s.ttl is not None:
                        s_node["ttl"] = s.ttl
                    if s.renew_ttl is not None:
                        s_node["renewTTL"] = s.renew_ttl
                    if s.duration is not None:
                        s_node["duration"] = s.duration
                    pipe_node[stream_key[s.id]] = s_node
                pipelines_node[pipe_name] = pipe_node
        svc_obj["pipelines"] = pipelines_node

        links = links_by_svc.get(svc.id)
        if links:
            links_node: dict[str, Any] = {}
            for i, l in enumerate(links):
                l_node: dict[str, Any] = {
                    "from": stream_key.get(l.var_from, str(l.var_from)),
                    "to": stream_key.get(l.to, str(l.to)),
                }
                if l.call_semantics != CallSemantics.Inherited and l.call_semantics != 0:
                    l_node["callSemantics"] = l.call_semantics.name
                if l.call_semantics == CallSemantics.FunctionCall and l.var_async:
                    l_node["async"] = True
                if l.pool_name:
                    l_node["poolName"] = l.pool_name
                if l.priority is not None:
                    l_node["priority"] = l.priority
                links_node[f"link{i + 1}"] = l_node
            svc_obj["links"] = links_node

        svcs_node[to_camel_case_first_lower(svc.name)] = svc_obj
    doc["services"] = svcs_node

    return yaml.dump(doc, allow_unicode=True, default_flow_style=False,
                     sort_keys=True).encode("utf-8")
