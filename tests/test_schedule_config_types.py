from pyservicelib_gorundebug.api.models.data_connector_implementation import (
    DataConnectorImplementation,
)
from pyservicelib_gorundebug.api.models.schedule_missed_run_policy import (
    ScheduleMissedRunPolicy,
)
from pyservicelib_gorundebug.api.models.schedule_overlap_policy import (
    ScheduleOverlapPolicy,
)
from pyservicelib_gorundebug.api.models.temporal_execution_type import (
    TemporalExecutionType,
)
from pyservicelib_gorundebug.runtime.config import (
    CronDataConnectorConfig,
    CronEndpointConfig,
    TemporalDataConnectorConfig,
    TemporalEndpointConfig,
)


def test_cron_config_preserves_portable_schedule_contract() -> None:
    implementation = DataConnectorImplementation.PythonAPScheduler
    connector = CronDataConnectorConfig(1, "localCron", implementation)
    endpoint = CronEndpointConfig(
        2,
        "reconcile",
        connector.id,
        enabled=True,
        schedule="*/5 * * * *",
        timezone="UTC",
        overlap_policy=ScheduleOverlapPolicy.ALLOW,
        missed_run_policy=ScheduleMissedRunPolicy.FIREONCE,
    )

    assert connector.to_dict() == {
        "id": 1,
        "name": "localCron",
        "type": 5,
        "implementation": implementation.value,
    }
    assert endpoint.to_dict() == {
        "id": 2,
        "name": "reconcile",
        "idDataConnector": 1,
        "enabled": True,
        "schedule": "*/5 * * * *",
        "timezone": "UTC",
        "overlapPolicy": "Allow",
        "missedRunPolicy": "FireOnce",
    }


def test_schedule_config_rejects_non_utc_timezone() -> None:
    try:
        CronEndpointConfig(1, "tick", 2, schedule="* * * * *", timezone="Europe/Moscow")
    except ValueError as error:
        assert "timezone must be UTC" in str(error)
    else:
        raise AssertionError("non-UTC timezone was accepted")


def test_temporal_config_preserves_connection_and_job_contract() -> None:
    implementation = DataConnectorImplementation.TemporalPython
    connector = TemporalDataConnectorConfig(
        3,
        "temporal",
        implementation,
        address="temporal:7233",
        namespace="default",
        max_concurrent_activities=8,
        max_concurrent_workflows=4,
    )
    endpoint = TemporalEndpointConfig(
        4,
        "durableReconcile",
        connector.id,
        TemporalExecutionType.ACTIVITY,
        enabled=True,
        task_queue="reconcile",
        activity_start_to_close_timeout=30_000,
        maximum_attempts=3,
    )

    assert connector.address == "temporal:7233"
    assert connector.namespace == "default"
    assert connector.max_concurrent_activities == 8
    assert connector.max_concurrent_workflows == 4
    assert endpoint.task_queue == "reconcile"
    assert endpoint.temporal_execution_type is TemporalExecutionType.ACTIVITY
    assert endpoint.activity_start_to_close_timeout == 30_000
    assert endpoint.maximum_attempts == 3


def test_temporal_config_rejects_non_operational_values() -> None:
    implementation = DataConnectorImplementation.TemporalPython

    try:
        TemporalDataConnectorConfig(
            1,
            "temporal",
            implementation,
            address="",
            namespace="default",
        )
    except ValueError as error:
        assert "address" in str(error)
    else:
        raise AssertionError("empty Temporal address must be rejected")

    try:
        TemporalEndpointConfig(
            2,
            "job",
            1,
            TemporalExecutionType.ACTIVITY,
            task_queue="jobs",
            activity_start_to_close_timeout=0,
        )
    except ValueError as error:
        assert "start-to-close" in str(error)
    else:
        raise AssertionError("missing activity timeout must be rejected")


def test_temporal_workflow_config_does_not_require_activity_timeout() -> None:
    endpoint = TemporalEndpointConfig(
        2,
        "workflow",
        1,
        TemporalExecutionType.WORKFLOW,
        task_queue="workflows",
        maximum_attempts=1,
    )
    assert endpoint.activity_start_to_close_timeout == 0
