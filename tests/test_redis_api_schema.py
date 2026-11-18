"""Schema availability does not imply a Redis runtime adapter is installed."""
import pytest
from pydantic import ValidationError
from pyservicelib_gorundebug.api.models.data_connector import DataConnector
from pyservicelib_gorundebug.api.models.endpoint import Endpoint
from pyservicelib_gorundebug.api.models.data_connector_type import DataConnectorType


def test_redis_api_roundtrip():
    connector = DataConnector(id=1, name='Redis', type=DataConnectorType.RedisStreams,
                              servers=['redis://broker:6379'], redisDatabase=0)
    endpoint = Endpoint(functionName='Handle', id=2, idDataConnector=1, name='Read', streamKey='events',
                        consumerGroup='workers', createConsumerGroup=False,
                        groupStartId='0-0', readCount=64, readBlockMs=1000,
                        claimIdleMs=60000, claimIntervalMs=1000)
    assert connector.model_dump(by_alias=True)['redisDatabase'] == 0
    restored = Endpoint.model_validate_json(endpoint.model_dump_json(by_alias=True))
    assert restored == endpoint
    assert restored.create_consumer_group is False
    assert restored.stream_key == 'events'
    omitted = Endpoint(functionName='Handle', id=3, idDataConnector=1, name='Draft')
    assert 'create_consumer_group' not in omitted.model_fields_set
    assert 'create_consumer_group' in restored.model_fields_set


@pytest.mark.parametrize('field,value', [('readCount',0),('readBlockMs',60001),
    ('claimIdleMs',2147483648),('claimIntervalMs',False),('createConsumerGroup',1)])
def test_redis_api_rejects_invalid_boundaries(field,value):
    with pytest.raises(ValidationError):
        Endpoint(functionName='Handle', id=2,idDataConnector=1,name='Read',**{field:value})
