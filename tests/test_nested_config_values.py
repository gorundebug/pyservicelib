from copy import deepcopy
from pyservicelib_gorundebug.runtime.config.config import replace_placeholders


def test_generated_nested_values_preserve_types_and_inputs():
    template = {'service': {'enabled': '$enabled', 'port': '$port', 'name': '$name',
                            'optional': '$optional', 'missing': '$missing'},
                'servers': ['$first'], 'literal': 'keep'}
    values = {'service': {'enabled': False, 'port': 0, 'name': '', 'optional': None},
              'servers': ['nats://broker']}
    before = deepcopy((template, values))
    assert replace_placeholders(template, values) == {
        'service': {'enabled': False, 'port': 0, 'name': '', 'optional': None, 'missing': '$missing'},
        'servers': ['nats://broker'], 'literal': 'keep'}
    assert (template, values) == before


def test_flat_values_retain_precedence_and_missing_stays_unresolved():
    assert replace_placeholders({'service': {'port': '$port'}},
                                {'port': 1234, 'service': {'port': 80}}) == {'service': {'port': 1234}}
    assert replace_placeholders({'value': '$missing'}, {'unrelated': 1}) == {'value': '$missing'}
