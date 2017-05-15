from __future__ import print_function, division, absolute_import

from framequery.parser.ast import Name, Column
from framequery.util._misc import *
import pytest

examples = [
    (42, Any, UnpackResult(True)),
    (42, Any(0), UnpackResult(True, {0: [42]})),
    ([13, 21, 42], Sequence(Any(0)), UnpackResult(True, {0: [13, 21, 42]})),
    ('foo', In('foo', 'bar', group=4), UnpackResult(True, {4: ['foo']})),
]


@pytest.mark.parametrize('obj, matcher, expected', examples)
def test_unpack_examples(obj, matcher, expected):
    actual = unpack(obj, matcher)
    assert actual == expected


def test_unpack_api_test():
    name, = unpack('foo', Any(0))
    assert name == 'foo'

    name, = unpack(Name('foo'), Name(Any(0)))
    assert name == 'foo'

    alias, = unpack(Column('hello', alias='world'), Column.any(alias=Any(0)))
    assert alias == 'world'


def test_unpack_instance_of():
    assert bool(match('foo', InstanceOf(int))) is False
