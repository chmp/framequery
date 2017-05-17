from __future__ import print_function, division, absolute_import

from framequery.executor._util import (
    column_from_parts, column_get_column, column_get_table, split_quoted_name,
)


def test_examples():
    assert column_get_table(column_from_parts('foo', 'bar')) == 'foo'
    assert column_get_column(column_from_parts('foo', 'bar')) == 'bar'

    assert column_get_table(column_from_parts(None, 'bar')) is None
    assert column_get_column(column_from_parts(None, 'bar')) is 'bar'


def test_split_quoted_name():
    assert split_quoted_name('foo') == ['foo']
    assert split_quoted_name('foo.bar') == ['foo', 'bar']
    assert split_quoted_name('foo."bar baz"') == ['foo', 'bar baz']

    assert split_quoted_name('"foo..."."bar baz"') == ['foo...', 'bar baz']
