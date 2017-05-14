from __future__ import print_function, division, absolute_import

from framequery.executor._util import column_from_parts, column_get_column, column_get_table


def test_examples():
    assert column_get_table(column_from_parts('foo', 'bar')) == 'foo'
    assert column_get_column(column_from_parts('foo', 'bar')) == 'bar'

    assert column_get_table(column_from_parts(None, 'bar')) is None
    assert column_get_column(column_from_parts(None, 'bar')) is 'bar'
