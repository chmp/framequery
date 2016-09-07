from pdsql.parser import *


def test_select_all():
    assert Select.parse('SELECT * FROM foo, bar, baz') == Select(
        select_list=Asterisk(),
        from_clause=[
            TableName('foo'), TableName('bar'), TableName('baz')
        ]
    )


def test_select_column():
    assert Select.parse('SELECT a FROM foo, bar, baz') == Select(
        select_list=[DerivedColumn(value=ColumnReference(['a']), alias=None)],
        from_clause=[
            TableName('foo'), TableName('bar'), TableName('baz')
        ]
    )


def test_select_multiple_columns():
    assert Select.parse('SELECT a, b, baz.d FROM foo, bar, baz') == Select(
        select_list=[
            DerivedColumn(value=ColumnReference(['a']), alias=None),
            DerivedColumn(value=ColumnReference(['b']), alias=None),
            DerivedColumn(value=ColumnReference(['baz', 'd']), alias=None)
        ],
        from_clause=[
            TableName('foo'), TableName('bar'), TableName('baz')
        ]
    )

def test_select_multiple_columns_alias():
    assert Select.parse('SELECT a, b, baz.d as c FROM foo, bar, baz') == Select(
        select_list=[
            DerivedColumn(value=ColumnReference(['a']), alias=None),
            DerivedColumn(value=ColumnReference(['b']), alias=None),
            DerivedColumn(value=ColumnReference(['baz', 'd']), alias='c')
        ],
        from_clause=[
            TableName('foo'), TableName('bar'), TableName('baz')
        ]
    )
