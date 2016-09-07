from pdsql.parser import *


def test_select_all():
    assert parse('SELECT * FROM foo, bar, baz') == Select(
        select_list=Asterisk(),
        from_clause=[
            TableName('foo'), TableName('bar'), TableName('baz')
        ]
    )


def test_select_column():
    assert parse('SELECT a FROM foo, bar, baz') == Select(
        select_list=[DerivedColumn(value=ColumnReference(['a']), alias=None)],
        from_clause=[
            TableName('foo'), TableName('bar'), TableName('baz')
        ]
    )


def test_select_column_alias():
    assert parse('SELECT a as b FROM foo') == Select(
        select_list=[DerivedColumn(value=ColumnReference(['a']), alias='b')],
        from_clause=[TableName('foo')],
    )


def test_select_column_addition():
    assert parse('SELECT a + b FROM foo') == Select(
        select_list=[
            DerivedColumn(
                BinaryExpression.add(
                    ColumnReference(['a']),
                    ColumnReference(['b']),
                ),
            )
        ],
        from_clause=[TableName('foo')],
    )


def test_select_column_parens():
    assert parse('SELECT (a) FROM foo, bar, baz') == Select(
        select_list=[DerivedColumn(value=ColumnReference(['a']), alias=None)],
        from_clause=[
            TableName('foo'), TableName('bar'), TableName('baz')
        ]
    )


def test_select_number():
    assert parse('SELECT 42 FROM DUAL') == Select(
        select_list=[DerivedColumn(value=Integer('42'))],
        from_clause=[TableName('DUAL')]
    )


def test_select_multiple_columns():
    assert parse('SELECT a, b, baz.d FROM foo, bar, baz') == Select(
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
    assert parse('SELECT a, b, baz.d as c FROM foo, bar, baz') == Select(
        select_list=[
            DerivedColumn(value=ColumnReference(['a']), alias=None),
            DerivedColumn(value=ColumnReference(['b']), alias=None),
            DerivedColumn(value=ColumnReference(['baz', 'd']), alias='c')
        ],
        from_clause=[
            TableName('foo'), TableName('bar'), TableName('baz')
        ]
    )


def test_select_count_all():
    assert parse('SELECT a, b, COUNT(*) FROM foo, bar, baz') == Select(
        select_list=[
            DerivedColumn(value=ColumnReference(['a'])),
            DerivedColumn(value=ColumnReference(['b'])),
            DerivedColumn(value=GeneralSetFunction('COUNT', Asterisk()))
        ],
        from_clause=[
            TableName('foo'), TableName('bar'), TableName('baz')
        ]
    )


def test_select_sum():
    assert parse('SELECT SUM(a) FROM foo') == Select(
        select_list=[
            DerivedColumn(value=GeneralSetFunction('SUM', ColumnReference(['a'])))
        ],
        from_clause=[TableName('foo')]
    )


def test_select_sum_group_by():
    assert parse('SELECT SUM(a) FROM foo GROUP BY c, d, e') == Select(
        select_list=[
            DerivedColumn(value=GeneralSetFunction('SUM', ColumnReference(['a'])))
        ],
        group_by_clause=[
            ColumnReference(['c']), ColumnReference(['d']), ColumnReference(['e'])
        ],
        from_clause=[TableName('foo')]
    )


def test_integer():
    assert Integer.parse('2') == Integer('2')


def test_arithmetic():
    assert ValueExpression.parse('2 * 3') == BinaryExpression.mul(
        Integer('2'), Integer('3')
    )

    assert ValueExpression.parse('2 * 3 + 5 + 6 * 3') == BinaryExpression.add(
        BinaryExpression.add(
            BinaryExpression.mul(Integer('2'), Integer('3')),
            Integer('5'),
        ),
        BinaryExpression.mul(Integer('6'), Integer('3')),
    )


def test_arithmetic_value_expression():
    assert ValueExpression.parse('a + b') == BinaryExpression.add(
        ColumnReference(['a']), ColumnReference(['b'])
    )


def test_count_all():
    assert CountAll.parse('COUNT(*)') == GeneralSetFunction('COUNT', Asterisk())


def test_general_set_function():
    assert GeneralSetFunction.parse('SUM(a)') == GeneralSetFunction('SUM', ColumnReference(['a']))
