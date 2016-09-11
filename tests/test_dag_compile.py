from pdsql.parser import *
from pdsql._dag_compile import split_aggregate


def _split(q):
    return split_aggregate(ValueExpression.parse(q))


def test_column_reference():
    result, aggs, pre_aggs = _split('foo')

    assert aggs == []
    assert pre_aggs == []
    assert result == ColumnReference(['foo'])


def test_sum_of_column():
    result, aggs, pre_aggs = _split('SUM(foo)')

    assert result == ColumnReference(['$1'])
    assert aggs == [
        DerivedColumn(GeneralSetFunction('SUM', ColumnReference(['$0'])), alias='$1')
    ]

    assert pre_aggs == [
        DerivedColumn(ColumnReference(['foo']), alias='$0')
    ]


def test_sum_of_sums_column():
    result, aggs, pre_aggs = _split('SUM(foo) + SUM(bar)')

    assert result == BinaryExpression.add(
        ColumnReference(['$1']),
        ColumnReference(['$3']),
    )
    assert aggs == [
        DerivedColumn(GeneralSetFunction('SUM', ColumnReference(['$0'])), alias='$1'),
        DerivedColumn(GeneralSetFunction('SUM', ColumnReference(['$2'])), alias='$3')
    ]

    assert pre_aggs == [
        DerivedColumn(ColumnReference(['foo']), alias='$0'),
        DerivedColumn(ColumnReference(['bar']), alias='$2')
    ]


def test_sum_of_col_sums():
    result, aggs, pre_aggs = _split('SUM(foo) + SUM(bar + baz)')

    assert result == BinaryExpression.add(
        ColumnReference(['$1']),
        ColumnReference(['$3']),
    )
    assert aggs == [
        DerivedColumn(GeneralSetFunction('SUM', ColumnReference(['$0'])), alias='$1'),
        DerivedColumn(GeneralSetFunction('SUM', ColumnReference(['$2'])), alias='$3')
    ]

    assert pre_aggs == [
        DerivedColumn(ColumnReference(['foo']), alias='$0'),
        DerivedColumn(
            BinaryExpression.add(
                ColumnReference(['bar']),
                ColumnReference(['baz']),
            ),
            alias='$2',
        )
    ]


def test_sum_with_integer():
    result, aggs, pre_aggs = _split('SUM(foo + 2)')

    assert result == ColumnReference(['$1'])
    assert aggs == [
        DerivedColumn(GeneralSetFunction('SUM', ColumnReference(['$0'])), alias='$1')
    ]

    assert pre_aggs == [
        DerivedColumn(
            BinaryExpression.add(
                ColumnReference(['foo']),
                Integer('2'),
            ),
            alias='$0',
        )
    ]
