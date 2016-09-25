from framequery import compile
from framequery._parser import *
from framequery import _dag, _parser
from framequery._dag_compile import split_aggregates


def _split(q):
    t = split_aggregates([ValueExpression.parse(q)])
    return t[0][0], t[1], t[2]


def _ref(*parts):
    return _parser.ColumnReference(list(parts))


def _derived(*parts, **kwargs):
    return _parser.DerivedColumn(_ref(*parts), **kwargs)


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


def test_compile_simple_select_all():
    assert compile('SELECT * FROM my_table') == _dag.GetTable('my_table')


def test_compile_simple_with_filter():
    assert compile('SELECT * FROM my_table WHERE a = 1') == _dag.Filter(
        _dag.GetTable('my_table'),
        _parser.ValueExpression.parse('a = 1'),
    )

    assert compile('SELECT * FROM my_table HAVING a = 1') == _dag.Filter(
        _dag.GetTable('my_table'),
        _parser.ValueExpression.parse('a = 1'),
    )

def test_compile_transform():
    assert compile('SELECT a as b, 2 * a FROM my_table') == _dag.Transform(
        _dag.GetTable('my_table'),
        [_parser.DerivedColumn.parse('a as b'),
         _parser.DerivedColumn.parse('2 * a')]
    )


def test_compile_transform_with_aggs():
    assert compile('SELECT SUM(a) as b FROM my_table') == _dag.Transform(
        _dag.Aggregate(
            _dag.Transform(
                _dag.GetTable('my_table'), [_derived('a', alias='$0')],
            ),
            [
                _parser.DerivedColumn(
                    _parser.GeneralSetFunction.sum(_ref('$0')),
                    alias="$1",
                )
            ]
        ),
        [_derived('$1', alias='b')],
    )


def test_compile_transform_with_multiple_aggs():
    actual = compile('SELECT SUM(a) as b, AVG(b) as c FROM my_table')
    expected = _dag.Transform(
        _dag.Aggregate(
            _dag.Transform(
                _dag.GetTable('my_table'),
                [_derived('a', alias='$0'), _derived('b', alias='$2')],
            ),
            [
                _parser.DerivedColumn(
                    _parser.GeneralSetFunction.sum(_ref('$0')),
                    alias="$1",
                ),
                _parser.DerivedColumn(
                    _parser.GeneralSetFunction.avg(_ref('$2')),
                    alias="$3",
                )
            ]
        ),
        [_derived('$1', alias='b'), _derived('$3', alias='c')],
    )

    print("actual", actual)
    print("expected", expected)
    assert actual == expected
