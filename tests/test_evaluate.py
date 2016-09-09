from pdsql.evaluate import *

import functools

import pandas as pd
import pandas.util.testing as pdt


import pytest


def make_test(func):
    @functools.wraps(func)
    def impl():
        args = func()

        actual = evaluate(args['query'], args.get('scope'))
        expected = args['expected']
        print("actual\n{!r}".format(actual))
        print("expected\n{!r}".format(expected))
        pdt.assert_frame_equal(actual, expected)

    return impl


@make_test
def test_evaluate_no_table():
    return dict(
        query='SELECT 42 as b FROM DUAL',
        expected=pd.DataFrame({'b': [42]}),
    )


@make_test
def test_evaluate_column():
    return dict(
        scope={
            'tbl': pd.DataFrame({
                'a': [1, 2, 3]
            })
        },
        query='SELECT a FROM tbl',
        expected=pd.DataFrame({
            'a': [1, 2, 3]
        })
    )


@make_test
def test_evaluate_simple_arithmetic():
    return dict(
        scope={
            'tbl': pd.DataFrame({
                'a': [1, 2, 3],
                'b': [4, 5, 6]
            })
        },
        query='SELECT 2 * a as a FROM tbl',
        expected=pd.DataFrame({
            'a': [2, 4, 6],
        })
    )


@make_test
def test_evaluate_simple_arithmetic_v2():
    return dict(
        scope={
            'tbl': pd.DataFrame({
                'a': [1, 2, 3],
                'b': [4, 5, 6]
            })
        },
        query='''
            SELECT
                2 * a as a, a + b as b, (a < b) AND (b > a) as c
            FROM tbl
        ''',
        expected=pd.DataFrame({
            'a': [2, 4, 6],
            'b': [5, 7, 9],
            'c': [True, True, True],
        })
    )


@make_test
def test_evaluate_aggregation():
    return dict(
        scope={
            'tbl': pd.DataFrame({
                'a': [1, 2, 3]
            })
        },
        query='''
            SELECT
                SUM(a) as s, AVG(a) as a, MIN(a) as mi, MAX(a) as ma
            FROM tbl
        ''',
        expected=pd.DataFrame({
            'a': [2.0],
            's': [6],
            'mi': [1],
            'ma': [3],
        }, columns=['s', 'a', 'mi', 'ma'])
    )


@make_test
def test_evaluate_aggregation_grouped():
    return dict(
        scope={
            'tbl': pd.DataFrame({
                'a': [1, 2, 3, 4],
                'g': [1, 0, 1, 0]
            })
        },
        query='SELECT g, SUM(a) as a FROM tbl GROUP BY g',
        expected=pd.DataFrame({
            'a': [6, 4],
            'g': [0, 1]
        }, columns=['g', 'a'])
    )


@make_test
def test_where():
    return dict(
        scope={
            'tbl': pd.DataFrame({
                'a': [1, 2, 3],
                'c': [0, 0, 0],
                'd': [1, 0, 1],
            })
        },
        query='''
            SELECT a
            FROM tbl
            WHERE c < d
        ''',
        expected=pd.DataFrame({ 'a': [1, 3] })
    )



def test_flatten_join_condition():
    from pdsql.parser import BinaryExpression, ColumnReference

    _col = lambda *parts: ColumnReference(list(parts))

    assert flatten_join_condition(
        BinaryExpression.eq(_col('a'), _col('b'))
    ) == [(['a'], ['b'])]

    assert flatten_join_condition(
        BinaryExpression.and_(
            BinaryExpression.eq(
                _col('b'), _col('a')
            ),
            BinaryExpression.and_(
                BinaryExpression.eq(_col('c'), _col('d')),
                BinaryExpression.eq(_col('f'), _col('e')),
            )
        )
    ) == [
        (['b'], ['a']),
        (['c'], ['d']),
        (['f'], ['e'])
    ]


def test_get_col_ref():
    cols = [('A', 'a'), ('B', 'b'), ('B', 'c')]

    assert get_col_ref(cols, ('A', 'a')) == ('A', 'a')
    assert get_col_ref(cols, ('B', 'c')) == ('B', 'c')
    assert get_col_ref(cols, ('a',)) == ('A', 'a')

    assert get_col_ref(cols, ('C', 'c')) is None
    assert get_col_ref(cols, ('d',)) is None


def test_as_pandas_join_condition():
    _col = lambda parts: ColumnReference(list(parts))

    _bin_expr = lambda head, *tail: (
        BinaryExpression.and_(_bin_expr(head), _bin_expr(*tail))
        if tail
        else BinaryExpression.eq(_col(head[0]), _col(head[1]))
    )

    left_cols = [('A', 'a'), ('A', 'b'), ('A', 'c')]
    right_cols = [('B', 'd'), ('B', 'e')]

    assert as_pandas_join_condition(
        left_cols, right_cols,
        _bin_expr(('a', 'd'))
    ) == (
        [('A', 'a')],
        [('B', 'd')],
    )

    assert as_pandas_join_condition(
        left_cols, right_cols,
        _bin_expr(('a', 'd'), (['B', 'e'], 'c'))
    ) == (
        [('A', 'a'), ('A', 'c')],
        [('B', 'd'), ('B', 'e')],
    )

    with pytest.raises(ValueError):
        # cannot join table to itself
        assert as_pandas_join_condition(
            left_cols, right_cols,
            _bin_expr(('a', 'b'))
        ) == (
            [('A', 'a')],
            [('B', 'b')]
        )
