from framequery._pandas_util import *

import pandas as pd
import pandas.util.testing as pdt

import pytest


def test_ensure_table_columns():
    actual = ensure_table_columns("my_table", pd.DataFrame({
        "a": [1],
        "b": [2],
    }, index=[4]))

    expected = pd.DataFrame({
        ("my_table", "a"): [1],
        ("my_table", "b"): [2],
    }, index=[4])

    pdt.assert_frame_equal(actual, expected)


def test_strip_table_name_from_columns():
    actual = strip_table_name_from_columns(pd.DataFrame({
        ("my_table", "a"): [1],
        ("my_table", "b"): [2],
    }, index=[4]))

    expected = pd.DataFrame({
        "a": [1],
        "b": [2],
    }, index=[4])

    pdt.assert_frame_equal(actual, expected)


def test_cross_join():
    df1 = pd.DataFrame({
        ('$0', 'a'): [1, 2, 3],
    })

    df2 = pd.DataFrame({
        ('$1', 'b'): [4, 5, 6],
    })

    actual = (
        cross_join(df1, df2)
        .sort_values([('$0', 'a'), ('$1', 'b')])
        .reset_index(drop=True)
    )
    expected = pd.DataFrame({
        ('$0', 'a'): [1, 1, 1, 2, 2, 2, 3, 3, 3],
        ('$1', 'b'): [4, 5, 6, 4, 5, 6, 4, 5, 6],
    })

    pdt.assert_frame_equal(actual, expected)


def test_generalized_merge():
    left = pd.DataFrame({
        '$0.a': [1, 2, 1, 2],
        '$0.b': [1, 2, 3, 4]
    })

    right = pd.DataFrame({
        '$1.c': [3, 2, 3, 2],
        '$1.d': [1, 2, 3, 4]
    })

    def expected(how):
        return df_sorted(
            pd.merge(
                left, right,
                how=how, left_on='$0.a', right_on='$1.c'
            )
        )

    def actual(how):
        return df_sorted(
            general_merge(
                left, right, how, lambda df: df['$0.a'] == df['$1.c']
            )
        )

    pdt.assert_frame_equal(actual('inner'), expected('inner'))
    pdt.assert_frame_equal(actual('left'), expected('left'))
    pdt.assert_frame_equal(actual('right'), expected('right'))
    pdt.assert_frame_equal(actual('outer'), expected('outer'))


def df_sorted(df):
    return df.sort_values(list(df.columns)).reset_index(drop=True)


def test_flatten_join_condition():
    from framequery._parser import BinaryExpression, ColumnReference

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
