from __future__ import print_function, division, absolute_import

import dask
import dask.dataframe as dd
from dask.dataframe.utils import eq
import pandas as pd

from framequery._dask import combine_series, DaskExecutor
from framequery._context import Context

import pytest


def test_select_column():
    assert eq(
        _context().select('SELECT a as a FROM my_table'),
        pd.DataFrame({
            '$0.a': [1, 2, 3]
        }),
    )


def test_simple_select_distinct():
    assert eq(
        _context()
        .select('SELECT DISTINCT g, one FROM my_table')
        .compute()
        .reset_index(drop=True),
        pd.DataFrame({
            '$0.g': [0, 1],
            '$0.one': [1, 1],
        }),
    )


def test_simple_arithmetic():
    assert eq(
        _context().select('SELECT 2 * a as a FROM my_table'),
        pd.DataFrame({
            '$0.a': [2, 4, 6],
        }),
    )


def test_simple_arithmetic_v2():
    assert eq(
        _context().select('''
            SELECT
                2 * a as a, a + b as b, (a < b) AND (b > a) as c
            FROM my_table
        '''),
        pd.DataFrame({
            '$0.a': [2, 4, 6],
            '$0.b': [5, 7, 9],
            '$0.c': [True, True, True],
        }),
    )


def test_simple_arithmetic_v3():
    assert eq(
        _context().select('SELECT - a + + b as a FROM my_table'),
        pd.DataFrame({
            '$0.a': [4 - 1, 5 - 2, 6 - 3]
        }),
    )


def test_simple_arithmetic_function_calls():
    assert eq(
        _context().select('SELECT ABS(a - 4 * g) as a FROM my_table'),
        pd.DataFrame({
            '$0.a': [1, 2, 1],
        }),
    )


def test_evaluate_aggregation():
    # TODO: add first_value to test
    assert eq(
        _context().select('''
            SELECT
                SUM(a) as s, AVG(a) as a, MIN(a) as mi, MAX(a) as ma
            FROM my_table
        '''),
        pd.DataFrame({
            '$2.a': [2.0],
            '$2.s': [6],
            '$2.mi': [1],
            '$2.ma': [3],
        })[['$2.s', '$2.a', '$2.mi', '$2.ma']],
    )


def test_simple_subquery():
    assert eq(
        _context().select('SELECT * FROM (SELECT * FROM my_table)'),
        pd.DataFrame({
            'my_table.a': [1, 2, 3],
            'my_table.b': [4, 5, 6],
            'my_table.c': [7, 8, 9],
            'my_table.g': [0, 0, 1],
            'my_table.one': [1, 1, 1],
        }),
    )


def test_simple_filter():
    assert eq(
        _context().select('SELECT * FROM my_table WHERE g = 0'),
        pd.DataFrame({
            'my_table.a': [1, 2],
            'my_table.b': [4, 5],
            'my_table.c': [7, 8],
            'my_table.g': [0, 0],
            'my_table.one': [1, 1],
        }),
    )


def test_simple_sum_cte():
    assert eq(
        _context().select('''
            WITH
                foo AS (
                    SELECT
                        a + b as a,
                        c + g as b
                    FROM my_table
                ),
                bar AS (
                    SELECT a + b as c
                    FROM foo
                )

            SELECT sum(c) as d FROM bar
        '''),
        pd.DataFrame({
            '$4.d': [1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 1],
        }),
    )


def test_evaluate_join():
    expected = pd.DataFrame({
        '$0.a': [1, 2, 3],
        '$0.d': [10, 10, 11],
    })

    def _compare(q):
        assert eq(_context().select(q), expected)

    _compare('SELECT a, d FROM my_table JOIN my_other_table ON g = h')
    _compare('SELECT a, d FROM my_table INNER JOIN my_other_table ON g = h')

    # TODO: add proper tests for outer joins
    _compare('SELECT a, d FROM my_table LEFT JOIN my_other_table ON g = h')
    _compare('SELECT a, d FROM my_table LEFT OUTER JOIN my_other_table ON g = h')

    _compare('SELECT a, d FROM my_table RIGHT JOIN my_other_table ON g = h')
    _compare('SELECT a, d FROM my_table RIGHT OUTER JOIN my_other_table ON g = h')


def test_evaluate_aggregation_grouped():
    actual = _context().select('SELECT g, SUM(b) as a FROM my_table GROUP BY g')
    expected = pd.DataFrame({
        '$2.g': [0, 1],
        '$2.a': [9, 6],
    }, columns=['$2.g', '$2.a'])

    assert eq(drop_index(actual), drop_index(expected))


def test_evaluate_aggregation_grouped_no_as():
    actual = _context().select('SELECT g, SUM(b) a FROM my_table GROUP BY g')

    expected = pd.DataFrame({
        '$2.g': [0, 1],
        '$2.a': [9, 6],
    }, columns=['$2.g', '$2.a'])

    assert eq(drop_index(actual), drop_index(expected))


def test_unsuported():
    with pytest.raises(NotImplementedError):
        _context().select('SELECT * FROM my_table ORDER BY a')

    with pytest.raises(NotImplementedError):
        _context().select('SELECT * FROM my_table LIMIT 1, 2')


def test_no_compute():
    # test that no operation triggers a compute
    with dask.set_options(get=get_raises):
        _context().select('SELECT * FROM my_table WHERE g = 0')
        _context().select('SELECT SUM(a) as s, AVG(a) as a, MIN(a) as mi, MAX(a) as ma FROM my_table')
        _context().select('''
            WITH
                foo AS (
                    SELECT
                        a + b as a,
                        c + g as b
                    FROM my_table
                ),
                bar AS (
                    SELECT a + b as c
                    FROM foo
                )

            SELECT sum(c) as d FROM bar
        ''')
        _context().select('''
            SELECT
                2 * a as a, a + b as b, (a < b) AND (b > a) as c
            FROM my_table
        ''')


def test_combine_series__simple():
    df = pd.DataFrame({
        'a': [1, 2, 3],
        'b': [4, 5, 6],
        'c': [7, 8, 9],
        'g': [1, 0, 1],
    })

    def dask_impl(df):
        df = dd.from_pandas(df, npartitions=2)

        return combine_series([
            ('$0', df['a'] + df['b']),
            ('$1', df['a'] * df['b']),
        ])

    def pandas_impl(df):
        return pd.DataFrame({
            '$0': df['a'] + df['b'],
            '$1': df['a'] * df['b'],
        })

    assert eq(dask_impl(df), pandas_impl(df))


def test_combine_series__grouped():
    df = pd.DataFrame({
        'a': [1, 2, 3],
        'b': [4, 5, 6],
        'c': [7, 8, 9],
        'g': [1, 0, 1],
    })

    def dask_impl(df):
        df = dd.from_pandas(df, npartitions=2)
        grouped = df.groupby('g')
        return (
            combine_series([
                ('$0', grouped['a'].sum()),
                ('$1', grouped['a'].max()),
            ])
            .reset_index()
            .compute()
        )

    def pandas_impl(df):
        grouped = df.groupby('g')

        return (
            pd.DataFrame({
                '$0': grouped['a'].sum(),
                '$1': grouped['a'].max()
            })
            .reset_index()
        )

    assert eq(dask_impl(df), pandas_impl(df))


def _context():
    return Context({
        'my_table': dd.from_pandas(pd.DataFrame({
            'a': [1, 2, 3],
            'b': [4, 5, 6],
            'c': [7, 8, 9],
            'g': [0, 0, 1],
            'one': [1, 1, 1],
        }), npartitions=2),
        'my_other_table': dd.from_pandas(pd.DataFrame({
            'h': [0, 1],
            'd': [10, 11],
        }), npartitions=2),
    }, executor_factory=DaskExecutor)


def get_raises(*args, **kwargs):
    raise ValueError()


def drop_index(df):
    if hasattr(df, 'dask'):
        df = df.compute()

    return df.sort_values(list(df.columns)).reset_index(drop=True)
