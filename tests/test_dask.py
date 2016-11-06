from __future__ import print_function, division, absolute_import

import dask
import dask.dataframe as dd
import pandas as pd
import pandas.util.testing as pdt

from framequery._dask import combine_series, DaskExecutor
from framequery._context import Context

import pytest


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
        _context().select('SELECT g, SUM(b) a FROM my_table GROUP BY g')


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

    assert_eq(dask_impl(df), pandas_impl(df))


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

    assert_eq(dask_impl(df), pandas_impl(df))


def assert_eq(a, b):
    if hasattr(a, 'dask'):
        a = a.compute()

    a = a.sort_values(list(a.columns))
    a = a.reset_index(drop=True)

    if hasattr(b, 'dask'):
        b = a.compute()

    b = b.sort_values(list(b.columns))
    b = b.reset_index(drop=True)

    pdt.assert_frame_equal(a, b)


def _context():
    return Context({
        'my_table': dd.from_pandas(pd.DataFrame({
            'a': [1, 2, 3] * 10,
            'b': [4, 5, 6] * 10,
            'c': [7, 8, 9] * 10,
            'g': [0, 0, 1] * 10,
            'one': [1, 1, 1] * 10,
        }), npartitions=10),
        'my_other_table': dd.from_pandas(pd.DataFrame({
            'h': [0, 1] * 10,
            'd': [10, 11] * 10,
        }), npartitions=10),
    }, executor_factory=DaskExecutor)


def get_raises(*args, **kwargs):
    raise ValueError()
