import dask.dataframe as dd
from dask.dataframe.utils import eq
import pandas as pd

from framequery._dask import combine_series, DaskExecutor
from framequery._context import Context


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


def test_simple():
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


def test_grouped():
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
