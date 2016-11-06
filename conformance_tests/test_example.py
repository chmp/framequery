import sqlite3

from framequery._util import version
from util import Table, Type, ColumnWithValues, ConformanceEnv

import pytest


configs = dict(
    dask=dict(executor='dask', strict=False),
    pandas=dict(executor='pandas', strict=False),
    pandas_strict=dict(executor='pandas', strict=True),
)


@pytest.fixture(params=sorted(configs))
def env(request):
    config = configs[request.param]
    env = ConformanceEnv(**config)
    env.add_tables(
        Table('my_table', [
            ColumnWithValues('a', Type.integer, [1, 2, 3, None]),
            ColumnWithValues('b', Type.integer, [1, 2, 3, None]),
            ColumnWithValues('c', Type.integer, [1, 2, 3, None]),
        ]),
        Table('my_other_table', [
            ColumnWithValues('d', Type.integer, [-1, 2, 3, None]),
            ColumnWithValues('e', Type.integer, [-1, 2, 3, None]),
        ]),
        Table('my_string_table', [
            ColumnWithValues('g', Type.string, ['0', '1', '2', None]),
            ColumnWithValues('a', Type.string, ['foO', 'bAr', 'BAZ', None]),
            ColumnWithValues('b', Type.string, ['foO', 'bAr', 'BAZ', None]),
            ColumnWithValues('c', Type.string, ['abcdef', 'ghijkl', 'mnopqrs', None]),
        ]),
        Table('my_string_table_no_na', [
            ColumnWithValues('g', Type.string, ['0', '1', '2']),
            ColumnWithValues('a', Type.string, ['foO', 'foo', 'bAr', 'BAZ']),
            ColumnWithValues('b', Type.string, ['foO', 'foo', 'bAr', 'BAZ']),
            ColumnWithValues('c', Type.string, ['abcdef', 'ghijkl', 'mnopqrs']),
        ]),
    )
    return env


def test_example(env):
    env.test('SELECT * FROM my_table')


def test_ungrouped_aggregates(env):
    env.test('''
        SELECT
            sum(a) as sum_a,
            avg(a) as avg_a,
            count(a) as count_a,
            min(a) as min_a,
            max(a) as max_a

        FROM my_table
    ''')


@pytest.mark.skipif(
    version(sqlite3.sqlite_version) <= version('3.8.2'),
    reason="sqlite version does not support CTEs",
)
def test_ungrouped_aggregates_cte(env):
    env.test('''
        WITH
            foo AS (
                SELECT a + b AS a FROM my_table
            ),
            bar AS (
                SELECT a * a AS a FROM foo
            )
        SELECT
            sum(a) as sum_a,
            avg(a) as avg_a,
            count(a) as count_a,
            min(a) as min_a,
            max(a) as max_a

        FROM bar
    ''')


def test_ungroupedt_aggregates_sub_query(env):
    env.test('''
        SELECT
            sum(a) as sum_a,
            avg(a) as avg_a,
            count(a) as count_a,
            min(a) as min_a,
            max(a) as max_a

        FROM (
            SELECT a * a AS a FROM (
                SELECT a + b AS a FROM my_table
            )
        )
    ''')


def test_ungroupedt_aggregates_no_as(env):
    env.test('''
        SELECT
            sum(a) sum_a,
            avg(a) avg_a,
            count(a) count_a,
            min(a) min_a,
            max(a) max_a

        FROM my_table
    ''')


def test_grouped_aggregates_simple_example(env):
    def postprocess_actual(env, df):
        return df.sort_values(list(df.columns))

    # pandas removes NULLs in groupby, SQL keeps them
    def postprocess_expected(env, df):
        if not env.strict:
            df = df[~df['b'].isnull()]

        return df.sort_values(list(df.columns))

    env.test(
        'SELECT b, SUM(b) as a FROM my_table GROUP BY b',
        postprocess_actual=postprocess_actual,
        postprocess_expected=postprocess_expected
    )


def test_grouped_aggregates(env):
    # pandas removes NULLs in groupby, SQL keeps them
    def postprocess_expected(env, df):
        if not env.strict:
            df = df[~df['b'].isnull() & ~df['c'].isnull()]

        return df

    env.test('''
        SELECT
            b, c,
            sum(a) as sum_a,
            avg(a) as avg_a,
            count(a) as count_a,
            min(a) as min_a,
            max(a) as max_a

        FROM my_table
        GROUP BY b, c
    ''', postprocess_expected=postprocess_expected)


def test_grouped_aggregates_where(env):
    # pandas removes NULLs in groupby, SQL keeps them
    def postprocess_expected(env, df):
        if not env.strict:
            df = df[~df['b'].isnull() & ~df['c'].isnull()]

        return df

    env.test('''
        SELECT
            b, c,
            sum(a) as sum_a,
            avg(a) as avg_a,
            count(a) as count_a,
            min(a) as min_a,
            max(a) as max_a

        FROM my_table
        WHERE a > 2 AND b > 2
        GROUP BY b, c
    ''', postprocess_expected=postprocess_expected)


@pytest.mark.xfail(reason="not yet supported")
def test_grouped_aggregates_where_extended(env):
    # pandas removes NULLs in groupby, SQL keeps them
    def postprocess_expected(self, env, df):
        if not env.strict:
            df = df[~df['b'].isnull() & ~df['c'].isnull()]

        return df

    env.test('''
        SELECT
            b, c, 3 * b as d
            sum(a) as sum_a,
            avg(a) as avg_a,
            count(a) as count_a,
            min(a) as min_a,
            max(a) as max_a

        FROM my_table
        WHERE a > 2 AND b > 2
        GROUP BY b, 2, 2 * c, d
    ''', postprocess_expected=postprocess_expected)


def test_join(env):
    # Pandas keeps NULLs in the joined columns, SQL removes NULLs
    def postprocess_actual(env, df):
        if not env.strict:
            df = df[~df['a'].isnull()]

        return postprocess_expected(env, df)

    def postprocess_expected(env, df):
        return df.sort_values(['a', 'b', 'c', 'd'])

    env.test(
        query='''
            SELECT *

            FROM my_table

            JOIN my_other_table
            ON a = e
        ''',
        postprocess_actual=postprocess_actual,
        postprocess_expected=postprocess_expected,
    )


@pytest.mark.parametrize('expr', [
    "UPPER(a)",
    "LOWER(a)",
    "a || b",
    "a LIKE '%oo'",
    "a NOT LIKE '%oo'",

    # not supported by sqlite:
    # "CONCAT(a, b, c)",
    # "MID(c, 2, 3)",
    # "MID(c, 2)",
])
def test_string_functions(env, expr):
    env.test('SELECT {} as value FROM my_string_table'.format(expr))
