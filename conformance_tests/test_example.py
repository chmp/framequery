import sqlite3
import unittest

from util import ConformanceTest, Table, Type, ColumnWithValues, version
import util

import pytest


class ExampleEnvironment(object):
    def configure_env(self, env):
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
        )


class TestExample(ExampleEnvironment, util.ConformanceTest):
    query = 'SELECT * FROM my_table'


class TestUngroupedtAggregates(ExampleEnvironment, util.ConformanceTest):
    query = '''
        SELECT
            sum(a) as sum_a,
            avg(a) as avg_a,
            count(a) as count_a,
            min(a) as min_a,
            max(a) as max_a

        FROM my_table
    '''


class TestUngroupedtAggregatesCTE(ExampleEnvironment, util.ConformanceTest):
    query = '''
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
    '''

    def configure_env(self, env):
        if version(sqlite3.sqlite_version) <= version('3.8.2'):
            raise unittest.SkipTest(
                "sqlite version ({}) does not support CTEs"
                .format(sqlite3.sqlite_version)
            )


class TestUngroupedtAggregatesSubQuery(ExampleEnvironment, util.ConformanceTest):
    query = '''
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
    '''


class TestUngroupedtAggregatesNoAs(ExampleEnvironment, util.ConformanceTest):
    query = '''
        SELECT
            sum(a) sum_a,
            avg(a) avg_a,
            count(a) count_a,
            min(a) min_a,
            max(a) max_a

        FROM my_table
    '''


class TestGroupedAggregates_SimpleExample(ExampleEnvironment, util.ConformanceTest):
    query = 'SELECT b, SUM(b) as a FROM my_table GROUP BY b'

    def postprocess_actual(self, env, df):
        return df.sort_values(list(df.columns))

    # pandas removes NULLs in groupby, SQL keeps them
    def postprocess_expected(self, env, df):
        if not env.strict:
            df = df[~df['b'].isnull()]

        return df.sort_values(list(df.columns))


class TestGroupedAggregates(ExampleEnvironment, util.ConformanceTest):
    query = '''
        SELECT
            b, c,
            sum(a) as sum_a,
            avg(a) as avg_a,
            count(a) as count_a,
            min(a) as min_a,
            max(a) as max_a

        FROM my_table
        GROUP BY b, c
    '''

    # pandas removes NULLs in groupby, SQL keeps them
    def postprocess_expected(self, env, df):
        if not env.strict:
            df = df[~df['b'].isnull() & ~df['c'].isnull()]

        return df


class TestGroupedAggregatesWhere(ExampleEnvironment, util.ConformanceTest):
    query = '''
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
    '''

    # pandas removes NULLs in groupby, SQL keeps them
    def postprocess_expected(self, env, df):
        if not env.strict:
            df = df[~df['b'].isnull() & ~df['c'].isnull()]

        return df



class TestGroupedAggregatesWhereExtended(ExampleEnvironment, util.ConformanceTest):
    query = '''
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
    '''

    def configure_env(self, env):
        raise pytest.xfail(reason="not yet supported")

    # pandas removes NULLs in groupby, SQL keeps them
    def postprocess_expected(self, env, df):
        if not env.strict:
            df = df[~df['b'].isnull() & ~df['c'].isnull()]

        return df


class TestJoin(ExampleEnvironment, util.ConformanceTest):
    query = '''
        SELECT *

        FROM my_table

        JOIN my_other_table
        ON a = e
    '''

    # Pandas keeps NULLs in the joined columns, SQL removes NULLs
    def postprocess_actual(self, env, df):
        if not env.strict:
            df = df[~df['a'].isnull()]

        return self.postprocess(df)

    def postprocess(self, df):
        return df.sort_values(['a', 'b', 'c', 'd'])
