import sqlite3

from util import ConformanceTest, Environment, Table, Type, ColumnWithValues


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


class TestExample(ExampleEnvironment, ConformanceTest):
    query = 'SELECT * FROM my_table'


class TestUngroupedtAggregates(ExampleEnvironment, ConformanceTest):
    query = '''
        SELECT
            sum(a) as sum_a,
            avg(a) as avg_a,
            count(a) as count_a,
            min(a) as min_a,
            max(a) as max_a

        FROM my_table
    '''

class TestUngroupedtAggregatesCTE(ExampleEnvironment, ConformanceTest):
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


class TestUngroupedtAggregatesSubQuery(ExampleEnvironment, ConformanceTest):
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


class TestUngroupedtAggregatesNoAs(ExampleEnvironment, ConformanceTest):
    query = '''
        SELECT
            sum(a) sum_a,
            avg(a) avg_a,
            count(a) count_a,
            min(a) min_a,
            max(a) max_a

        FROM my_table
    '''


class TestUngroupedAggregates(ExampleEnvironment, ConformanceTest):
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


class TestUngroupedAggregatesWhere(ExampleEnvironment, ConformanceTest):
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


class TestJoin(ExampleEnvironment, ConformanceTest):
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
