import sqlite3

from util import ConformanceTest, Environment, Table, Type, ColumnWithValues


class ExampleEnvironment(object):
    def create_env(self):
        env = Environment(lambda: sqlite3.connect(':memory:'))
        env.add_tables(
            Table('my_table', [
                ColumnWithValues('a', Type.integer, [1, 2, 3, None]),
                ColumnWithValues('b', Type.integer, [1, 2, 3, None]),
            ]),
            Table('my_other_table', [
                ColumnWithValues('c', Type.integer, [-1, 2, 3, None]),
                ColumnWithValues('d', Type.integer, [-1, 2, 3, None]),
            ]),
        )
        return env


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


class TestUngroupedAggregates(ExampleEnvironment, ConformanceTest):
    query = '''
        SELECT
            b,
            sum(a) as sum_a,
            avg(a) as avg_a,
            count(a) as count_a,
            min(a) as min_a,
            max(a) as max_a

        FROM my_table
        GROUP BY b
    '''

    # TODO: check what is standard SQL
    # pandas removes NULLs in groupby, sqlite keeps them
    def postprocess(self, df):
        return df[~df['b'].isnull()]


class TestUngroupedAggregatesWhere(ExampleEnvironment, ConformanceTest):
    query = '''
        SELECT
            b,
            sum(a) as sum_a,
            avg(a) as avg_a,
            count(a) as count_a,
            min(a) as min_a,
            max(a) as max_a

        FROM my_table
        WHERE a > 2 AND b > 2
        GROUP BY b
    '''

    # TODO: check what is standard SQL
    # pandas removes NULLs in groupby, sqlite keeps them
    def postprocess(self, df):
        return df[~df['b'].isnull()]


class TestJoin(ExampleEnvironment, ConformanceTest):
    query = '''
        SELECT *

        FROM my_table

        JOIN my_other_table
        ON a = c
    '''

    # TODO: check what is standard SQL for NULLs
    # Pandas keeps NULLs in the joined columns, sqlite removes NULLs
    def postprocess(self, df):
        return df[~df['a'].isnull()].sort_values(['a', 'b', 'c', 'd'])
