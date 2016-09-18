from __future__ import print_function, division, absolute_import

import enum
import random
import unittest

import pandas as pd
import numpy
import pandas.util.testing as pdt

from framequery import make_context
from framequery._pandas_util import strip_table_name_from_columns


# TODO: hide this class from pytest
class ConformanceTest(unittest.TestCase):
    number_of_rows = 100

    def create_env(self):
        raise NotImplementedError()

    def postprocess(self, df):
        return df

    def run(self, result=None):
        if type(self) is ConformanceTest:
            return

        super(ConformanceTest, self).run(result)

    def test_conformance(self):
        env = self.create_env()
        realization = env.create_realization(self.number_of_rows)
        actual, expected = realization.execute(self.query)

        actual = self.postprocess(actual)
        expected = self.postprocess(expected)

        print("actual:")
        print(actual)
        print("expected:")
        print(expected)
        #pdt.assert_frame_equal(actual, expected)

        # NOTE: use all_close to tollerate type changes caused by pandas
        self.assertEqual(list(actual.columns), list(expected.columns))
        numpy.testing.assert_allclose(actual.values, expected.values)


class Environment(object):
    def __init__(self, connection_factory):
        self.connection_factory = connection_factory
        self.tables = []

    def add_tables(self, *tables):
        for table in tables:
            self.add_table(table)

    def add_table(self, table):
        self.tables.append(table)

    def create_realization(self, rows):
        realizations = {
            table.name: table.create_realization(rows) for table in self.tables
        }
        return EnvironmentRealization(self, realizations)


class EnvironmentRealization(object):
    def __init__(self, env, realizations):
        self.env = env
        self.realizations = realizations

    def get_context(self):
        scope = {
            name: realization.get_dataframe()
            for name, realization in self.realizations.items()
        }
        return make_context(scope)

    def execute(self, q):
        fq_result = self._fq_execute(q)
        sql_result = self._sql_execute(q)

        return fq_result, sql_result

    def _fq_execute(self, q):
        ctx = self.get_context()
        result = ctx.select(q)
        return strip_table_name_from_columns(result)

    def _sql_execute(self, q):
        conn = self.env.connection_factory()

        for realization in self.realizations.values():
            realization.insert(conn)

        result = conn.execute(q)
        return get_dataframe_from_cursor(result)



class Table(object):
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns

    def get_ddl(self):
        columns = ', '.join(
            '{} {}'.format(col.name, col.type.value) for col in self.columns
        )
        return 'CREATE TABLE {} ({})'.format(self.name, columns)

    def get_insert_statement(self):
        cols = ', '.join(col.name for col in self.columns)
        placeholders = ', '.join('?' for _ in self.columns)
        return 'INSERT INTO {}({}) VALUES({})'.format(
            self.name, cols, placeholders
        )

    def create_realization(self, rows):
        values = {
            col.name: col.generate(rows) for col in self.columns
        }

        return TableRealization(self, values)


class TableRealization(object):
    def __init__(self, table, values):
        self.table = table
        self.values = values

    def __repr__(self):
        return 'TableRealization({}, {})'.format(self.table, self.values)

    def insert(self, conn):
        values = self.get_values()

        conn.execute(self.table.get_ddl())
        conn.executemany(self.table.get_insert_statement(), values)

    def get_dataframe(self):
        columns = ['{}.{}'.format(self.table.name, col.name) for col in self.table.columns]
        data = {
            '{}.{}'.format(self.table.name, col): values
            for col, values in self.values.items()
        }
        return pd.DataFrame(data, columns=columns)

    def get_values(self):
        cols = [self.values[col.name] for col in self.table.columns]
        return list(zip(*cols))


class ColumDescription(object):
    pass


class ColumnWithValues(ColumDescription):
    def __init__(self, name, type, values):
        self.name = name
        self.type = type
        self.values = values

    def generate(self, rows):
        return [random.choice(self.values) for _ in range(rows)]


class Type(str, enum.Enum):
    integer = 'INTEGER'


def get_dataframe_from_cursor(cursor):
    columns = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    data = [dict(zip(columns, row)) for row in rows]
    return pd.DataFrame(data, columns=columns)
