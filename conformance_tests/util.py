from __future__ import print_function, division, absolute_import

import json
import logging
import os
import random
import unittest

import pandas as pd
import dask.dataframe as dd
import numpy
import sqlalchemy

from framequery import make_context
from framequery._dask import DaskExecutor
from framequery._pandas import PandasExecutor
from framequery._pandas_util import strip_table_name_from_columns

_logger = logging.getLogger(__name__)
default_env_file = os.path.join(os.path.dirname(__file__), 'config', 'sqlite.json')


# TODO: hide this class from pytest
class ConformanceTest(unittest.TestCase):
    number_of_rows = 100

    def create_env(self):
        env_file = os.environ.get('FRAMEQUERY_ENV', default_env_file)
        _logger.info("using environment %s", env_file)

        with open(env_file, 'r') as fobj:
            config = json.load(fobj)

        connection_string = config['connection']
        strict = config.get('context', {}).get('strict', False)

        executor_factory = config.get('context', {}).get('executor', 'pandas')

        if executor_factory == 'pandas':
            executor_factory = PandasExecutor

        elif executor_factory == 'dask':
            executor_factory = DaskExecutor

        return Environment(
            connection_factory=lambda: sqlalchemy.create_engine(connection_string),
            executor_factory=executor_factory,
            strict=strict,
        )

    def configure_env(self, env):
        pass

    def postprocess_actual(self, env, df):
        return self.postprocess(df)

    def postprocess_expected(self, env, df):
        return self.postprocess(df)

    def postprocess(self, df):
        return df

    def run(self, result=None):
        if type(self) is ConformanceTest:
            return

        super(ConformanceTest, self).run(result)

    def test_conformance(self):
        env = self.create_env()
        self.configure_env(env)

        realization = env.create_realization(self.number_of_rows)
        actual, expected = realization.execute(self.query)

        actual = self.postprocess_actual(env, actual)
        expected = self.postprocess_expected(env, expected)

        print("actual:")
        print(actual)
        print("expected:")
        print(expected)

        # NOTE: use all_close to tollerate type changes caused by pandas
        self.assertEqual(list(actual.columns), list(expected.columns))
        numpy.testing.assert_allclose(actual.values, expected.values)


class Environment(object):
    def __init__(self, connection_factory, executor_factory, strict=False):
        self.connection_factory = connection_factory
        self.executor_factory = executor_factory
        self.strict = strict
        self.tables = []

    def add_tables(self, *tables):
        for table in tables:
            self.add_table(table)

    def add_table(self, table):
        self.tables.append(table)

    def create_realization(self, rows):
        realizations = {
            table.name: table.create_realization(rows, self) for table in self.tables
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
        return make_context(
            scope,
            strict=self.env.strict,
            executor_factory=self.env.executor_factory,
        )

    def execute(self, q):
        fq_result = self._fq_execute(q)
        sql_result = self._sql_execute(q)

        return fq_result, sql_result

    def _fq_execute(self, q):
        ctx = self.get_context()
        result = ctx.select(q)

        if issubclass(self.env.executor_factory, DaskExecutor):
            # TODO: make sync / async configurable
            import dask
            with dask.set_options(get=dask.async.get_sync):
                result = result.compute()

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

    def create_realization(self, rows, env):
        values = {
            col.name: col.generate(rows) for col in self.columns
        }

        return TableRealization(self, values, env)


class TableRealization(object):
    def __init__(self, table, values, env):
        self.table = table
        self.values = values
        self.env = env

    def __repr__(self):
        return 'TableRealization({}, {})'.format(self.table, self.values)

    def insert(self, conn):
        values = self.get_values()

        conn.execute(self.table.get_ddl())
        conn.execute(self.table.get_insert_statement(), values)

    def get_dataframe(self):
        columns = ['{}.{}'.format(self.table.name, col.name) for col in self.table.columns]
        data = {
            '{}.{}'.format(self.table.name, col): values
            for col, values in self.values.items()
        }
        df = pd.DataFrame(data, columns=columns)

        if issubclass(self.env.executor_factory, DaskExecutor):
            df = dd.from_pandas(df, npartitions=10)

        return df

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


class _Value(object):
    def __init__(self, value):
        self.value = value


class Type(str):
    integer = _Value('INTEGER')


def get_dataframe_from_cursor(cursor):
    rows = cursor.fetchall()
    columns = cursor.keys()
    data = [dict(row) for row in rows]
    return pd.DataFrame(data, columns=columns)
