from __future__ import print_function, division, absolute_import

import os.path
import json

from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.engine import Engine

from ..executor import Executor
from . import dbapi


class Dialect(PGDialect):
    @classmethod
    def dbapi(self):
        return dbapi

    def create_connect_args(self, url):
        if url.database:
            with open(url.database, 'r') as fobj:
                context = json.load(fobj)

            basepath = os.path.abspath(os.path.dirname(url.database))

        else:
            context = {}
            basepath = os.path.abspath('.')

        context.update(url.query)

        executor = self.build_executor(context, basepath)
        return (executor,), {}

    @staticmethod
    def build_executor(context, basepath):
        context.setdefault('model', 'pandas')
        context.setdefault('scope', {})

        basepath = context.get('basepath', basepath)

        executor = Executor({}, model=context['model'], basepath=context.get('basepath', '.'))

        for q in context.pop('setup', []):
            executor.execute(q, basepath=basepath)

        # TODO: add custom table functions to the executor

        return executor

    @classmethod
    def engine_created(cls, engine):
        with engine.connect() as conn:
            engine.executor = conn.connection.executor

        return engine

    def get_table_names(self, conn, schema=None, **kwargs):
        # TODO: handle information schema and pg_catalog schema
        executor = get_executor(conn)
        return sorted(executor.scope.keys())

    on_connect = do_rollback = lambda *args: None


def get_executor(obj):
    """Extract the executor from a framequery sqlalchemy engine or connection.

    Usage::

        executor = get_executor(engine)

    """
    if isinstance(obj, Engine):
        return obj.executor

    return obj.engine.executor
