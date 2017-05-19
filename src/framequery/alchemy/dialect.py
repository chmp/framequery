from __future__ import print_function, division, absolute_import

import os.path
import json

from sqlalchemy.dialects.postgresql.base import PGDialect

from ..executor import Executor, execute
from . import dbapi


class PandasDialect(PGDialect):
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

        return executor

    def get_table_names(self, conn, schema=None, **kwargs):
        return sorted(conn.connection.executor.scope.keys())

    on_connect = do_rollback = lambda *args: None
