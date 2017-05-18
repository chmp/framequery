from __future__ import print_function, division, absolute_import

import os.path
import json

from sqlalchemy.dialects.postgresql.base import PGDialect

from ..executor import execute
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

        self.init_context(context, basepath)

        # NOTE: make the scope part of the connect args to reuse it
        return (), context

    @staticmethod
    def init_context(context, basepath):
        context.setdefault('model', 'pandas')
        context.setdefault('scope', {})

        basepath = context.get('basepath', basepath)

        for q in context.pop('setup', []):
            execute(q, scope=context['scope'], model=context['model'], basepath=basepath)

    def noop(self, *args):
        pass

    on_connect = do_rollback = noop

    del noop
