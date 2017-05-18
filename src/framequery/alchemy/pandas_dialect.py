from __future__ import print_function, division, absolute_import

from sqlalchemy.dialects.postgresql.base import PGDialect

from . import dbapi


class PandasDialect(PGDialect):
    @classmethod
    def dbapi(self):
        return dbapi

    def create_connect_args(self, url):
        # NOTE: make the scope part of the connect args to reuse it
        return (), {'model': 'pandas', 'scope': {}}

    def noop(self, *args):
        pass

    on_connect = do_rollback = noop

    del noop
