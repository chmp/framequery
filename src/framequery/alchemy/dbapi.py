from __future__ import print_function, division, absolute_import

from framequery import execute


paramstyle = 'pyformat'
threadsafety = 1
apilevel = '2.0'


class Error(Exception):
    pass


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


STRING = str
BINARY = None
NUMBER = float
DATETIME = None
ROWID = None


def connect(**kwargs):
    return Connection(**kwargs)


class Connection(object):
    def __init__(self, scope=None, model='pandas'):
        if scope is None:
            scope = {}

        self.scope = scope
        self.model = model

    def cursor(self):
        return Cursor(self)


class Cursor(object):
    def __init__(self, scope):
        self.scope = scope
        self.description = self.result = None

        self.arraysize = 1

    def close(self):
        self.description = self.result = None

    def execute(self, q, params=None):
        if params:
            raise ValueError('params (%s) not yet supported' % params)

        self.result = execute(q, self.scope.scope, model=self.scope.model)

        self.description = []

        typemap = {'object': object, 'float': float}

        for col in self.result.columns:
            name = repr(col)
            typecode = self.result.dtypes[col]
            typecode = typemap[typecode.name]

            self.description.append((name, typecode, None, None, None, None, None))

        self.rownumber = 0

    def fetchone(self):
        if self.rownumber > self.result.shape[0]:
            return None

        row = tuple(self.result.iloc[self.rownumber])
        self.rownumber += 1

        return row

    def fetchmany(self, size=None):
        raise NotImplementedError()

    def fetchall(self):
        raise NotImplementedError()
