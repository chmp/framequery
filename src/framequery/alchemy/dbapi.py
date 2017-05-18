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
    def __init__(self, scope, **exec_kwargs):
        self.scope = scope
        self.exec_kwargs = exec_kwargs

    def cursor(self):
        return Cursor(self)

    def noop(self, *args):
        pass

    commit = rollback = close = noop
    del noop


class Cursor(object):
    def __init__(self, connection):
        self.connection = connection
        self.rowcount = self.description = self.result = None

        self.arraysize = 100

    def close(self):
        self.rowcount = self.description = self.result = None

    def execute(self, q, params=None):
        if q.strip().startswith('!'):
            return self._execute_extension(q, params)

        if params:
            raise ValueError('params (%s) not yet supported' % params)

        self.result = execute(q, self.connection.scope, **self.connection.exec_kwargs)

        if self.result is None:
            return

        self.description = []

        typemap = {'object': object, 'float': float, 'int64': int}

        for col in self.result.columns:
            name = repr(col)
            typecode = self.result.dtypes[col]
            typecode = typemap[typecode.name]

            self.description.append((name, typecode, None, None, None, None, None))

        self.rownumber = 0
        self.rowcount = self.result.shape[0]

    def executemany(self, q, parameters):
        for p in parameters:
            self.execute(q, p)

    def _execute_extension(self, q, params):
        q = q.strip().lower()
        if q == '!update':
            assert isinstance(params, dict)
            self.connection.scope.update(params)
            self.rowcount = 0

        else:
            raise RuntimeError('no extension for %s implemented' % q)

    def fetchone(self):
        if self.rownumber > self.result.shape[0]:
            return None

        row = tuple(self.result.iloc[self.rownumber])
        self.rownumber += 1

        return row

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize

        start, end = self.rownumber, self.rownumber + size
        self.rownumber += size

        return [
            tuple(row) for _, row in self.result.iloc[start:end].iterrows()
        ]

    def fetchall(self):
        old_rownumber = self.rownumber
        self.rownumber = self.rowcount

        return [
            tuple(row) for _, row in self.result.iloc[old_rownumber:].iterrows()
        ]

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass
