from __future__ import print_function, division, absolute_import

from .executor import execute

__all__ = ['execute', 'Scope', 'ResultProxy']


class Scope(object):
    def __init__(self, scope, model='pandas'):
        self.scope = scope
        self.model = model

    def execute(self, q):
        result = execute(q, self.scope, model=self.model)
        return ResultProxy(result)


class ResultProxy(object):
    def __init__(self, df):
        self.df = df

    def get(self):
        return self.df

    def fetchall(self):
        return list(tuple(t) for _, t in self.df.iterrows())
