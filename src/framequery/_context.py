from __future__ import print_function, division, absolute_import

from ._dag_compile import compile_dag
from ._pandas import PandasExecutor
from ._parser import Select

import collections
import logging

_logger = logging.getLogger(__name__)


class Context(object):
    def __init__(self, scope=None):
        if scope is None:
            scope = {}

        self._scope = build_scope(scope)
        self._ex = PandasExecutor()

    def select(self, query):
        ast = self.compile(query)

        _logger.debug('eval ast %s', ast)
        return self._ex.evaluate(ast, self._scope)

    def compile(self, query):
        node = Select.parse(query)
        return compile_dag(node)


def build_scope(obj=None):
    if obj is None:
        obj = insepct.getcurrentframe()

    if isinstance(obj, collections.Mapping):
        return obj

    return _get_caller_scope(obj)


def _get_caller_scope(frame):
    if frame.f_back is None:
        raise ValueError("needs to be called inside a function")

    scope = dict(frame.f_back.f_globals)
    scope.update(frame.f_back.f_locals)
    return scope
