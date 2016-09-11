from __future__ import print_function, division, absolute_import

from ._dag_compile import compile_dag
from ._parser import Select


def select(query, scope=None):
    # TODO: get scope from stack if not passed
    if scope is None:
        raise NotImplementedError()


def compile(query):
    node = Select.parse(query)
    return compile_dag(node)
