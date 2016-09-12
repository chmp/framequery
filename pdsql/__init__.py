from __future__ import print_function, division, absolute_import

import inspect

from ._context import Context


def make_context(scope):
    return Context(scope=scope)

def select(query, scope=None):
    # TODO: get scope from stack if not passed
    if scope is None:
        scope = inspect.currentframe()

    return make_context(scope=scope).select(query)


def compile(query):
    return make_context({}).compile(query)
