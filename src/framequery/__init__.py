from __future__ import print_function, division, absolute_import

import inspect

from ._context import Context
from ._graphviz import to_dot, show


def make_context(scope):
    return Context(scope=scope)


def select(query, scope=None):
    """Execute a SELECT query on the given scope.

    :param str query: the select statement as a string.

    :param Optional[Mapping[str,pandas.DataFrame]] scope: the scope as a mapping
        of table name to DataFrame. If not given the locals and globals of the
        calling scope are used to build the dictionary.

    :returns pandas.DataFrame: the result of the query.
    """
    if scope is None:
        scope = inspect.currentframe()

    return make_context(scope=scope).select(query)


def compile(query):
    """Compile a query into a DAG of highlevel dataframe transformations.
    """
    return make_context({}).compile(query)
