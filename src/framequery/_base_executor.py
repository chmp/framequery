from __future__ import print_function, division, absolute_import

import collections

from ._pandas_util import (
    as_pandas_join_condition,
    column_from_parts,
    cross_join,
    ensure_table_columns,
)
from ._parser import get_selected_column_name
from ._util.executor import default_id_generator
from ._util.introspect import call_handler


class BaseExecutor(object):
    """Common helper functions for executor classes.
    """
    def _set_id_generator(self, id_generator=None):
        if id_generator is None:
            id_generator = default_id_generator()

        self.id_generator = id_generator

    def _set_strict(self, strict=False):
        self.strict = bool(strict)

    def _get_selected_column_name(self, col):
        col_id = get_selected_column_name(col)
        if col_id is not None:
            return col_id

        return next(self.id_generator)

    def _get_dual(self):
        raise NotImplementedError()

    def _combine_series(self, series):
        """Given a mapping object of series construct a dataframe.
        """
        raise NotImplementedError()

    def evaluate(self, node, arg):
        return call_handler(self, 'evaluate', node, arg)

    def evaluate_get_table(self, node, scope):
        if node.table == 'DUAL':
            table = self._get_dual()

        else:
            table = scope[node.table]

        alias = node.alias if node.alias is not None else node.table
        return ensure_table_columns(alias, table)

    def evaluate_transform(self, node, scope):
        table = self.evaluate(node.table, scope)

        result = collections.OrderedDict()

        table_id = next(self.id_generator)
        for col in node.columns:
            col_id = self._get_selected_column_name(col)
            value = self.evaluate_value(col.value, table)

            result[column_from_parts(table_id, col_id)] = value

        return self._combine_series(result)

    def evaluate_drop_duplicates(self, node, scope):
        table = self.evaluate(node.table, scope)
        table = table.drop_duplicates()
        return table
