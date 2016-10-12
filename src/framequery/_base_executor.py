from __future__ import print_function, division, absolute_import

import collections

import pandas as pd

from ._pandas_util import (
    as_pandas_join_condition,
    column_from_parts,
    cross_join,
    ensure_table_columns,
)
from ._parser import get_selected_column_name, GeneralSetFunction, ColumnReference
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

    def evaluate_literal(self, node, _):
        return node.value

    def evaluate_get_table(self, node, scope):
        if node.table == 'DUAL':
            table = self._get_dual()

        else:
            table = scope[node.table]

        alias = node.alias if node.alias is not None else node.table
        return ensure_table_columns(alias, table)

    def evaluate_define_tables(self, node, scope):
        scope = scope.copy()

        for name, sub_node in node.tables:
            scope[name] = self.evaluate(sub_node, scope)

        return self.evaluate(node.node, scope)
    
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

    def evaluate_aggregate(self, node, scope):
        table = self.evaluate(node.table, scope)

        if node.group_by is None:
            return self._evaluate_aggregation_non_grouped(node, table)

        else:
            return self._evaluate_aggregation_grouped(node, table)

    def _evaluate_aggregation_non_grouped(self, node, table):
        values = self._evaluate_aggregation_base(node, table, table.columns)
        return self._dataframe_from_scalars(values)

    def _evaluate_aggregation_grouped(self, node, table):
        raise NotImplementedError()

    def _evaluate_aggregation_base(self, node, table, columns):
        """Helper function to simplify implementation of groups
        """
        table_id = next(self.id_generator)
        result = collections.OrderedDict()

        for col in node.columns:
            col_id = col.alias if col.alias is not None else next(self.id_generator)
            result[column_from_parts(table_id, col_id)] = self._agg(col.value, table, columns)

        return result

    def _agg(self, node, table, columns):
        if not isinstance(node, GeneralSetFunction):
            raise ValueError("indirect aggregations not supported")

        function = node.function.upper()
        value = node.value

        if not isinstance(value, ColumnReference):
            raise ValueError("indirect aggregations not supported")

        col_ref = self._normalize_col_ref(value.value, columns)
        col = table[col_ref]

        # TODO: handle set quantifiers
        assert node.quantifier is None

        impls = {
            'SUM': lambda col: col.sum(),
            'AVG': lambda col: col.mean(),
            'MIN': lambda col: col.min(),
            'MAX': lambda col: col.max(),
            'COUNT': lambda col: col.count(),
            'FIRST_VALUE': self._first,
        }

        try:
            impl = impls[function]

        except KeyError:
            raise ValueError("unknown aggregation function {}".format(function))

        else:
            result = impl(col)

        return result

    def _dataframe_from_scalars(self, values):
        raise NotImplementedError()


    def _first(self, s):
        if isinstance(s, pd.Series):
            return s.iloc[0]

        return s.first()
