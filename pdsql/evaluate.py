from __future__ import print_function, division, absolute_import

from .parser import as_parsed, ColumnReference, DerivedColumn
from ._pandas_util import strip_table_name_from_columns, ensure_table_columns
from ._visitor import node_name_to_handler_name

from collections import OrderedDict

import numpy as np
import pandas as pd


def evaluate(q, scope=None):
    if scope is None:
        scope = {}

    q = as_parsed(q)

    evaluator = Evaluator()
    return evaluator.evaluate(q, scope)


class Evaluator(object):
    def evaluate(self, q, scope):
        assert len(q.from_clause) == 1

        # TODO: filter table
        table = lookup_table(scope, q.from_clause[0])

        if q.group_by_clause is None:
            result = self.evaluate_direct(q.select_list, table)
            result = self.wrap_result(result)

        else:
            result = self.evaluate_grouped(q, table)

        return strip_table_name_from_columns(result)

    def evaluate_grouped(self, q, table):
        group_derived = self._groupby_as_derived(q.group_by_clause)
        group_columns = self.evaluate_direct(group_derived, table)

        grouped = table.groupby(list(group_columns.values()))

        parts = []
        for (group_idx, (key, group)) in enumerate(grouped):
            group_table = {
                k: v for (k, v) in group.items()
            }

            if len(group_columns) == 1:
                key = [key]

            group_table.update(zip(group_columns.keys(), key))

            group_result = self.evaluate_direct(q.select_list, group_table)
            group_result = self.wrap_result(group_result, index=[group_idx])
            parts.append(group_result)

        return self.concat(parts)

    def _groupby_as_derived(self, group_by):
        return [
            DerivedColumn(col, alias=col.value[-1]) for col in group_by
        ]

    def evaluate_direct(self, select_list, table):
        result = OrderedDict()
        for idx, column in enumerate(select_list):
            alias = normalize_alias(table, idx, column)
            result[alias] = self.evaluate_value(column.value, table)

        return result

    def evaluate_value(self, col, table):
        col_type = col.__class__.__name__
        handler_name = node_name_to_handler_name(col_type, prefix="evaluate_value")

        handler = getattr(self, handler_name, None)
        if handler is None:
            raise ValueError('cannot handle {} (no handler {})'.format(col, handler_name))

        return handler(col, table)

    def evaluate_value_integer(self, col, table):
        return self.as_scalar(int(col.value))

    def evaluate_value_column_reference(self, col, table):
        col_ref = normalize_col_ref(table, col)
        return table[col_ref]

    def evaluate_value_binary_expression(self, col, table):
        left = self.evaluate_value(col.left, table)
        right = self.evaluate_value(col.right, table)

        if col.operator == '*':
            return left * right

        elif col.operator == '/':
            return left / right

        elif col.operator == '+':
            return left + right

        elif col.operator == '-':
            return left - right

        else:
            raise ValueError("unknown operator {}".format(col.operator))

    def evaluate_value_general_set_function(self, col, table):
        if col.quantifier is not None:
            raise ValueError("quantifiers are currently not supported")

        value = self.evaluate_value(col.value, table)
        return self.aggregate(col.function, value)

    def concat(self, parts):
        return pd.concat(parts, axis=0)

    def wrap_result(self, result, index=None):
        return pd.DataFrame(result, index=index)

    def as_scalar(self, value):
        return [value]

    def aggregate(self, function, value):
        if function == 'SUM':
            return self.as_scalar(value.sum())

        elif function == 'AVG':
            return self.as_scalar(value.mean())

        elif function == 'MIN':
            return self.as_scalar(value.min())

        elif function == 'MAX':
            return self.as_scalar(value.max())

        else:
            raise NotImplementedError("unsupported aggregation function {}".format(function))


def lookup_table(scope, table_ref):
    table_ref = normalize_table_ref(scope, table_ref)

    if table_ref == 'DUAL':
        return None

    return scope[table_ref]


def normalize_table_ref(scope, table_ref):
    # TODO: handle schemas, etc ...
    return table_ref.value


def normalize_col_ref(table, col_ref):
    # TODO: handle schemas, etc ...
    return col_ref.value[0]


def normalize_alias(table, idx, col):
    if isinstance(col.value, ColumnReference):
        return col.value.value[-1]

    if col.alias is None:
        return '__{}'.format(idx)

    return col.alias
