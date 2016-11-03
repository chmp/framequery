"""Evaluation of expressions.
"""
from __future__ import print_function

import operator
import logging

from ._util.introspect import call_handler
from ._pandas_util import (
    apply_analytics_function,
    normalize_col_ref,
)
from ._parser import ColumnReference

_logger = logging.getLogger(__name__)


class ExpressionEvaluator(object):
    def __init__(self):
        self.functions = {}

    def evaluate_value(self, node, scope):
        return call_handler(self, 'evaluate_value', node, scope)

    def evaluate_value_function_call(self, node, scope):
        args = [self.evaluate_value(arg, scope) for arg in node.arguments]
        func = self.functions[node.function.upper()]
        return func(*args)

    def evaluate_value_analytics_function(self, node, table):
        if node.partition_by is not None:
            partition_by = [
                normalize_col_ref(n.value, table.columns)
                for n in node.partition_by
            ]

        else:
            partition_by = None

        if node.order_by is not None:
            sort_by, ascending = self._split_order_by_items(node.order_by, table)

        else:
            sort_by = None
            ascending = None

        func = node.function
        func_name = func.function.upper()

        if func_name == 'SUM':
            impl = operator.methodcaller('sum')

        elif func_name == 'AVG':
            impl = operator.methodcaller('mean')

        else:
            raise ValueError("unknown analytics function {}".format(func_name))

        assert len(func.arguments) == 1
        arg0 = normalize_col_ref(func.arguments[0].value, table.columns)

        return apply_analytics_function(
            table, arg0, impl,
            partition_by=partition_by,
            sort_by=sort_by,
            ascending=ascending,
        )

    def evaluate_value_unary_expression(self, col, table):
        op = col.operator.upper()
        operand = self.evaluate_value(col.operand, table)

        if op == '-':
            return -operand

        elif op == '+':
            return operand

        elif op == 'NOT':
            return ~operand

    def evaluate_value_binary_expression(self, col, table):
        _logger.debug("evaluate binary expression %s", col.operator)
        op = col.operator.upper()
        left = self.evaluate_value(col.left, table)
        right = self.evaluate_value(col.right, table)

        operator_map = {
            '*': operator.mul,
            '/': operator.truediv,
            '+': operator.add,
            '-': operator.sub,
            'AND': operator.and_,
            'OR': operator.or_,
            '<': operator.lt,
            '>': operator.gt,
            '<=': operator.le,
            '>=': operator.ge,
            '=': operator.eq,
            '!=': operator.ne,
        }

        if op in operator_map:
            op = operator_map[op]
            return op(left, right)

        elif op == 'IN':
            return left.isin(right)

        else:
            raise ValueError("unknown operator {}".format(operator))

    def evaluate_value_integer(self, col, table):
        _logger.debug("eval integer")
        return int(col.value)

    def evaluate_value_float(self, col, table):
        _logger.debug("eval float")
        return float(col.value)

    def evaluate_value_column_reference(self, col, table):
        _logger.debug("eval column reference %s", col.value)
        ref = normalize_col_ref(col.value, table.columns)
        return table[ref]

    def _split_order_by_items(self, items, table):
        values = []
        ascending = []

        for col in items:
            val, asc = self._split_order_by_item(col, table)
            values.append(val)
            ascending.append(asc)

        return values, ascending

    def _split_order_by_item(self, item, table):
        assert isinstance(item.value, ColumnReference)

        return (
            normalize_col_ref(item.value.value, table.columns),
            item.order == 'ASC'
        )
