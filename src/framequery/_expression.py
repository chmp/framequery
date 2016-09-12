"""Evaluation of expressions.
"""
from __future__ import print_function

import operator
import logging

from ._util.introspect import call_handler

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
        _logger.info("evaluate binary expression %s", col.operator)
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
        _logger.info("eval column reference %s", col.value)
        ref = self._normalize_col_ref(col.value, table.columns)
        return table[ref]

    def _normalize_col_ref(self, ref, columns):
        ref = ref[-2:]

        if len(ref) == 2:
            return tuple(ref)

        candidates = [
            (source, name) for (source, name) in columns
            if name == ref[-1]
        ]

        if len(candidates) == 0:
            raise ValueError("column {} not found".format(ref))

        if len(candidates) > 1:
            raise ValueError("column {} is ambigious".format(ref))

        return tuple(candidates[0])
