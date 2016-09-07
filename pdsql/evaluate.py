from __future__ import print_function, division, absolute_import

from .parser import as_parsed, ColumnReference
from ._visitor import node_name_to_handler_name


def evaluate(q, scope=None):
    if scope is None:
        scope = {}

    q = as_parsed(q)

    evaluator = Evaluator(scope)
    return evaluator.evaluate(q)


class Evaluator(object):
    def __init__(self, scope):
        self.scope = scope

    def evaluate(self, q):
        assert len(q.from_clause) == 1

        # TODO: filter table
        table = lookup_table(self.scope, q.from_clause[0])
        return self.evaluate_direct(q, table)

    def evaluate_direct(self, q, table):
        result = {}
        for idx, column in enumerate(q.select_list):
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
        return int(col.value)

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

    def wrap_result(self):
        raise NotImplementedError()


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
