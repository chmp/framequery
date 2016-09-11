from __future__ import print_function, division, absolute_import

import itertools as it

from .parser import DerivedColumn, ColumnReference
from ._util.introspect import call_handler


def compile_dag(node):
    raise NotImplementedError()


def split_aggregate(expr, id_generator=None):
    """Split expr into pre-aggregation, aggregation, and post-aggregation.

    :return Tuple[Node,List[DerivedColumn],List[DerivedColumn]]:
    """
    if id_generator is None:
        id_generator = default_generator()

    return SplitAggregateTransformer(id_generator).split(expr)


class SplitAggregateTransformer(object):
    def __init__(self, id_generator):
        self.id_generator = id_generator

    def split(self, node):
        return call_handler(self, "split", node)

    def split_column_reference(self, node):
        return node, [], []

    def split_general_set_function(self, node):
        value_node, aggs, pre_aggs = self.split(node.value)

        if aggs or pre_aggs:
            raise ValueError("multiple aggregation levels not allowed")

        pre_agg_col = self._tmp_column()
        agg_col = self._tmp_column()

        pre_agg = DerivedColumn(value_node, alias=pre_agg_col)
        agg = DerivedColumn(
            node.with_values(value=_ref(pre_agg_col)),
            alias=agg_col,
        )
        result = _ref(agg_col)

        return result, [agg], [pre_agg]

    def split_binary_expression(self, node):
        left_node, left_aggs, left_pre_aggs = self.split(node.left)
        right_node, right_aggs, right_pre_aggs = self.split(node.right)

        return (
            node.with_values(left=left_node, right=right_node),
            left_aggs + right_aggs,
            left_pre_aggs + right_pre_aggs,
        )

    def split_integer(self, node):
        return node, [], []

    def _tmp_column(self):
        return '${}'.format(next(self.id_generator))

def default_generator():
    for i in it.count():
        yield str(i)


def _ref(col_name):
    return ColumnReference([col_name])
