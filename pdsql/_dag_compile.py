from __future__ import print_function, division, absolute_import

import itertools as it

from . import _parser, _dag
from ._parser import DerivedColumn, ColumnReference
from ._util.introspect import call_handler


def compile_dag(node, id_generator=None):
    if id_generator is None:
        id_generator = default_generator()

    return DagCompiler(id_generator).compile(node)


def split_aggregate(expr, id_generator=None):
    """Split expr into pre-aggregation, aggregation, and post-aggregation.

    :return Tuple[Node,List[DerivedColumn],List[DerivedColumn]]:
    """
    if id_generator is None:
        id_generator = default_generator()

    return SplitAggregateTransformer(id_generator).split(expr)


def split_aggregates(exprs, id_generator=None):
    if id_generator is None:
        id_generator = default_generator()

    cols = []
    aggs = []
    pre_aggs = []

    for col in exprs:
        c, a, p = split_aggregate(col, id_generator=id_generator)
        cols.append(c)
        aggs.extend(a)
        pre_aggs.extend(p)

    return cols, aggs, pre_aggs


class DagCompiler(object):
    def __init__(self, id_generator):
        self.id_generator = id_generator

    def compile(self, node):
        return call_handler(self, "compile", node)

    def compile_select(self, node):
        table = self.compile_from_clause(node.from_clause)
        table = self._filter_pre_transform(node, table)
        table = self._transform_table(node, table)
        table = self._filter_post_transform(node, table)
        return table

    def _filter_pre_transform(self, node, table):
        if node.where_clause is None:
            return table

        return _dag.Filter(table, node.where_clause)

    def _transform_table(self, node, table):
        if node.select_list == _parser.Asterisk():
            assert node.group_by_clause is None
            return table

        columns, aggregates, pre_aggregates = split_aggregates(
            node.select_list,
            id_generator=self.id_generator
        )
        print(columns)
        print(aggregates)
        print(pre_aggregates)

        if pre_aggregates:
            table = _dag.Transform(table, pre_aggregates)

        if aggregates:
            table = _dag.Aggregate(table, aggregates)

        return _dag.Transform(table, columns)

    def _filter_post_transform(self, node, table):
        if node.having_clause is None:
            return table

        return _dag.Filter(table, node.having_clause)

    def compile_from_clause(self, from_clause):
        table = from_clause[0]
        assert isinstance(table, _parser.TableName)
        return _dag.GetTable(table.table, alias=table.alias)


class SplitAggregateTransformer(object):
    def __init__(self, id_generator):
        self.id_generator = id_generator

    def split(self, node):
        return call_handler(self, "split", node)

    def split_derived_column(self, node):
        value, agg, pre_aggs = self.split(node.value)
        return node.with_values(value=value), agg, pre_aggs

    def split_column_reference(self, node):
        return node, [], []

    def split_general_set_function(self, node):
        # TODO: add shortcut for direct column references in aggregates

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
