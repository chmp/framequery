from __future__ import print_function, division, absolute_import

import itertools as it

from . import _parser, _dag
from ._parser import DerivedColumn, ColumnReference, get_selected_column_name
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
        # NOTE: ordering is performed in the transform step to allow ordering on
        #       non-selected columns.
        table = self.compile_from_clause(node.from_clause)
        table = self._filter_pre_transform(node, table)
        table = self._transform_table(node, table)
        table = self._limit(node, table)
        table = self._filter_post_transform(node, table)
        return table

    def _filter_pre_transform(self, node, table):
        if node.where_clause is None:
            return table

        return _dag.Filter(table, node.where_clause)

    def _transform_table(self, node, table):
        if node.select_list == _parser.Asterisk():
            assert node.group_by_clause is None
            table = self._order(node, table)
            return table

        columns, aggregates, pre_aggregates = split_aggregates(
            node.select_list,
            id_generator=self.id_generator
        )

        group_by, group_by_cols = self._normalize_group_by(node.group_by_clause)

        pre_aggregates.extend(group_by_cols)
        if pre_aggregates:
            table = _dag.Transform(table, pre_aggregates)

        if aggregates:
            table = _dag.Aggregate(table, aggregates, group_by=group_by)

        table = self._order(node, table)
        return _dag.Transform(table, columns)

    def _normalize_group_by(self, group_by):
        if group_by is None:
            return None, []

        result_group_by = []
        result_pre_aggs = []

        for col in group_by:
            col_id = self._group_by_col_alias(col)
            result_group_by.append(ColumnReference([col_id]))
            result_pre_aggs.append(DerivedColumn(col, alias=col_id))

        return result_group_by, result_pre_aggs

    def _group_by_col_alias(self, col):
        col_id = get_selected_column_name(col)
        return col_id if col_id is not None else self._tmp_column()

    def _filter_post_transform(self, node, table):
        if node.having_clause is None:
            return table

        return _dag.Filter(table, node.having_clause)

    def _order(self, node, table):
        if node.order_by_clause is None:
            return table

        return _dag.Sort(table, node.order_by_clause)

    def _limit(self, node, table):
        if node.limit_clause is None:
            return table

        return _dag.Limit(table, node.limit_clause.offset, node.limit_clause.limit)

    def compile_from_clause(self, from_clause):
        table = from_clause[0]
        assert isinstance(table, _parser.TableName)
        return _dag.GetTable(table.table, alias=table.alias)

    def _tmp_column(self):
        return '${}'.format(next(self.id_generator))


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
