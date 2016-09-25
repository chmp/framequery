"""Compile the SQL AST into dataframe transformations.
"""
from __future__ import print_function, division, absolute_import

import itertools as it

from . import _parser, _dag
from ._parser import DerivedColumn, ColumnReference, get_selected_column_name
from ._util.introspect import call_handler


def compile_dag(node, id_generator=None):
    if id_generator is None:
        id_generator = default_generator()

    return DagCompiler(id_generator).compile(node)


def split_analytics_functions(exprs, id_generator=None):
    if id_generator is None:
        id_generator = default_generator()

    is_synthetic = []
    cols = []
    aggs = []
    pre_aggs = []

    splitter = SplitAnalyticsFunctionTransformer(id_generator)

    for col in exprs:
        s, c, a, p = splitter.do_split(col)
        is_synthetic.append(s)
        cols.append(c)
        aggs.extend(a)
        pre_aggs.extend(p)

    # shortcut to speed up non analytic functions queries
    if all(is_synthetic):
        return exprs, [], []

    return cols, aggs, pre_aggs


def split_aggregates(exprs, id_generator=None):
    """Split exprs into pre-aggregation, aggregation, and post-aggregation.

    :return Tuple[List[Node],List[DerivedColumn],List[DerivedColumn]]:
    """
    if id_generator is None:
        id_generator = default_generator()

    cols = []
    aggs = []
    pre_aggs = []

    splitter = SplitAggregateTransformer(id_generator)

    for col in exprs:
        c, a, p = splitter.do_split(col)
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
        table = self._filter_post_transform(node, table)
        table = self._drop_duplicates(node, table)
        table = self._limit(node, table)

        if node.common_table_expressions:
            table = self._add_definitions(node, table)

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

        post_analytics, analytics, pre_analytics = split_analytics_functions(
            columns,
            id_generator=self.id_generator,
        )

        group_by, group_by_cols = self._normalize_group_by(node.group_by_clause)

        pre_aggregates.extend(group_by_cols)
        if pre_aggregates:
            table = _dag.Transform(table, pre_aggregates)

        if aggregates:
            table = _dag.Aggregate(table, aggregates, group_by=group_by)

        if analytics:
            # to avoid unnecessary copies, pre-analytics are not used
            assert not pre_analytics
            table = _dag.Transform(table, analytics)
            table = self._order(node, table)
            return _dag.Transform(table, post_analytics)

        else:
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

    def _drop_duplicates(self, node, table):
        set_quantifier = node.set_quantifier.upper()
        if set_quantifier == 'ALL':
            return table

        elif set_quantifier == 'DISTINCT':
            return _dag.DropDuplicates(table)

        else:
            raise ValueError('unknown set quantifier {}'.format(set_quantifier))

    def _add_definitions(self, node, table):
        tables = [
            (cte.name, self.compile(cte.select))
            for cte in node.common_table_expressions
        ]
        return _dag.DefineTables(table, tables)

    def compile_from_clause(self, from_clause):
        if len(from_clause) == 0:
            raise ValueError("cannot handle empty from clauses")

        head, tail = from_clause[0], from_clause[1:]

        result = self._compile_single_table(head)

        for next in tail:
            next = self._compile_single_table(next)
            result = _dag.CrossJoin(result, next)

        return result

    def _compile_single_table(self, table):
        if isinstance(table, _parser.TableName):
            return _dag.GetTable(table.table, alias=table.alias)

        if isinstance(table, _parser.Select):
            return self.compile(table)

        assert isinstance(table, _parser.JoinedTable)

        result = self.compile_from_clause([table.table])

        for join in table.joins:
            right = self.compile_from_clause([join.table])

            if isinstance(join, _parser.Join):
                result = _dag.Join(result, right, how=join.how.lower(), on=join.on)

            elif isinstance(join, _parser.CrossJoin):
                result = _dag.CrossJoin(result, right)

            else:
                raise ValueError("unknown join {}".format(join))

        return result

    def _tmp_column(self):
        return '${}'.format(next(self.id_generator))


class SplitBaseTransformer(object):
    def __init__(self, id_generator):
        self.id_generator = id_generator

    def do_split(self, node):
        return self.split(node)

    def split(self, node):
        return call_handler(self, "split", node)

    def split_derived_column(self, node):
        value, agg, pre_aggs = self.split(node.value)
        return node.with_values(value=value), agg, pre_aggs

    def split_column_reference(self, node):
        return node, [], []

    def split_binary_expression(self, node):
        left_node, left_aggs, left_pre_aggs = self.split(node.left)
        right_node, right_aggs, right_pre_aggs = self.split(node.right)

        return (
            node.with_values(left=left_node, right=right_node),
            left_aggs + right_aggs,
            left_pre_aggs + right_pre_aggs,
        )

    def split_unary_expression(self, node):
        inner_node, aggs, pre_aggs = self.split(node.operand)
        return (node.with_values(operand=inner_node), aggs, pre_aggs)

    def split_function_call(self, node):
        args = []
        aggs = []
        pre_aggs = []

        for arg in node.arguments:
            self._split_append(arg, args, aggs, pre_aggs)

        return node.with_values(arguments=args), aggs, pre_aggs

    def split_integer(self, node):
        return node, [], []

    def split_float(self, node):
        return node, [], []

    def _tmp_column(self):
        return '${}'.format(next(self.id_generator))

    def _split_append(self, node, values, aggs, pre_aggs):
        t = self.split(node)
        values.append(t[0])
        aggs.extend(t[1])
        pre_aggs.extend(t[2])


class SplitAnalyticsFunctionTransformer(SplitBaseTransformer):
    def do_split(self, node):
        node, analytics, pre_analytics = self.split(node)

        assert isinstance(node, DerivedColumn)

        if not analytics:
            output = node.alias

            # chain the column along
            pre_analytics = []
            analytics = [node.with_values(alias=output)]
            node = DerivedColumn(value=_ref(output), alias=output)

            is_synthetic = True

        else:
            is_synthetic = False

        return is_synthetic, node, analytics, pre_analytics

    def split_general_set_function(self, node):
        value_node, aggs, pre_aggs = self.split(node.value)
        return node.with_values(value=value_node), aggs, pre_aggs

    def split_analytics_function(self, node):
        arguments = []
        aggs = []
        pre_aggs = []

        for child in node.function.arguments:
            self._split_append(child, arguments, aggs, pre_aggs)

        if aggs or pre_aggs:
            raise ValueError("multiple levels of analytics functions not allowed")

        agg_id = self._tmp_column()
        aggs = [DerivedColumn(node, alias=agg_id)]

        return _ref(agg_id), aggs, []


class SplitAggregateTransformer(SplitBaseTransformer):
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

    def split_analytics_function(self, node):
        func, aggs, pre_aggs = self.split(node.function)
        return node.with_values(function=func), aggs, pre_aggs


def default_generator():
    for i in it.count():
        yield str(i)


def _ref(col_name):
    return ColumnReference([col_name])
