"""Support for running a DAG on pandas dataframes
"""
from __future__ import print_function, division, absolute_import

import collections
import itertools as it
import logging
import operator

import pandas as pd
import six

from . import _dag
from ._base_executor import BaseExecutor
from ._expression import ExpressionEvaluator
from ._pandas_util import (
    as_pandas_join_condition,
    column_from_parts,
    cross_join,
    ensure_table_columns,
    is_equality_join,
    is_scalar,
)
from ._parser import GeneralSetFunction, ColumnReference
from ._util.executor import default_id_generator

_logger = logging.getLogger(__name__)


class PandasExecutor(BaseExecutor, ExpressionEvaluator):
    def __init__(self, id_generator=None, strict=False):
        super(PandasExecutor, self).__init__()

        self._set_id_generator(id_generator)
        self._set_strict(strict)

        self.functions['ABS'] = abs
        self.functions['POW'] = operator.pow

    def _get_dual(self):
        return pd.DataFrame()

    def _combine_series(self, result):
        all_scalar = all(is_scalar(val) for val in result.values())
        return pd.DataFrame(result) if not all_scalar else pd.DataFrame(result, index=[0])

    def evaluate_define_tables(self, node, scope):
        scope = scope.copy()

        for name, sub_node in node.tables:
            scope[name] = self.evaluate(sub_node, scope)

        return self.evaluate(node.node, scope)

    def evaluate_literal(self, node, _):
        return node.value

    def evaluate_join(self, node, scope):
        if not is_equality_join(node.on):
            return self._evaluate_non_equality_join(node, scope)

        left = self.evaluate(node.left, scope)
        right = self.evaluate(node.right, scope)

        assert node.how in {'inner', 'outer', 'left', 'right'}
        left_on, right_on = as_pandas_join_condition(left.columns, right.columns, node.on)

        result = pd.merge(left, right, how=node.how, left_on=left_on, right_on=right_on)

        if self.strict:
            subdag = _dag.Filter(_dag.Literal(result), node.on)
            return self.evaluate(subdag, scope)

        return result

    def _evaluate_non_equality_join(self, node, scope):
        """Replace inner joins with a non-equality condition by a cross join with filter.
        """
        if node.how != 'inner':
            raise ValueError("can only handle non equality conditions for inner joins")

        # TODO: optimize to use inner join with equality condition if possible
        subdag = _dag.Filter(_dag.CrossJoin(node.left, node.right), node.on)
        return self.evaluate(subdag, scope)

    def evaluate_cross_join(self, node, scope):
        _logger.warning("evaluating cross join, possible performance problem")
        left = self.evaluate(node.left, scope)
        right = self.evaluate(node.right, scope)

        return cross_join(left, right)

    def evaluate_aggregate(self, node, scope):
        table = self.evaluate(node.table, scope)

        if node.group_by is None:
            return self._evaluate_aggregation_non_grouped(node, table)

        else:
            return self._evaluate_aggregation_grouped(node, table)

    def evaluate_filter(self, node, scope):
        table = self.evaluate(node.table, scope)
        condition = self.evaluate_value(node.filter, table)
        table = table[condition]
        return table.reset_index(drop=True)

    def evaluate_sort(self, node, scope):
        table = self.evaluate(node.table, scope)
        values, ascending = self._split_order_by_items(node.values, table)
        table = table.sort_values(values, ascending=ascending)
        return table.reset_index(drop=True)

    def evaluate_limit(self, node, scope):
        table = self.evaluate(node.table, scope)
        table = table.iloc[node.offset:node.offset + node.limit]
        return table.reset_index(drop=True)

    def _evaluate_aggregation_grouped(self, node, table):
        group_cols = self._get_group_columns(table, node.group_by)
        table, groupby = self._add_null_markers(table, group_cols)

        grouped = table.groupby(groupby)
        result = self._evaluate_aggregation_base(node, grouped, table.columns)

        df = pd.DataFrame(result)
        df = df.reset_index()
        df = self._strip_null_markers(df, group_cols)
        return df

    def _get_group_columns(self, table, group_by):
        if not all(isinstance(obj, ColumnReference) for obj in group_by):
            raise ValueError("indirect group-bys not supported")

        return [self._normalize_col_ref(ref.value, table.columns) for ref in group_by]

    def _add_null_markers(self, df, group_cols):
        """Add null markers to use SQL null semantics in groupby.

        .. note::

            If null markers are added, a copied is made of the original
            dataframe. Leaving the input argument unchanged.
        """
        if not self.strict:
            return df, list(group_cols)

        # TODO: minimize memory overhead for non groupby columns
        df = df.copy()
        groupby = []

        for col in group_cols:
            null_marker = _get_null_marker_name(col)
            df[null_marker] = df[col].isnull()
            df[col].fillna(0, inplace=True)
            groupby.extend((col, null_marker))

        return df, groupby

    def _strip_null_markers(self, df, group_cols):
        """Remove any null markers added to use SQL null semantics in groupby.

        .. note::

            The dataframe is modified in-place.
        """
        if not self.strict:
            return df

        for col in group_cols:
            null_marker = _get_null_marker_name(col)
            sel = df[null_marker]
            df.loc[sel, col] = None
            del df[null_marker]

        return df

    def _evaluate_aggregation_non_grouped(self, node, table):
        return pd.DataFrame(
            self._evaluate_aggregation_base(node, table, table.columns),
            index=[0],
        )

    def _evaluate_aggregation_base(self, node, table, columns):
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
        col = _get(table, col_ref)

        # TODO: handle set quantifiers
        assert node.quantifier is None

        impls = {
            'SUM': lambda col: col.sum(),
            'AVG': lambda col: col.mean(),
            'MIN': lambda col: col.min(),
            'MAX': lambda col: col.max(),
            'COUNT': lambda col: col.count(),
            'FIRST_VALUE': _first,
        }

        try:
            impl = impls[function]

        except KeyError:
            raise ValueError("unknown aggregation function {}".format(function))

        else:
            result = impl(col)

        if isinstance(result, pd.DataFrame):
            return result[col_ref]

        return result


def _string_pair(t):
    a, b = t
    return six.text_type(a), six.text_type(b)


def _get(obj, tuple_key):
    if isinstance(obj, pd.DataFrame):
        return obj[tuple_key]

    return obj[tuple_key,]


def _first(s):
    if isinstance(s, pd.Series):
        return s.iloc[0]

    return s.first()


def _get_null_marker_name(col):
    return '$$.{}'.format(col)
