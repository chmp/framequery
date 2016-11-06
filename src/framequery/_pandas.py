"""Support for running a DAG on pandas dataframes
"""
from __future__ import print_function, division, absolute_import

import logging

import pandas as pd
import six

from . import _dag
from ._base_executor import BaseExecutor
from ._expression import ExpressionEvaluator
from ._pandas_util import cross_join

_logger = logging.getLogger(__name__)


class PandasExecutor(BaseExecutor, ExpressionEvaluator):
    def __init__(self, id_generator=None, strict=False):
        super(PandasExecutor, self).__init__()

        self._set_id_generator(id_generator)
        self._set_strict(strict)

    def _get_dual(self):
        return pd.DataFrame(index=[0])

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

    def _dataframe_from_scalars(self, values):
        return pd.DataFrame(values, index=[0])

    def _merge(self, left, right, how, left_on, right_on):
        return pd.merge(left, right, how=how, left_on=left_on, right_on=right_on)


def _string_pair(t):
    a, b = t
    return six.text_type(a), six.text_type(b)


def _get(obj, tuple_key):
    if isinstance(obj, pd.DataFrame):
        return obj[tuple_key]

    return obj[tuple_key, ]


def _get_null_marker_name(col):
    return '$$.{}'.format(col)
