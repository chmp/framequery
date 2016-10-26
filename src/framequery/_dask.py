from __future__ import print_function, division, absolute_import

import collections
import functools as ft
import logging
import operator

import pandas as pd
import dask.dataframe as dd
import dask.dataframe.utils

from ._base_executor import BaseExecutor
from ._dask_util import dataframe_from_scalars
from ._expression import ExpressionEvaluator
from ._pandas_util import (
    as_pandas_join_condition,
    column_from_parts,
    column_set_table,
    cross_join,
    ensure_table_columns,
    get_col_ref,
)

_logger = logging.getLogger(__name__)


class DaskExecutor(BaseExecutor, ExpressionEvaluator):
    def __init__(self, id_generator=None, strict=False):
        super(DaskExecutor, self).__init__()

        if strict:
            raise ValueError("strict mode is not yet supported")

        self._set_id_generator(id_generator)

        self.functions['ABS'] = abs
        self.functions['POW'] = operator.pow

    def evaluate_transform(self, node, scope):
        _logger.debug("evaluate transform %s", node)
        table = self.evaluate(node.table, scope)

        table_id = next(self.id_generator)
        col_id_expr_pairs = [
            (column_from_parts(table_id, self._get_selected_column_name(col)), col.value)
            for col in node.columns
        ]
        meta = dd.utils.make_meta([
            (col_id, self._determine_dtype(expr, table))
            for (col_id, expr) in col_id_expr_pairs
        ])

        return table.map_partitions(ft.partial(transform_partitions, meta=meta), col_id_expr_pairs, meta=meta)

    def _determine_dtype(self, expr, table):
        # NOTE: since no compute is triggered, this operation is cheap
        return self.evaluate_value(expr, table).dtype

    def evaluate_sort(self, node, scope):
        # TODO: implement sorting of dataframes
        raise NotImplementedError("sort of dask dataframes not supported")

    def evaluate_limit(self, node, scope):
        # TODO: implement limiting
        # NOTE: dask head() **does not** return the global head
        raise NotImplementedError("limit of dask dataframes not supported")

    def _combine_series(self, result):
        return combine_series(result.items())

    def _dataframe_from_scalars(self, values):
        return dataframe_from_scalars(values)

    def _frist(self, s):
        raise NotImplementedError()

    def _reset_index(self, df):
        # DASK does not support dropping the index, return unchanged
        columns = list(df.columns)
        df = df.reset_index()
        return df[columns]

    def _merge(self, left, right, how, left_on, right_on):
        result = left.merge(right, how=how, left_on=left_on, right_on=right_on)

        # work around dask bug in single-parition merge
        result.divisions = [None for _ in result.divisions]
        return result

    def _evaluate_aggregation_grouped(self, node, table):
        # TODO: use .aggregate if available
        table_id = next(self.id_generator)

        meta = self._evaluate_aggregation_base(node, table, table.columns, table_id=table_id)
        meta = [(k, v.dtype) for (k, v) in meta.items()]

        # TODO: clean this up
        groupby_meta = [
            (column_set_table('.'.join(col.value), table_id),
             table.dtypes[get_col_ref(table.columns, col.value[-1])])
            for col in node.group_by
        ]

        meta = groupby_meta + meta
        meta = dd.utils.make_meta(meta)
        group_cols = self._get_group_columns(table, node.group_by)

        assert len(group_cols) == 1

        res = (
            table.groupby(group_cols[0])
            .apply(ft.partial(aggregate_partitions, self, node, table_id, group_cols), meta)
            #.reset_index()
        )
        return res


def transform_partitions(df, col_id_expr_pairs, meta):
    if not len(df):
        return meta

    ex = ExpressionEvaluator()
    ex.functions['ABS'] = abs
    ex.functions['POW'] = operator.pow

    result = collections.OrderedDict()

    for col_id, expr in col_id_expr_pairs:
        result[col_id] = ex.evaluate_value(expr, df)

    return pd.DataFrame(result, index=df.index)


def aggregate_partitions(ex, node, table_id, group_cols, df):
    columns = df.columns
    df = df.groupby(group_cols)
    res = ex._evaluate_aggregation_base(node, df, columns, table_id=table_id)
    df = pd.DataFrame(res)
    df = df.reset_index()
    df.columns = [column_set_table(col, table_id) for col in df.columns]
    return df


def combine_series(items, how='inner'):
    """Helper function to combine mutliple series into a single dataframe.
    """
    item_to_frame = lambda item: item[1].to_frame(name=item[0])
    join_two_frames = lambda a, b: a.join(b, how=how)
    return ft.reduce(join_two_frames, map(item_to_frame, items))
