from __future__ import print_function, division, absolute_import

import collections
import functools as ft
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
    cross_join,
    ensure_table_columns,
)


class DaskExecutor(BaseExecutor, ExpressionEvaluator):
    def __init__(self, id_generator=None, strict=False):
        super(DaskExecutor, self).__init__()

        if strict:
            raise ValueError("strict mode is not yet supported")

        self._set_id_generator(id_generator)

        self.functions['ABS'] = abs
        self.functions['POW'] = operator.pow

    def evaluate_transform(self, node, scope):
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

        return table.map_partitions(transform_partitions, col_id_expr_pairs, meta=meta)

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


def transform_partitions(df, col_id_expr_pairs):
    ex = ExpressionEvaluator()
    ex.functions['ABS'] = abs
    ex.functions['POW'] = operator.pow

    result = collections.OrderedDict()

    for col_id, expr in col_id_expr_pairs:
        result[col_id] = ex.evaluate_value(expr, df)

    return pd.DataFrame(result, index=df.index)


def combine_series(items, how='inner'):
    """Helper function to combine mutliple series into a single dataframe.
    """
    item_to_frame = lambda item: item[1].to_frame(name=item[0])
    join_two_frames = lambda a, b: a.join(b, how=how)
    return ft.reduce(join_two_frames, map(item_to_frame, items))
