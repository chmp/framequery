from __future__ import print_function, division, absolute_import

import collections
import functools as ft
import logging
import operator

import pandas as pd

import dask
import dask.base as db
import dask.dataframe as dd
import dask.dataframe.core as ddc
import dask.dataframe.utils as ddu

from ._parser import ColumnReference
from ._base_executor import BaseExecutor
from ._dask_util import dataframe_from_scalars
from ._expression import ExpressionEvaluator
from ._pandas_util import (
    column_from_parts,
    normalize_col_ref,
    column_set_table,
)
from ._util import version

_logger = logging.getLogger(__name__)


class DaskExecutor(BaseExecutor, ExpressionEvaluator):
    def __init__(self, id_generator=None, strict=False):
        super(DaskExecutor, self).__init__()

        if strict:
            raise ValueError("strict mode is not yet supported")

        self._set_id_generator(id_generator)

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
        table_id = next(self.id_generator)
        columns = table.columns

        group_by = [normalize_col_ref(ref.value, columns) for ref in node.group_by]
        meta = self._evaluate_aggregation_base(
            node, table._meta.groupby(group_by), columns, table_id=table_id
        )
        meta = pd.DataFrame(meta).reset_index()
        meta = ddu.make_meta(meta)

        aca_kwargs = dict(
            chunk=aggregate_chunk,
            aggregate=aggregate_agg,
            meta=meta,
            token='framequery-aggregate',
            chunk_kwargs=dict(node=node, table_id=table_id),
            aggregate_kwargs=dict(node=node, table_id=table_id),
        )

        # disable tree reductions if available
        if version(dask.__version__) >= version('0.12.0'):
            aca_kwargs.update(split_every=False)

        return ddc.apply_concat_apply(table, **aca_kwargs)


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


def aggregate_chunk(df, node, table_id):
    group_by = [normalize_col_ref(ref.value, df.columns) for ref in node.group_by]
    grouped = df.groupby(group_by)

    result = collections.OrderedDict()

    for col in node.columns:
        agg_node = col.value

        function = agg_node.function.upper()
        value = agg_node.value

        if not isinstance(value, ColumnReference):
            raise ValueError("indirect aggregations not supported")

        col_ref = normalize_col_ref(value.value, df.columns)
        col_id = db.tokenize(function, value.value)

        if function == 'SUM':
            result[col_id] = grouped[col_ref].sum()

        elif function == 'MIN':
            result[col_id] = grouped[col_ref].min()

        elif function == 'MAX':
            result[col_id] = grouped[col_ref].max()

        elif function == 'FIRST_VALUE':
            result[col_id] = grouped[col_ref].apply(lambda s: s.iloc[0])

        elif function == 'COUNT':
            result[col_id] = grouped[col_ref].count()

        elif function == 'AVG':
            result['sum-' + col_id] = grouped[col_ref].sum()
            result['count-' + col_id] = grouped[col_ref].count()

        else:
            raise NotImplementedError()

    return pd.DataFrame(result)


def aggregate_agg(df, node, table_id):
    levels = 0 if len(node.group_by) == 1 else list(range(len(node.group_by)))
    grouped = df.groupby(level=levels)

    result = collections.OrderedDict()

    for col in node.columns:
        agg_node = col.value

        function = agg_node.function.upper()
        value = agg_node.value

        if not isinstance(value, ColumnReference):
            raise ValueError("indirect aggregations not supported")

        col_id = db.tokenize(function, value.value)
        res_id = column_from_parts(table_id, col.alias)

        if function == 'SUM':
            result[res_id] = grouped[col_id].sum()

        elif function == 'MIN':
            result[res_id] = grouped[col_id].min()

        elif function == 'MAX':
            result[res_id] = grouped[col_id].max()

        elif function == 'FIRST_VALUE':
            result[res_id] = grouped[col_id].apply(lambda s: s.iloc[0])

        elif function == 'COUNT':
            result[res_id] = grouped[col_id].sum()

        elif function == 'AVG':
            # TODO: handle tree aggregations correctly
            result[res_id] = (grouped['sum-' + col_id].sum() /
                              grouped['count-' + col_id].sum())

        else:
            raise NotImplementedError()

    result = pd.DataFrame(result)
    result = result.reset_index()
    result.columns = [column_set_table(col, table_id) for col in result.columns]
    return result


def combine_series(items, how='inner'):
    """Helper function to combine mutliple series into a single dataframe.
    """
    def item_to_frame(item):
        return item[1].to_frame(name=item[0])

    def join_two_frames(a, b):
        return a.join(b, how=how)

    return ft.reduce(join_two_frames, map(item_to_frame, items))
