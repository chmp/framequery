from __future__ import print_function, division, absolute_import

import collections
import functools as ft
import operator

from ._base_executor import BaseExecutor
from ._dask_util import dataframe_from_scalars
from ._expression import ExpressionEvaluator
from ._pandas_util import (
    as_pandas_join_condition,
    column_from_parts,
    cross_join,
    ensure_table_columns,
)
from ._parser import GeneralSetFunction, ColumnReference


# TODO: implement transforms via map_partitions to avoid join overhead
class DaskExecutor(BaseExecutor, ExpressionEvaluator):
    def __init__(self, id_generator=None, strict=False):
        super(DaskExecutor, self).__init__()

        if strict:
            raise ValueError("strict mode is not yet supported")

        self._set_id_generator(id_generator)

        self.functions['ABS'] = abs
        self.functions['POW'] = operator.pow

    def _combine_series(self, result):
        return combine_series(result.items())

    def evaluate_aggregate(self, node, scope):
        table = self.evaluate(node.table, scope)
        columns = table.columns
        table_id = next(self.id_generator)
        result = collections.OrderedDict()

        for col in node.columns:
            col_id = col.alias if col.alias is not None else next(self.id_generator)
            result[column_from_parts(table_id, col_id)] = self._agg(col.value, table, columns)

        return dataframe_from_scalars(result)

    def _agg(self, node, table, columns):
        if not isinstance(node, GeneralSetFunction):
            raise ValueError("indirect aggregations not supported")

        function = node.function.upper()
        value = node.value

        if not isinstance(value, ColumnReference):
            raise ValueError("indirect aggregations not supported")

        col_ref = self._normalize_col_ref(value.value, columns)
        col = table[col_ref]

        # TODO: handle set quantifiers
        assert node.quantifier is None

        impls = {
            'SUM': lambda col: col.sum(),
            'AVG': lambda col: col.mean(),
            'MIN': lambda col: col.min(),
            'MAX': lambda col: col.max(),
            'COUNT': lambda col: col.count(),
        }

        try:
            impl = impls[function]

        except KeyError:
            raise ValueError("unknown aggregation function {}".format(function))

        else:
            result = impl(col)

        print(result)
        return result


def combine_series(items, how='inner'):
    """Helper function to combine mutliple series into a single dataframe.
    """
    item_to_frame = lambda item: item[1].to_frame(name=item[0])
    join_two_frames = lambda a, b: a.join(b, how=how)
    return ft.reduce(join_two_frames, map(item_to_frame, items))
