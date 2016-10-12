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

    def _dataframe_from_scalars(self, values):
        return dataframe_from_scalars(values)

    def _frist(self, s):
        raise NotImplementedError()

    def _reset_index(self, df):
        # DASK does not support dropping the index, return unchanged
        columns = list(df.columns)
        df = df.reset_index()
        return df[columns]

def combine_series(items, how='inner'):
    """Helper function to combine mutliple series into a single dataframe.
    """
    item_to_frame = lambda item: item[1].to_frame(name=item[0])
    join_two_frames = lambda a, b: a.join(b, how=how)
    return ft.reduce(join_two_frames, map(item_to_frame, items))
