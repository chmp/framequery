from __future__ import print_function, division, absolute_import

import collections

from ..parser import ast as a
from ..util._misc import match, ExactSequence, Any, InstanceOf
from ._util import all_unique
from ._pandas import PandasModel

import dask.dataframe as dd


class DaskModel(PandasModel):
    def transform(self, table, columns, name_generator):
        name_generator = name_generator.fix(all_unique(columns))

        meta = super(DaskModel, self).transform(table._meta_nonempty, columns, name_generator)
        meta = meta.iloc[:0]

        return dd.map_partitions(
            self.transform_partitions, table, columns, name_generator,

            # NOTE: pass empty_result as kw to prevent aligning it
            meta=meta, empty_result=meta,
        )

    def transform_partitions(self, df, columns, name_generator, empty_result):
        if not len(df):
            return empty_result

        return super(DaskModel, self).transform(df, columns, name_generator)

    def sort_values(self, table, names, ascending=False):
        raise NotImplementedError('dask does not yet support sorting')

    # TODO: unify impl between pandas / dask
    # TODO: add execution hints to allow group-by based aggregate?
    def aggregate(self, table, columns, group_by, name_generator):
        group_spec = [name_generator.get(col.alias) for col in group_by]

        agg_spec = {}
        rename__spec = []

        for col in columns:
            function = col.value.func
            arg = name_generator.get(col.value.args[0].name)
            alias = name_generator.get(col.alias)

            assert col.value.quantifier is None

            agg_spec.setdefault(arg, []).append(function)
            rename__spec.append((alias, (arg, function)))

        table = table.groupby(group_spec).aggregate(agg_spec)
        table = dd.map_partitions(self.rename_aggregation_results, table, rename__spec)
        table = table.reset_index(drop=False)

        return table

    def rename_aggregation_results(self, df, spec):
        df = df[[input_col for _, input_col in spec]]
        df.columns = [output_col for output_col, _ in spec]
        return df
