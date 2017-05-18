from __future__ import print_function, division, absolute_import

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

    def select_rename(self, df, spec):
        return dd.map_partitions(super(DaskModel, self).select_rename, df, spec)

    def copy_from(self, scope, name, filename, options):
        raise NotImplementedError()

    def copy_to(self, scope, name, filename, options):
        raise NotImplementedError()

    def eval_table_valued(self, node, scope):
        raise NotImplementedError()
