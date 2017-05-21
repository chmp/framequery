from __future__ import print_function, division, absolute_import

import functools as ft
import os.path

from ._util import all_unique
from ._pandas import PandasModel

import dask.dataframe as dd


class DaskModel(PandasModel):
    def __init__(self, **kwargs):
        super(DaskModel, self).__init__(**kwargs)

        self.lateral_functions = dict(self.lateral_functions)

        self.table_functions = {
            k: to_dd_table_function(self.table_functions[k])
            for k in {'json_each', 'json_array_elements'}
        }

        self.table_functions.update(
            copy_from=copy_from,
        )

    def transform(self, table, columns, name_generator):
        name_generator = name_generator.fix(all_unique(columns))

        meta = super(DaskModel, self).transform(table._meta_nonempty, columns, name_generator)
        meta = meta.iloc[:0]

        return dd.map_partitions(
            self.transform_partitions, table, columns, name_generator,

            # NOTE: pass empty_result as kw to prevent aligning it
            meta=meta, empty_result=meta,
        )

    def dual(self):
        return dd.from_pandas(super(DaskModel, self).dual(), npartitions=1)

    def transform_partitions(self, df, columns, name_generator, empty_result):
        if not len(df):
            return empty_result

        return super(DaskModel, self).transform(df, columns, name_generator)

    def sort_values(self, table, names, ascending=False):
        raise NotImplementedError('dask does not yet support sorting')

    def select_rename(self, df, spec):
        return dd.map_partitions(super(DaskModel, self).select_rename, df, spec)

    def copy_to(self, scope, name, filename, options):
        raise NotImplementedError()

    def compute(self, val):
        return val.compute()


def to_dd_table_function(pd_func, npartitions=20):
    @ft.wraps(pd_func)
    def impl(*args, **kwargs):
        df = pd_func(*args, **kwargs)
        return dd.from_pandas(df, npartitions=npartitions)

    return impl


def copy_from(filename, *args):
    options = dict(zip(args[:-1:2], args[1::2]))

    format = options.pop('format', 'csv')

    if format == 'csv':
        filename = os.path.abspath(filename)

        if 'delimiter' in options:
            options['sep'] = options.pop('delimiter')

        return dd.read_csv(filename, **options)

    else:
        raise RuntimeError('unknown format %s' % format)
