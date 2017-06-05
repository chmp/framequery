from __future__ import print_function, division, absolute_import

import functools as ft
import os.path

import dask.dataframe as dd
import pandas as pd

from ._util import all_unique
from ._pandas import PandasModel

from ..util import dask_sort_values, dask_offset_limit


class DaskModel(PandasModel):
    """A framequery model for ``dask.dataframe.DataFrame`` objects.

    For a list of keyword arguments see :class:`framequery.PandasModel`.

    The dask executor supports scopes with both pandas and dask dataframes.
    The former will be converted into later automatically, as needed.
    """
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

    def transform_partitions(self, df, columns, name_generator, empty_result):
        if not len(df):
            return empty_result

        return super(DaskModel, self).transform(df, columns, name_generator)

    def add_columns(self, table, columns, name_generator):
        name_generator = name_generator.fix(all_unique(columns))

        meta = super(DaskModel, self).add_columns(table._meta_nonempty, columns, name_generator)
        meta = meta.iloc[:0]

        return dd.map_partitions(
            self.add_columns_partitions, table, columns, name_generator,

            # NOTE: pass empty_result as kw to prevent aligning it
            meta=meta, empty_result=meta,
        )

    def add_columns_partitions(self, df, columns, name_generator, empty_result):
        if not len(df):
            return empty_result

        return super(DaskModel, self).add_columns(df, columns, name_generator)

    def get_table(self, scope, name, alias=None):
        if name in self.special_tables:
            return self.get_special_table(scope, name, alias)

        table = super(DaskModel, self).get_table(scope, name, alias)
        if isinstance(table, pd.DataFrame):
            return dd.from_pandas(table, npartitions=20)

        return table

    def get_special_table(self, scope, name, alias):
        return dd.from_pandas(
            super(DaskModel, self).get_special_table(scope, name, alias),
            npartitions=1,
        )

    def filter_table(self, table, expr, name_generator):
        name_generator = name_generator.fix(all_unique(expr))
        return dd.map_partitions(super(DaskModel, self).filter_table, table, expr, name_generator)

    def dual(self):
        return dd.from_pandas(super(DaskModel, self).dual(), npartitions=1)

    def sort_values(self, table, names, ascending=False):
        return dask_sort_values(table, names, ascending)

    def select_rename(self, df, spec):
        return dd.map_partitions(super(DaskModel, self).select_rename, df, spec)

    def copy_to(self, scope, name, filename, options):
        raise NotImplementedError()

    def compute(self, val):
        return val.compute()

    def lateral(self, table, name_generator, func, args, alias):
        func = func.lower()
        if func not in self.lateral_functions:
            raise ValueError('unknown lateral function %s' % func)

        if func not in self.lateral_meta:
            raise ValueError('unknown meta for lateral function %s' % func)

        alias = name_generator.get(alias)
        name_generator = name_generator.fix(all_unique(args))

        meta = pd.concat([
            table._meta,
            self.add_table_to_columns(self.lateral_meta[func], alias),
        ], axis=1)

        return dd.map_partitions(
            self.lateral_partitions, table, name_generator, func, args, alias,

            # NOTE: pass empty_result as kw to prevent aligning it
            meta=meta, empty_result=meta,
        )

    def lateral_partitions(self, table, name_generator, func, args, alias, empty_result):
        if not len(table):
            return empty_result

        return super(DaskModel, self).lateral(table, name_generator, func, args, alias)

    def limit_offset(self, table, limit=None, offset=None):
        return dask_offset_limit(table, limit=limit, offset=offset)


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
