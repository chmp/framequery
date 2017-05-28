"""Helpers to implement missing dask functionality."""
from __future__ import print_function, division, absolute_import

import itertools as it
import functools as ft

import dask
import dask.dataframe as dd
from dask.base import tokenize

import pandas as pd


def dask_sort_values(df, by, ascending=True):
    """Sort a dataframe with an even-odd block sort."""
    sort_values_kwargs = dict(by=by, ascending=ascending)

    if df.npartitions == 1:
        return df.map_partitions(sort_dataframes, **sort_values_kwargs)

    dsk = dict(df.dask)
    name = df._name

    n_iterations = 2 * (1 + df.npartitions // 2)
    for iteration, offset in zip(range(n_iterations), it.cycle([0, 1])):
        name, dsk_update = sort_values_step(name, df.npartitions, offset, sort_values_kwargs)
        dsk.update(dsk_update)

    return dd.DataFrame(dsk, name, df._meta, divisions=(None,) * (1 + df.npartitions))


def sort_values_step(input_name, npartitions, offset, sort_values_kwargs):
    dsk = {}
    output_name = 'sort_values-{}'.format(tokenize(input_name, sort_values_kwargs))
    merge_name = 'sort_values_merge-{}'.format(tokenize(input_name, sort_values_kwargs))

    sorter = ft.partial(sort_dataframes, **sort_values_kwargs)

    for idx in range(offset):
        dsk[output_name, idx] = lambda x: x, (input_name, idx)

    for a, b in zip(range(offset + 0, npartitions, 2), range(offset + 1, npartitions, 2)):
        dsk[merge_name, a] = sorter, (input_name, a), (input_name, b)
        dsk[output_name, a] = lower_half, (merge_name, a)
        dsk[output_name, b] = upper_half, (merge_name, a)

    consumed = offset + 2 * ((npartitions - offset) // 2)

    for idx in range(consumed, npartitions):
        dsk[output_name, idx] = lambda x: x, (input_name, idx)

    return output_name, dsk


def sort_dataframes(*dataframes, **kwargs):
    df = pd.concat(list(dataframes), axis=0, ignore_index=True)
    return df.sort_values(**kwargs)


def lower_half(ab):
    return ab.iloc[:ab.shape[0] // 2]


def upper_half(ab):
    return ab.iloc[ab.shape[0] // 2:]


def dask_offset_limit(df, offset, limit):
    """Perform a limit-offset operation against a dataframe."""
    parts = df.to_delayed()

    lens = [dask.delayed(len)(part) for part in parts]
    lens = dask.delayed(as_list)(*lens)
    parts = [
        dask.delayed(select_subset)(idx, part, lens, offset, limit, df._meta)
        for idx, part in enumerate(parts)
    ]

    return dd.from_delayed(parts, meta=df._meta)


def select_subset(idx, part, lens, offset, limit, empty_df):
    if offset is None:
        offset = 0

    if limit is None:
        limit = sum(lens)

    part_start = sum(l for l in lens[:idx])
    part_end = part_start + len(part)

    if part_end <= offset or part_start > (offset + limit):
        return empty_df

    start_in_part = max(0, offset - part_start)
    end_in_part = max(0, offset + limit - part_start)
    return part.iloc[start_in_part:end_in_part]


def as_list(*vals):
    return list(vals)


