from __future__ import print_function, division, absolute_import

import collections
import hashlib

import dask.dataframe as dd
import pandas as pd
import toolz as tz


def dataframe_from_scalars(values, name=None):
    """Build a ``dd.DataFrame`` from a mapping of ``(key, dd.Scalar)`` pairs.
    """
    # make sure to preserve order accross all operations
    values = collections.OrderedDict(values)

    if name is None:
        parts = [(k, v.key) for (k, v) in values.items()]
        name = hashlib.md5(repr(parts).encode()).hexdigest()
        name = 'dataframe_from_scalars-{}'.format(name)

    divisions = [None, None]
    meta = dd.utils.make_meta([
        (k, values[k].dtype) for k in values.keys()
    ])

    dsk = tz.merge(value.dask for value in values.values())
    dsk[(name, 0)] = (
        _build_dataframe_from_series,
        list(values.keys()),
        [values[k].key for k in values.keys()]
    )

    return dd.DataFrame(dsk, name, meta, divisions)


def _as_dask(key, func, *args):
    return {key: (func,) + tuple(args)}


def _build_dataframe_from_series(keys, values):
    data = collections.OrderedDict(zip(keys, values))
    return pd.DataFrame(data, index=[0])


def scalar_from_python(obj):
    """Create a ``dd.Scalar`` from a python object.

    .. note::

        This function is quite inefficinet and should only be used in tests.
    """
    # NOTE: dd.Scalar is not document, therefore use dataframe route
    df = dd.from_pandas(pd.DataFrame({'x': [obj]}), npartitions=1)
    return df['x'].max()
