from __future__ import print_function, division, absolute_import

import pandas as pd


def ensure_table_columns(name, df):
    """Encode column and table names in the dataframe.
    """
    old_columns = list(df.columns)
    new_columns = list(_as_pair(name, col) for col in old_columns)

    return pd.DataFrame(
        _get_data(new_columns, old_columns, df),
        columns=pd.MultiIndex.from_tuples(new_columns),
        index=df.index,
    )


def strip_table_name_from_columns(df):
    old_columns = list(df.columns)
    new_columns = list(_as_name(col) for col in old_columns)

    return pd.DataFrame(
        _get_data(new_columns, old_columns, df),
        columns=new_columns,
        index=df.index,
    )


def _get_data(new_columns, old_columns, df):
    return {
        new_col: df[old_col]
        for (new_col, old_col) in zip(new_columns, old_columns)
    }


def _as_pair(key, obj):
    if not isinstance(obj, tuple):
        return (key, obj)

    return (key, obj[-1])


def _as_name(obj):
    if not isinstance(obj, tuple):
        return obj

    else:
        return obj[-1]
