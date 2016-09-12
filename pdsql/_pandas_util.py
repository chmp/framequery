from __future__ import print_function, division, absolute_import

import itertools as it

import pandas as pd

from .parser import BinaryExpression, ColumnReference


def ensure_table_columns(name, df):
    """Encode column and table names in the dataframe.
    """
    if len(df.columns) == 0:
        return df

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


def as_pandas_join_condition(left_columns, right_columns, condition):
    flat_condition = flatten_join_condition(condition)

    left = []
    right = []

    for a, b in flat_condition:
        a_is_left, a = _is_left(left_columns, right_columns, a)
        b_is_left, b = _is_left(left_columns, right_columns, b)

        if a_is_left == b_is_left:
            raise ValueError("cannot join a table to itslef ({}, {})".format(a, b))

        if a_is_left:
            left.append(a)
            right.append(b)

        else:
            right.append(a)
            left.append(b)

    return left, right


def is_scalar(obj):
    return not hasattr(obj, 'shape')


def _is_left(left_columns, right_columns, ref):
    left_ref = get_col_ref(left_columns, ref)
    right_ref = get_col_ref(right_columns, ref)

    if (left_ref is None) == (right_ref is None):
        raise ValueError('col ref {} is ambigious'.format(ref))

    return (left_ref is not None), left_ref if left_ref is not None else right_ref


def get_col_ref(columns, ref):
    for col in columns:
        if all(t == u for (t, u) in zip(reversed(col), reversed(ref))):
            return col

    return None


def flatten_join_condition(condition):
    return list(_flatten_join_condition(condition))


def _flatten_join_condition(condition):
    if not isinstance(condition, BinaryExpression):
        raise ValueError("can only handle equality joins")

    if condition.operator == 'AND':
        return it.chain(
            _flatten_join_condition(condition.left),
            _flatten_join_condition(condition.right),
        )

    elif condition.operator == '=':
        if not (
            isinstance(condition.left, ColumnReference) and
            isinstance(condition.right, ColumnReference)
        ):
            raise ValueError("requires column references")

        return [(condition.left.value, condition.right.value)]

    else:
        raise ValueError("can only handle equality joins")


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
