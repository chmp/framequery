from __future__ import print_function, division, absolute_import

from ._util import column_set_table, column_get_column, normalize_col_ref
from ..parser import ast as a
from ..util import _monadic as m
from ..util import like, not_like

import collections
import operator

import pandas as pd


class PandasModel(object):
    def __init__(self, debug=False):
        self._debug = debug
        self.eval = eval_pandas

        self.aggregates = {
            'sum': lambda col: col.sum(),
            'avg': lambda col: col.mean(),
            'min': lambda col: col.min(),
            'max': lambda col: col.max(),
            'count': lambda col: col.count(),

            # TODO: add first_value again
            # 'first_value': self._first,
        }

    def debug(self, msg, *args, **kwargs):
        if self._debug:
            print(msg.format(*args, **kwargs))

    def call(self, *args, **kwargs):
        obj, method = args[:2]
        args = args[2:]
        return getattr(obj, method)(*args, **kwargs)

    def __getattr__(self, name):
        def caller(*args, **kwargs):
            obj, = args[:1]
            args = args[1:]
            return self.call(obj, name, *args, **kwargs)

        return caller

    def get_table(self, scope, name, alias=None):
        table = scope[name]
        return self.add_table_to_columns(table, alias) if alias is not None else table

    @staticmethod
    def add_table_to_columns(df, table_name):
        return df.rename(columns=lambda c: column_set_table(c, table_name))

    @staticmethod
    def remove_table_from_columns(df):
        return df.rename(columns=column_get_column)

    def evaluate(self, df, expr, name_generator):
        return self.eval(expr, df, name_generator)

    def transform(self, table, columns, name_generator):
        result = collections.OrderedDict()

        for col in columns:
            if isinstance(col, a.InternalName):
                result[col.name] = table[col.name]

            else:
                alias = name_generator.get(col.alias)
                result[alias] = self.evaluate(table, col.value, name_generator)

        return pd.DataFrame(result, index=table.index)

    # TODO: add execution hints to allow group-by-apply based aggregate?
    def aggregate(self, table, columns, group_by, name_generator):
        group_spec = [name_generator.get(col.alias) for col in group_by]

        agg_spec = {}
        rename_spec = []

        for col in columns:
            function = col.value.func
            arg = name_generator.get(col.value.args[0].name)
            alias = name_generator.get(col.alias)

            assert col.value.quantifier is None

            agg_spec.setdefault(arg, []).append(function)
            rename_spec.append((alias, (arg, function)))

        table = table.groupby(group_spec).aggregate(agg_spec)
        table = self.select_rename(table, rename_spec)
        table = table.reset_index(drop=False)
        return table

    def select_rename(self, df, spec):
        df = df[[input_col for _, input_col in spec]]
        df.columns = [output_col for output_col, _ in spec]
        return df


eval_pandas = m.RuleSet(name='eval_pandas')


@eval_pandas.rule(m.instanceof(a.Name))
def eval_pandas_name(_, expr, df, name_generator):
    name = name_generator.get(expr.name)
    col = normalize_col_ref(name, df.columns)
    return df[col]


@eval_pandas.rule(m.instanceof(a.Integer))
def eval_integer(_, expr, *__):
    return int(expr.value)


@eval_pandas.rule(m.instanceof(a.BinaryOp))
def eval_pandas_binary_op(eval_pandas, expr, df, name_generator):
    left = eval_pandas(expr.left, df, name_generator)
    right = eval_pandas(expr.right, df, name_generator)

    operator_map = {
        '*': operator.mul,
        '/': operator.truediv,
        '+': operator.add,
        '-': operator.sub,
        '||': operator.add,  # TODO: add type test for string?
        'and': operator.and_,
        'or': operator.or_,
        '<': operator.lt,
        '>': operator.gt,
        '<=': operator.le,
        '>=': operator.ge,
        '=': operator.eq,
        '!=': operator.ne,
        'like': like,
        'not like': not_like,
        'in': lambda a, b: a.isin(b),
        'not in': lambda a, b: ~(a.isin(b)),
    }

    try:
        op = operator_map[expr.op]

    except KeyError:
        raise ValueError("unknown operator {}".format(expr.op))

    else:
        return op(left, right)
