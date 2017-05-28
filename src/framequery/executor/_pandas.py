from __future__ import print_function, division, absolute_import

from ._executor import Model
from ._util import (
    as_pandas_join_condition,
    column_from_parts,
    column_get_column,
    column_set_table,
    eval_string_literal,
    normalize_col_ref,
)
from ..parser import ast as a
from ..util import _monadic as m, like, not_like, make_meta

from .. import util

import collections
import contextlib
import logging
import operator
import os.path

import pandas as pd

_logger = logging.getLogger(__name__)


class PandasModel(Model):
    """A framequery model for ``pandas.DataFrame`` objects.

    :param str basepath:
        the path relative to which any ``copy from`` and ``copy to`` statements
        are interpreted.

    :param bool strict:
        if True, mimic SQL behavior in group-by and join.

    """
    def __init__(self, basepath='.', strict=False):
        self.strict = strict
        self.eval = eval_pandas
        self.basepath = basepath

        self.functions = {
            'version': lambda: 'PostgreSQL 9.6.0',
            'current_schema': lambda: 'public',
            'concat': util.concat,
            'lower': util.lower,
            'position': util.position,
            'trim': util.trim,
            'upper': util.upper,
        }

        self.table_functions = {
            'copy_from': util.copy_from,
            'json_each': util.json_each,
            'json_array_elements': util.json_array_elements,
        }

        self.lateral_functions = self.table_functions

        self.lateral_meta = {
            'json_each': make_meta([
                ('key', object),
                ('value', object),
            ]),
            'json_array_elements': make_meta([
                ('value', object),
            ])
        }

        self.special_tables = {'pg_namespace'}

    @contextlib.contextmanager
    def with_basepath(self, basepath):
        old_basepath = self.basepath

        try:
            self.basepath = basepath
            yield self

        finally:
            self.basepath = old_basepath

    def dual(self):
        """Return an empty single-row dataframe."""
        return pd.DataFrame({}, index=[0])

    def get_table(self, scope, name, alias=None):
        if name in self.special_tables:
            return self.get_special_table(scope, name, alias)

        if alias is None:
            alias = name

        table = scope[name]
        return self.add_table_to_columns(table, alias)

    def get_special_table(self, scope, name, alias):
        if alias is None:
            alias = name

        if name == 'pg_namespace':
            result = pd.DataFrame()
            result[column_from_parts('pg_namespace', 'nspname')] = ['public', 'pg_catalog', 'information_schema']
            result[column_from_parts('pg_namespace', 'nspowner')] = [0, 0, 0]
            result[column_from_parts('pg_namespace', 'nspacl')] = pd.Series([None, None, None], dtype=object)

            return result

        else:
            raise ValueError('unknown special table %s')

    def filter_table(self, table, expr, name_generator):
        sel = self.evaluate(table, expr, name_generator)
        return table[sel]

    @staticmethod
    def add_table_to_columns(df, table_name):
        return df.rename(columns=lambda c: column_set_table(c, table_name))

    @staticmethod
    def remove_table_from_columns(df):
        return df.rename(columns=column_get_column)

    def evaluate(self, df, expr, name_generator):
        return self.eval(expr, df, self, name_generator)

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
        if self.strict:
            raise NotImplementedError('strict group-by not yet implemented')

        group_spec = [name_generator.get(col.alias) for col in group_by]

        agg_spec = {}
        rename_spec = []

        func_impl = {'avg': 'mean'}

        for col in columns:
            assert col.value.quantifier is None

            function = col.value.func.lower()
            function = func_impl.get(function, function)

            arg = name_generator.get(col.value.args[0].name)
            alias = name_generator.get(col.alias)

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

    def copy_from(self, scope, name, filename, options):
        try:
            copy_from = self.table_functions['copy_from']

        except KeyError:
            raise NotImplementedError('%s does not support copy_from' % self)

        args = []
        for k, v, in options.items():
            args += [k, v]

        filename = os.path.join(self.basepath, filename)
        scope[name] = copy_from(filename, *args)

    def copy_to(self, scope, name, filename, options):
        df = scope[name]
        df = self.remove_table_from_columns(df)

        format = options.pop('format', 'csv')

        if format == 'csv':
            filename = os.path.join(self.basepath, filename)

            if 'delimiter' in options:
                options['sep'] = options.pop('delimiter')

            df.to_csv(filename, index=False, **options)

        else:
            raise RuntimeError('unknown format %s' % format)

    def eval_table_valued(self, node, scope):
        # TODO: rename the table
        func = node.func.lower()
        if func not in self.table_functions:
            raise RuntimeError('unknown table valued function: %r' % func)

        func = self.table_functions[func]
        args = [eval_pandas(arg, None, self, None) for arg in node.args]
        return func(*args)

    def join(self, left, right, on, how):
        if self.strict:
            raise NotImplementedError('strict join not yet implemented')

        # TODO: re-add support for-non-equality joins, re-add support for strict joins
        assert how in {'inner', 'outer', 'left', 'right'}

        left_on, right_on = as_pandas_join_condition(left.columns, right.columns, on)
        return left.merge(right, left_on=left_on, right_on=right_on, how=how)

    def lateral(self, table, name_generator, func, args, alias):
        if func not in self.lateral_functions:
            raise ValueError('unknown lateral function %s' % func)

        alias = name_generator.get(alias)
        func = self.lateral_functions[func]
        args = [self.evaluate(table, arg, name_generator) for arg in args]

        combined = [table[col] for col in table.columns] + args
        df = pd.DataFrame({idx: s for idx, s in enumerate(combined)})

        num_origin_column = len(table.columns)

        parts = []

        for _, row in df.iterrows():
            child_df = func(*row.iloc[num_origin_column:])
            child_df = self.add_table_to_columns(child_df, alias)

            all_columns = list(table.columns) + list(child_df.columns)

            # copy the values of the original table
            for k, v in zip(table.columns, row.iloc[:num_origin_column]):
                child_df[k] = v

            print(list(child_df.columns))
            print(all_columns)
            parts.append(child_df[all_columns])

        return pd.concat(parts, axis=0, ignore_index=True)

    def sort_values(self, table, names, ascending):
        return table.sort_values(names, ascending=ascending)

    def compute(self, val):
        return val

    def limit_offset(self, table, limit=None, offset=None):
        if limit is None:
            limit = table.shape[0]

        if offset is None:
            offset = 0

        return table.iloc[offset:offset + limit]

    def drop_duplicates(self, tables):
        return tables.drop_duplicates()


eval_pandas = m.RuleSet(name='eval_pandas')


@eval_pandas.rule(m.instanceof(a.Name))
def eval_pandas_name(_, expr, df, model, name_generator):
    name = name_generator.get(expr.name)
    col = normalize_col_ref(name, df.columns)
    return df[col]


@eval_pandas.rule(m.instanceof(a.Integer))
def eval_integer(_, expr, *__):
    return int(expr.value)


@eval_pandas.rule(m.instanceof(a.Float))
def eval_float(_, expr, *__):
    return float(expr.value)


@eval_pandas.rule(m.instanceof(a.String))
def eval_string(_, expr, *__):
    return eval_string_literal(expr.value)


@eval_pandas.rule(m.instanceof(a.Bool))
def eval_bool(_, expr, *__):
    if expr.value.lower() == 'true':
        return True

    elif expr.value.lower() == 'false':
        return False

    raise ValueError('not a bool: %s' % expr.value)


@eval_pandas.rule(m.instanceof(a.UnaryOp))
def eval_pandas_unary_op(eval_pandas, expr, df, model, name_generator):
    operator_map = {
        '-': operator.neg,
    }

    if expr.op not in operator_map:
        raise ValueError('unknown unary operator %s' % expr.op)

    op = operator_map[expr.op]
    arg = eval_pandas(expr.arg, df, model, name_generator)

    return op(arg)


@eval_pandas.rule(m.instanceof(a.BinaryOp))
def eval_pandas_binary_op(eval_pandas, expr, df, model, name_generator):
    left = eval_pandas(expr.left, df, model, name_generator)
    right = eval_pandas(expr.right, df, model, name_generator)

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


@eval_pandas.rule(m.instanceof(a.Call))
def eval_call(eval_pandas, expr, df, model, name_generator):
    func = expr.func.lower()

    if func not in model.functions:
        raise ValueError('unknown function %s' % func)

    func = model.functions[func]
    args = [eval_pandas(arg, df, model, name_generator) for arg in expr.args]
    return func(*args)


@eval_pandas.rule(m.instanceof(a.Cast))
def eval_cast(eval_pandas, expr, df, model, name_generator):
    value = eval_pandas(expr.value, df, model, name_generator)

    if m.match(expr.type, m.record(a.Call, func=m.eq('VARCHAR'))):
        base_type = str

    elif m.match(expr.type, m.record(a.Name, name=m.pred(lambda v: v.lower() == 'json'))):
        base_type = util.cast_json

    else:
        raise ValueError('unknown type: {}'.format(expr.type))

    if not isinstance(value, pd.Series):
        return base_type(value)

    if base_type in {str, int, float, bool}:
        return value.astype(base_type)

    return value.map(base_type)


@eval_pandas.rule(m.instanceof(a.Null))
def eval_null(eval_pandas, expr, df, model, name_generator):
    return None


@eval_pandas.rule(m.instanceof(a.CaseExpression))
def eval_case_expression(eval_pandas, expr, df, model, name_generator):
    rest_sel = pd.Series(True, index=df.index)

    parts = []
    for case in expr.cases:
        sel = eval_pandas(case.condition, df.loc[rest_sel], model, name_generator)
        matched = df.loc[rest_sel & sel]

        result = eval_pandas(case.result, matched, model, name_generator)
        parts += [pd.Series(result, index=matched.index)]

        rest_sel = rest_sel & (~sel)

    matched = df.loc[rest_sel]
    result = eval_pandas(expr.else_, matched, model, name_generator) if expr.else_ else None
    parts += [pd.Series(result, index=matched.index)]

    result = pd.concat(parts, axis=0, ignore_index=False)
    result = result.loc[df.index]

    return result
