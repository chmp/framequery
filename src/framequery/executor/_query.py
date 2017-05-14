"""Execute queries on dataframes.

The most general query involves the following transformations:

- pre-agg
- aggregate
- post-agg (in particular analytics functions)

"""
from __future__ import print_function, division, absolute_import

import collections
import operator
import re

import pandas as pd

from framequery.util._misc import match, Any, In, Sequence, InstanceOf, RuleSet
from . import dag as d
from ._util import normalize_col_ref, column_get_column, column_set_table
from ..parser import ast as a, parse


def query(q, scope):
    model = PandasModel()

    scope = {
        table_name: model.add_table_to_columns(df, table_name)
        for (table_name, df) in scope.items()
    }
    ast = parse(q, a.Select)

    dag = dag_compile(ast)
    result = dag_execute(dag, scope, model)
    result = model.remove_table_from_columns(result)
    return result


def get_alias(col, idx):
    if match(col, a.Column(value=Any, alias=InstanceOf(str))):
        return col.alias

    elif match(col, a.Column(value=a.Name(Any))):
        return col.value.name

    return str(idx)


dag_compile = RuleSet(name='dag_compile')


@dag_compile.rule(a.Select(columns=Any, from_clause=Any, order_by_clause=Any))
def dag_compile_select_star(dag_compile, node):
    dag = dag_compile(node.from_clause)

    if node.columns != ['*']:
        dag = d.Transform(dag, columns=node.columns)

    if node.order_by_clause is not None:
        # TODO: include any sort by columns in transforms
        dag = d.Sort(dag, values=node.order_by_clause)

    # TODO: filter to a subset of columns

    return dag


@dag_compile.rule(a.FromClause(Sequence(Any)))
def compile_from_clause(dag_compile, node):
    tables = [dag_compile(table) for table in node.tables]

    if len(tables) == 0:
        raise NotImplementedError('no dual support')

    elif len(tables) == 1:
        return tables[0]

    else:
        raise NotImplementedError('no cross join support')


@dag_compile.rule(InstanceOf(a.TableRef))
def compile_table_ref(dag_compile, node):
    if node.schema:
        name = '{}.{}'.format(node.schema, node.name)

    else:
        name = node.name

    return d.GetTable(name, alias=node.alias)


dag_execute = RuleSet(name='dag_execute')


@dag_execute.rule(InstanceOf(d.GetTable))
def execute_get_table(dag_execute, node, scope, model):
    return model.get_table(scope, node.table, alias=node.alias)


@dag_execute.rule(InstanceOf(d.Sort))
def execute_dag_sort(dag_execute, node, scope, model):
    if not match(node.values, Sequence(a.OrderBy(a.Name(Any), In('desc', 'asc')))):
        raise ValueError('cannot sort by: %s' % node.values)

    table = dag_execute(node.table, scope, model)

    names = []
    ascending = []
    for val in node.values:
        names += [normalize_col_ref(val.value.name, table.columns)]
        ascending += [val.order == 'asc']

    return model.sort_values(table, names, ascending=False)



@dag_execute.rule(InstanceOf(d.Transform))
def execute_dag_transform(dag_execute, node, scope, model):
    table = dag_execute(node.table, scope, model)

    result = collections.OrderedDict()

    for col in node.columns:
        result[col.alias] = model.evaluate(table , col.value)

    return model.build_data_frame(result, like=table)


class PandasModel(object):
    def __init__(self):
        self.eval = eval_pandas

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

    def evaluate(self, df, expr):
        return self.eval(expr, df)

    def build_data_frame(self, columns, like=None):
        index = like.index if like is not None else None
        return pd.DataFrame(columns, index=index)


eval_pandas = RuleSet(name='eval_pandas')


@eval_pandas.rule(InstanceOf(a.Name))
def eval_pandas_name(eval_pandas, expr, df):
    col = normalize_col_ref(expr.name, df.columns)
    return df[col]


@eval_pandas.rule(InstanceOf(a.BinaryOp))
def eval_pandas_binary_op(eval_pandas, expr, df):
    left = eval_pandas(expr.left, df)
    right = eval_pandas(expr.right, df)

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
        'like': _func_like,
        'not like': _func_not_like,
        'in': lambda a, b: a.isin(b),
        'not in': lambda a, b: ~(a.isin(b)),
    }

    try:
        op = operator_map[expr.op]

    except KeyError:
        raise ValueError("unknown operator {}".format(expr.op))

    else:
        return op(left, right)


def _func_like(s, pattern):
    pattern = re.escape(pattern)
    pattern = pattern.replace(r'\%', '.*')
    pattern = pattern.replace(r'\_', '.')
    pattern = '^' + pattern + '$'

    # sqlite is case insenstive, is this always the case?
    return pd.Series(s).str.contains(pattern, flags=re.IGNORECASE)


def _func_not_like(s, pattern):
    res = _func_like(s, pattern)
    # handle inversion with missing numbers
    res = (1 - res).astype(res.dtype)
    return res
