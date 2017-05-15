"""Execute queries on dataframes.

The most general query involves the following transformations:

- pre-agg
- aggregate
- post-agg (in particular analytics functions)

"""
from __future__ import print_function, division, absolute_import

import collections
import itertools as it
import operator

import pandas as pd

from ._util import normalize_col_ref, column_get_column, column_set_table, column_match
from ..parser import ast as a, parse

from ..util import _funcs
from ..util._misc import (
    match, Any, In, Sequence, InstanceOf, RuleSet, Matcher, UnpackResult, unpack,
    OneOf, Not, Transform, Literal, Eq, ExactSequence
)


def query(q, scope, debug=False):
    model = PandasModel(debug=debug)

    scope = {
        table_name: model.add_table_to_columns(df, table_name)
        for (table_name, df) in scope.items()
    }
    ast = parse(q, a.Select)

    result = execute(ast, scope, model)
    result = model.remove_table_from_columns(result)
    return result


def get_alias(col, idx):
    if match(col, a.Column(value=Any, alias=InstanceOf(str))):
        return col.alias

    elif match(col, a.Column(value=a.Name(Any))):
        return col.value.name

    return str(idx)


execute = RuleSet(name='dag_compile')


@execute.rule(InstanceOf(a.Select))
def execute_select(execute, node, scope, model):
    name_generator = UniqueNameGenerator()

    table = execute(node.from_clause, scope, model)

    group_by = normalize_group_by(table.columns, node.columns, node.group_by_clause)

    if group_by is not None:
        split = SplitResult.chain(aggregate_split(col, group_by) for col in node.columns)
        post_aggregate, aggregate, pre_aggregate = split.by_levels(2)

        # chain group-by columns
        aggregate = aggregate
        pre_aggregate = pre_aggregate + group_by

        model.debug('pre-aggregate columns: {}', pre_aggregate)
        table = model.transform(table, pre_aggregate, name_generator)
        model.debug('pre-aggregate result: {}', table)

        model.debug('aggregate columns: {}', aggregate)
        table = model.aggregate(table, aggregate, group_by, name_generator)
        model.debug('aggregate result: {)', table)

        model.debug('post-aggregate columns: {}', post_aggregate)
        table = model.transform(table, post_aggregate, name_generator)
        model.debug('post-aggregate result: {}', table)

    else:
        if node.columns != ['*']:
            table = model.transform(table, node.columns, name_generator)

    if node.order_by_clause is not None:
        table = _sort(table, node.order_by_clause, model)

    return table


def normalize_group_by(table_columns, columns, group_by):
    """
    Different cases:

    1. a existing column is selected
    2. alias of selected expression is used as in group by
    3. a group by expression is selected verbatim

    The strategy is to transform case 2 into case 3 and then replace all
    occurrences of the group-by expression by an anonymous alias that is filled
    while grouping. Also, prefer case 1 over case 2.
    """
    if group_by is None:
        return None

    aliases = {col.alias: col.value for col in columns if col.alias is not None}

    matcher = OneOf(
        Transform(
            lambda name: a.Column(a.Name(name), alias=name),
            a.Name(InternalColumnMatcher(table_columns, group=0)),
        ),
        Transform(
            lambda name: a.Column(aliases[name], alias=name),
            a.Name(In(*aliases, group=0)),
        ),
        Transform(
            lambda value: a.Column(value, alias=Unique()),
            Not(a.Name(Any), group=0),
        )
    )

    normalized = []
    for expr in group_by:
        m = unpack(expr, matcher)

        if not m:
            raise ValueError('cannot handle %s', expr)

        normalized.append(m[0])

    return normalized


class InternalColumnMatcher(Matcher):
    def __init__(self, internal_columns, group=None):
        self.internal_columns = internal_columns
        self.group = group

    def unpack(self, obj):
        for icol in self.internal_columns:
            if column_match(obj, icol):
                return UnpackResult.make(True, self.group, obj)

        return UnpackResult(False)


def _transform(table, columns, model):
    result = collections.OrderedDict()

    for col in columns:
        result[col.alias] = model.evaluate(table, col.value)

    return model.build_data_frame(result, like=table)


def _sort(table, values, model):
    if not match(values, Sequence(a.OrderBy(a.Name(Any), In('desc', 'asc')))):
        raise ValueError('cannot sort by: %s' % values)

    names = []
    ascending = []
    for val in values:
        names += [normalize_col_ref(val.value.name, table.columns)]
        ascending += [val.order == 'asc']

    return model.sort_values(table, names, ascending=False)


@execute.rule(InstanceOf(a.FromClause))
def execute_from_clause(execute, node, scope, model):
    tables = [execute(table, scope, model) for table in node.tables]

    if len(tables) == 0:
        raise NotImplementedError('no dual support')

    elif len(tables) > 1:
        raise NotImplementedError('no cross join support')

    return tables[0]


@execute.rule(InstanceOf(a.TableRef))
def execute_table_ref(execute, node, scope, model):
    if node.schema:
        name = '{}.{}'.format(node.schema, node.name)

    else:
        name = node.name

    return model.get_table(scope, name, alias=node.alias)


class SplitResult(list):
    @classmethod
    def from_levels(cls, *levels):
        return cls(
            (level, item)
            for level, items in enumerate(levels)
            for item in items
        )

    @classmethod
    def chain(cls, iterable):
        return cls(it.chain.from_iterable(iterable))

    def promote(self):
        return SplitResult((level + 1, obj) for level, obj in self)

    def by_levels(self, maxlevel):
        r = {}

        for level, obj in self:
            r.setdefault(level, []).append(obj)

        assert max(r) <= maxlevel

        return tuple(r.get(level, []) for level in range(maxlevel + 1))


@RuleSet.make(name='aggregate_split')
def aggregate_split(aggregate_split, node, group_by):
    group_by_map = {col.value: a.Name(col.alias) for col in group_by}
    if node in group_by_map:
        return SplitResult([(0, group_by_map[node])])

    return aggregate_split.apply_rules(node, group_by)


@aggregate_split.rule(InstanceOf(a.Column))
def aggregate_split_column(aggregate_split, node, group_by):
    alias, = unpack(node, OneOf(
        a.Column(value=Any, alias=Not(Eq(None), group=0)),
        a.Column(value=a.Name(name=Any(group=0)), alias=None),
        Literal(Unique(), group=0),
    ))

    result = aggregate_split(node.value, group_by)
    post, agg, pre = result.by_levels(2)

    post, = post
    post = [a.Column(post, alias=alias)]
    return SplitResult.from_levels(post, agg, pre)


@aggregate_split.rule(InstanceOf(a.Name))
def aggregate_split_name(aggregate_split, node, group_by):
    return SplitResult([(0, node)])


@aggregate_split.rule(InstanceOf(a.CallSetFunction))
def aggregate_split_call_set_function(aggregate_split, node, group_by):
    ids = [Unique() for _ in node.args]
    self_id = Unique()
    deferred_args = [a.Name(id) for id in ids]

    result = SplitResult()
    result.extend((2, a.Column(arg, alias=id)) for arg, id in zip(node.args, ids))
    result.append((1, a.Column(node.update(args=deferred_args), alias=self_id)))
    result.append((0, a.Name(self_id)))

    return result


class Unique(object):
    def __hash__(self):
        return hash(id(self))


class UniqueNameGenerator(object):
    def __init__(self):
        self.names = {}
        self.ids = iter(it.count())

    def get(self, obj):
        if not isinstance(obj, Unique):
            return obj

        if obj not in self.names:
            self.names[obj] = 'unique-{}'.format(next(self.ids))

        return self.names[obj]


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

    def build_data_frame(self, columns, like=None):
        index = like.index if like is not None else None
        return pd.DataFrame(columns, index=index)

    def transform(self, table, columns, name_generator):
        result = collections.OrderedDict()

        for col in columns:
            alias = name_generator.get(col.alias)
            result[alias] = self.evaluate(table, col.value, name_generator)

        return self.build_data_frame(result, like=table)

    def aggregate(self, table, columns, group_by, name_generator):
        group_by = [name_generator.get(col.alias) for col in group_by]
        grouped = table.groupby(group_by)

        result = collections.OrderedDict()

        for col in columns:
            matcher = a.Column(
                a.CallSetFunction.any(args=ExactSequence(InstanceOf(a.Name))),
                alias=Any,
            )
            if not match(col, matcher):
                raise ValueError('cannot aggregate {}, expected {}'.format(col, matcher))

            function = col.value.func
            arg = name_generator.get(col.value.args[0].name)
            quantifier = col.value.quantifier

            assert quantifier is None

            impl = self.aggregates[function]

            alias = name_generator.get(col.alias)
            result[alias] = impl(grouped[arg])

        result = pd.DataFrame(result)
        return result.reset_index(drop=False)


eval_pandas = RuleSet(name='eval_pandas')


@eval_pandas.rule(InstanceOf(a.Name))
def eval_pandas_name(eval_pandas, expr, df, name_generator):
    name = name_generator.get(expr.name)
    col = normalize_col_ref(name, df.columns)
    return df[col]


@eval_pandas.rule(InstanceOf(a.Integer))
def eval_integer(_, expr, df, name_generator):
    return int(expr.value)


@eval_pandas.rule(InstanceOf(a.BinaryOp))
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
        'like': _funcs.like,
        'not like': _funcs.not_like,
        'in': lambda a, b: a.isin(b),
        'not in': lambda a, b: ~(a.isin(b)),
    }

    try:
        op = operator_map[expr.op]

    except KeyError:
        raise ValueError("unknown operator {}".format(expr.op))

    else:
        return op(left, right)
