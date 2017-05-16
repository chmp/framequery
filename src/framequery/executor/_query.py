"""execute_ast queries on dataframes.

The most general query involves the following transformations:

- pre-agg
- aggregate
- post-agg (in particular analytics functions)

"""
from __future__ import print_function, division, absolute_import

import itertools as it

from ._util import (
    normalize_col_ref, Unique, UniqueNameGenerator, InternalColumnMatcher, column_get_table,
)
from ..parser import ast as a, parse
from ..util._misc import (
    match, Any, In, Sequence, InstanceOf, RuleSet, unpack, OneOf, Not, Transform, Literal, Eq,
)


# TOOD: add option autodetect the required model
def execute(q, scope, model='pandas'):
    model = get_model(model)

    scope = {
        table_name: model.add_table_to_columns(df, table_name)
        for (table_name, df) in scope.items()
    }
    ast = parse(q, a.Select)

    result = execute_ast(ast, scope, model)
    result = model.remove_table_from_columns(result)
    return result


def get_model(model, debug=False):
    if not isinstance(model, str):
        return model

    if model == 'pandas':
        from ._pandas import PandasModel
        return PandasModel()

    elif model == 'dask':
        from ._dask import DaskModel
        return DaskModel()

    else:
        raise ValueError('unknown fq model: {}'.format(model))


def get_alias(col, idx):
    if match(col, a.Column(value=Any, alias=InstanceOf(str))):
        return col.alias

    elif match(col, a.Column(value=a.Name(Any))):
        return col.value.name

    return str(idx)


execute_ast = RuleSet(name='dag_compile')


@execute_ast.rule(InstanceOf(a.Select))
def execute_ast_select(execute_ast, node, scope, model):
    name_generator = UniqueNameGenerator()

    table = execute_ast(node.from_clause, scope, model)

    columns = normalize_columns(table.columns, node.columns)

    if node.group_by_clause is not None:
        group_by = normalize_group_by(table.columns, columns, node.group_by_clause)

        split = SplitResult.chain(aggregate_split(col, group_by) for col in columns)
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
        table = model.transform(table, columns, name_generator)

    if node.order_by_clause is not None:
        table = sort(table, node.order_by_clause, model)

    return table


def normalize_columns(table_columns, columns):
    result = []

    for col in columns:
        # TODO: expand `.*` style columns
        if isinstance(col, a.WildCard):
            if col.table is None:
                result.extend(a.InternalName(c) for c in table_columns)

            else:
                result.extend(
                    a.InternalName(c)
                    for c in table_columns if column_get_table(c) == col.table
                )

        elif isinstance(col, a.Column):
            alias, = unpack(col, OneOf(
                a.Column.any(alias=Not(Eq(None), group=0)),
                a.Column(value=a.Name(Any(group=0)), alias=None),
                Literal(Unique(), group=0),
            ))

            # make sure a column always has a name
            result.append(col.update(alias=alias))

        else:
            raise ValueError('cannot normalize {}'.format(col))

    return result


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
        return []

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


def sort(table, values, model):
    if not match(values, Sequence(a.OrderBy(a.Name(Any), In('desc', 'asc')))):
        raise ValueError('cannot sort by: %s' % values)

    names = []
    ascending = []
    for val in values:
        names += [normalize_col_ref(val.value.name, table.columns)]
        ascending += [val.order == 'asc']

    return model.sort_values(table, names, ascending=False)


@execute_ast.rule(InstanceOf(a.FromClause))
def execute_ast_from_clause(execute_ast, node, scope, model):
    tables = [execute_ast(table, scope, model) for table in node.tables]

    if len(tables) == 0:
        raise NotImplementedError('no dual support')

    elif len(tables) > 1:
        raise NotImplementedError('no cross join support')

    return tables[0]


@execute_ast.rule(InstanceOf(a.TableRef))
def execute_ast_table_ref(execute_ast, node, scope, model):
    if node.schema:
        name = '{}.{}'.format(node.schema, node.name)

    else:
        name = node.name

    return model.get_table(scope, name, alias=node.alias)


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
