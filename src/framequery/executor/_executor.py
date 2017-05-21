"""execute_ast queries on dataframes.

The most general query involves the following transformations:

- pre-agg
- aggregate
- post-agg (in particular analytics functions)

"""
from __future__ import print_function, division, absolute_import

import inspect
import itertools as it
import logging

from ._util import (
    Unique,
    UniqueNameGenerator,

    column_get_table,
    eval_string_literal,
    internal_column,
    normalize_col_ref,
    to_internal_col,
)
from ..parser import ast as a, parse
from ..util import _monadic as m
from ..util._record import walk

_logger = logging.getLogger(__name__)


class Executor(object):
    """A persistent executor - to allow reusing scopes and models.

    :param scope:
        a mapping of table-names to dataframes. If not given, an empty scope
        is created.

    :param model:
        the model to use, see :func:`framequery.execute`.

    :param str basepath:
        the basepath of the model.

    """
    def __init__(self, scope=None, model='pandas', basepath='.'):
        if scope is None:
            scope = scope

        self.scope = scope
        self.model = get_model(model, basepath)

    def execute(self, q, basepath=None):
        if basepath is None:
            basepath = self.model.basepath

        with self.model.with_basepath(basepath) as model:
            return execute(q, self.scope, model=model)

    def update(self, *args, **kwargs):
        self.scope.update(*args, **kwargs)

    def compute(self, val):
        return self.model.compute(val)

    def add_function(self, name, func):
        self.model.functions[name] = func

    def add_table_function(self, name, func):
        self.model.table_functions[name] = func

    def add_lateral_function(self, name, func):
        self.model.lateral_functions[name] = func


# TOOD: add option autodetect the required model
def execute(q, scope=None, model='pandas', basepath='.'):
    """Execute queries against the provided scope.

    :param dict scope:

        a mapping of table names to dataframes. If not provided the globals and
        locals of the calling scope are used.

    :param Union[str,Model] model:

        the datamodel to use. Currently ``"pandas"`` and ``"dask"`` are
        supported as string values. For better customization create the model
        instances independently and pass them as arguments.

        See :class:`framequery.PandasModel` and :class:`framequery.DaskModel`
        for further information.

    :param str basepath:

        the basepath of ``copy from`` and ``copy to`` operations. This argument
        is only when constructing the models. For independently constructed
        models, the basepath can be set via their ``__init__`` arguments.

    """
    if scope is None:
        frame = inspect.currentframe()
        assert frame.f_back is not None

        scope = dict(frame.f_back.f_globals)
        scope.update(frame.f_back.f_locals)

    model = get_model(model, basepath=basepath)

    ast = parse(q)
    result = execute_ast(ast, scope, model)

    if result is not None:
        result = model.remove_table_from_columns(result)

    return result


class Model(object):
    pass


def get_model(model, basepath='.'):
    if not isinstance(model, str):
        return model

    if model == 'pandas':
        from ._pandas import PandasModel
        return PandasModel(basepath=basepath)

    elif model == 'dask':
        from ._dask import DaskModel
        return DaskModel(basepath=basepath)

    else:
        raise ValueError('unknown fq model: {}'.format(model))


execute_ast = m.RuleSet(name='execute_ast')


@execute_ast.rule(m.instanceof(a.Select))
def execute_ast_select(execute_ast, node, scope, model):
    name_generator = UniqueNameGenerator()

    if node.from_clause is None:
        table = model.dual()

    else:
        table = execute_ast(node.from_clause, scope, model)

    columns = normalize_columns(table.columns, node.columns)

    # hack for non group-by aggregates, introduce an artificial column
    # TODO: use DataFrame.agg in pandas
    if any(isinstance(n, a.CallSetFunction) for n in walk(columns)) and not node.group_by_clause:
        node = node.update(group_by_clause=[a.Bool('true')])

    if node.where_clause is not None:
        table = model.filter_table(table, node.where_clause, name_generator)

    if node.group_by_clause is not None:
        group_by = normalize_group_by(table.columns, columns, node.group_by_clause)

        split = SplitResult.chain(aggregate_split(col, group_by) for col in columns)
        post_aggregate, aggregate, pre_aggregate = split.by_levels(2)

        # chain group-by columns
        aggregate = aggregate
        pre_aggregate = pre_aggregate + group_by

        table = model.transform(table, pre_aggregate, name_generator)
        table = model.aggregate(table, aggregate, group_by, name_generator)
        table = model.transform(table, post_aggregate, name_generator)

    else:
        table = model.transform(table, columns, name_generator)

    if node.having_clause is not None:
        raise NotImplementedError('having is not yet implemented')

    if node.order_by_clause is not None:
        table = sort(table, node.order_by_clause, model)

    if node.limit_clause is not None:
        raise NotImplementedError('limit is not yet implemented')

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
            alias = get_alias(col)

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

    # replace integers by the corresponding one-based column
    group_by = [
        col if not isinstance(col, a.Integer) else columns[int(col.value) - 1].value
        for col in group_by
    ]

    matcher = m.any(
        m.map_capture(
            lambda name: a.Column(a.Name(name), alias=name),
            m.record(a.Name, m.capture(internal_column(table_columns))),
        ),
        m.map_capture(
            # note call to to_internal_col is required to handle table.column refs
            lambda name: a.Column(aliases[name], alias=to_internal_col(name)),
            m.record(a.Name, m.capture(m.verb(*aliases))),
        ),
        m.map_capture(
            lambda value: a.Column(value, alias=Unique()),
            m.capture(m.pred(lambda obj: type(obj) is not a.Name)),
        )
    )

    normalized = []
    for expr in group_by:
        match = m.match(expr, matcher)

        if not match:
            raise ValueError('cannot handle %s', expr)

        normalized.append(match[0])

    return normalized


def sort(table, values, model):
    if not m.match(values, m.rep(
        m.record(a.OrderBy, m.record(a.Name, m.wildcard), m.verb('desc', 'asc'))
    )):
        raise ValueError('cannot sort by: %s' % values)

    names = []
    ascending = []
    for val in values:
        names += [normalize_col_ref(val.value.name, table.columns)]
        ascending += [val.order == 'asc']

    return model.sort_values(table, names, ascending=ascending)


@execute_ast.rule(m.instanceof(a.FromClause))
def execute_ast_from_clause(execute_ast, node, scope, model):
    tables = [execute_ast(table, scope, model) for table in node.tables]

    if len(tables) == 0:
        raise NotImplementedError('no dual support')

    elif len(tables) > 1:
        raise NotImplementedError('no cross join support')

    return tables[0]


@execute_ast.rule(m.instanceof(a.TableRef))
def execute_ast_table_ref(execute_ast, node, scope, model):
    if node.schema:
        name = '{}.{}'.format(node.schema, node.name)

    else:
        name = node.name

    return model.get_table(scope, name, alias=node.alias)


@execute_ast.rule(m.instanceof(a.SubQuery))
def execute_ast_subquery(execute_ast, node, scope, model):
    if not node.alias:
        raise RuntimeError('subqueries need to be named')

    table = execute_ast(node.query, scope, model)
    return model.add_table_to_columns(table, node.alias.name)


@execute_ast.rule(m.instanceof(a.Join))
def execute_ast_join(execute_ast, node, scope, model):
    left = execute_ast(node.left, scope, model)
    right = execute_ast(node.right, scope, model)
    return model.join(left, right, node.on, node.how)


@execute_ast.rule(m.instanceof(a.Call))
def execute_ast_all(excecute_ast, node, scope, model):
    # TODO: fix test for lateral joins, .e.g, in casts types are referenced as names
    # assert not any(isinstance(n, a.Name) for n in walk(node.args)), "lateral joins not yet implemented"
    return model.eval_table_valued(node, scope)


@execute_ast.rule(m.instanceof(a.Show))
def execute_show(_, node, scope, model):
    config = {
        ('transaction', 'isolation', 'level'): 'read only',
        ('standard_conforming_strings',): 'on'
    }

    if node.args not in config:
        raise NotImplementedError('unknown option: %s' % node.args)

    value = config[node.args]
    return model.dual().assign(value=value)


@execute_ast.rule(m.instanceof(a.CopyFrom))
def execute_copy_from(_, node, scope, model):
    # TODO: parse the options properly
    options = {
        name.name: eval_string_literal(value.value)
        for name, value in node.options
    }

    model.copy_from(scope, node.name.name, eval_string_literal(node.filename.value), options)


@execute_ast.rule(m.instanceof(a.CopyTo))
def execute_copy_to(_, node, scope, model):
    # TODO: parse the options properly
    options = {
        name.name: eval_string_literal(value.value)
        for name, value in node.options
    }

    model.copy_to(scope, node.name.name, eval_string_literal(node.filename.value), options)


@execute_ast.rule(m.instanceof(a.DropTable))
def execute_drop_table(_, node, scope, __):
    for name in node.names:
        del scope[name.name]


@execute_ast.rule(m.instanceof(a.CreateTableAs))
def execute_create_table_as(execute_ast, node, scope, model):
    _logger.info('create table %s', node.name.name)
    scope[node.name.name] = execute_ast(node.query, scope, model)


@m.RuleSet.make(name='aggregate_split')
def aggregate_split(aggregate_split, node, group_by):
    group_by_map = {col.value: a.Name(col.alias) for col in group_by}
    if node in group_by_map:
        return SplitResult([(0, group_by_map[node])])

    return aggregate_split.apply_rules(node, group_by)


@aggregate_split.rule(m.instanceof(a.Column))
def aggregate_split_column(aggregate_split, node, group_by):
    alias = get_alias(node)

    result = aggregate_split(node.value, group_by)
    post, agg, pre = result.by_levels(2)

    post, = post
    post = [a.Column(post, alias=alias)]
    return SplitResult.from_levels(post, agg, pre)


@aggregate_split.rule(m.instanceof(a.Name))
def aggregate_split_name(aggregate_split, node, group_by):
    return SplitResult([(0, node)])


@aggregate_split.rule(m.instanceof(a.CallSetFunction))
def aggregate_split_call_set_function(aggregate_split, node, group_by):
    # replace count(*) by count(1)
    if node.func.lower() == 'count' and node.args == (a.WildCard(),):
        node = a.CallSetFunction('count', (a.Integer('1'),))

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


def get_alias(col_node):
    alias, = m.match(col_node, m.any(
        m.record(a.Column, alias=m.capture(m.ne(None))),
        m.record(a.Column, value=m.record(a.Name, m.capture(m.wildcard)), alias=m.eq(None)),
        m.capture(m.lit(Unique())),
    ))
    return to_internal_col(alias)
