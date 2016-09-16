"""Generate an AST represention of a SQL statement.
"""
from __future__ import print_function, division, absolute_import

import operator

from funcparserlib.parser import some, maybe, skip, finished, forward_decl, many, pure
import six

from ._tokenize import Tokens
from ._base import Node, ForwardDecl, Unvalued, TransparentNode, ListNode, RecordNode
from ._util.grammar import optional, token, failing


def parse(s):
    return Select.parse(s)


def as_parsed(s):
    if isinstance(s, six.string_types):
        return parse(s)

    return s


comma = token(Tokens.Punctuation, ',')
period = token(Tokens.Punctuation, '.')
as_ = token(Tokens.Keyword, 'as')
name = token(Tokens.Name)
wildcard = token(Tokens.Wildcard)
paren_open = token(Tokens.Punctuation, '(')
paren_close = token(Tokens.Punctuation, ')')


def named(name):
    return lambda value: (name, value)


def get_value(obj):
    return obj.value


def specific_name(name):
    return token(Tokens.Name, name)


def one_of(ttype, names):
    return some(lambda t:
        t.ttype is ttype and any(t.value.upper() == name for name in names)
    )


def one_name_of(names):
    return one_of(Tokens.Name, names)


def one_keyword_of(names):
    return one_of(Tokens.Keyword, names)


def concat(head_tail):
    head, tail = head_tail
    return [head] + list(tail)


def flatten(head_tail):
    head, tail = head_tail
    return [head] + [
        item
        for part in tail
        for item in part
    ]


class Asterisk(Unvalued):
    parser = wildcard


class Integer(Node):
    parser = token(Tokens.Integer) >> get_value


class Float(Node):
    parser = token(Tokens.Float) >> get_value


class ValueExpression(TransparentNode, ForwardDecl):
    pass


class IdentifierChain(ListNode):
    item_parser = token(Tokens.Name) >> get_value
    separator_parser = period


_make_unary_op = lambda op: classmethod(lambda cls, operand: UnaryExpression(op, operand))
_make_binary_op = lambda op: classmethod(lambda cls, left, right: BinaryExpression(op, left, right))


class UnaryExpression(RecordNode):
    __fields__ = ['operator', 'operand']

    not_ = _make_unary_op('NOT')
    pos = _make_unary_op('+')
    neg = _make_unary_op('-')


class BinaryExpression(RecordNode):
    __fields__ = ['operator', 'left', 'right']

    add = _make_binary_op('+')
    sub = _make_binary_op('-')
    mul = _make_binary_op('*')
    div = _make_binary_op('/')

    eq = _make_binary_op('=')
    ne = _make_binary_op('!=')
    lt = _make_binary_op('<')
    gt = _make_binary_op('>')
    le = _make_binary_op('<=')
    ge = _make_binary_op('>=')

    and_ = _make_binary_op('AND')
    or_ = _make_binary_op('OR')

    @classmethod
    def from_list(cls, parts):
        parts = list(parts)
        assert parts

        current, parts = parts[0], parts[1:]

        while parts:

            op, right, parts = parts[0], parts[1], parts[2:]
            current = BinaryExpression(op, current, right)

        return current


class ColumnReference(Node):
    # simplify grammar: alias for <identifier chain>
    parser = IdentifierChain.get_parser()


_make_set_func = lambda op: classmethod(lambda cls, value, quantifier=None: cls(op, value, quantifier))


class GeneralSetFunction(RecordNode):
    __fields__ = ['function', 'value', 'quantifier']

    sum = _make_set_func('SUM')
    min = _make_set_func('MIN')
    max = _make_set_func('MAX')
    avg = _make_set_func('AVG')
    count = _make_set_func('COUNT')

    known_set_functions = [
        'AVG', 'MAX', 'MIN', 'SUM', 'EVERY', 'ANY', 'SOME'
        'COUNT', 'STDDEV_POP', 'STDDEV_SAMP', 'VAR_SAMP', 'VAR_POP',
        'COLLECT', 'FUSION', 'INTERSECTION', 'COUNT',
    ]

    parser = (
        (one_name_of(known_set_functions) >> get_value >> named('function')) +
        (skip(paren_open)) +
        (maybe(one_keyword_of(['DISTINCT', 'ALL']) >> get_value) >> named('quantifier')) +
        (ValueExpression.get_parser() >> named('value')) +
        skip(paren_close)
    )


class CountAll(Node):
    @classmethod
    def from_parsed(cls, val):
        return GeneralSetFunction('COUNT', Asterisk())

    parser = (
        specific_name('COUNT') +
        paren_open +
        wildcard +
        paren_close
    )


class FunctionArguments(ListNode):
    item_parser = ValueExpression.get_parser()
    separator_parser = comma


class FunctionCall(RecordNode):
    __fields__ = ['function', 'arguments']

    parser = (
        (name >> get_value >> named('function')) +
        skip(paren_open) +
        ((FunctionArguments.get_parser() | pure([])) >> named('arguments')) +
        skip(paren_close)
    )


def _make_binary_ops(op_class, instances, arg):
    op = one_of(op_class, instances) >> get_value
    return (arg + many(op + arg)) >> flatten >> BinaryExpression.from_list


def _build_arithmetic_tower(root, *levels):
    current = root

    for arity, op in levels:
        if arity == 2:
            op = op >> get_value
            current = (current + many(op + current)) >> flatten >> BinaryExpression.from_list

        elif arity == 1:
            unary_op = (
                (op >> get_value >> named('operator')) +
                (current >> named('operand'))
            ) >> UnaryExpression.from_parsed
            current = unary_op | current

        else:
            raise ValueError("cannot handle arity {}".format(arity))

    return current


@ValueExpression.define
def define_value_expression(cls):
    # TODO: support signed expressions
    factor = (
        (skip(paren_open) + ValueExpression.get_parser() + skip(paren_close)) |

        # row value expressions
        CountAll.get_parser() |
        GeneralSetFunction.get_parser() |
        FunctionCall.get_parser() |
        ColumnReference.get_parser() |

        Float.get_parser() |
        Integer.get_parser()

        # TODO: add support for non numbers
    )

    cls.define(
        _build_arithmetic_tower(
            factor,
            # TODO: support bitwise not
            (2, one_of(Tokens.Operator, '*/%')),
            (1, one_of(Tokens.Operator, '+-')),
            (2, one_of(Tokens.Operator, '+-&|^')),
            (2, one_of(Tokens.Comparison, ['=', '!=', '>', '<', '>=', '<=', '<>', '!>', '!<'])),
            (1, one_of(Tokens.Keyword, ['NOT'])),
            (2, one_of(Tokens.Keyword, ['AND'])),

            # TODO: support ALL, ANY, SOME, BETWEEN
            (2, one_of(Tokens.Keyword, ['IN', 'OR', 'LIKE'])),
        )
    )


class Select(ForwardDecl, RecordNode):
    __fields__ = [
        'set_quantifier', 'select_list', 'from_clause', 'where_clause',
        'group_by_clause', 'having_clause', 'order_by_clause', 'limit_clause',
        'common_table_expressions',
    ]


class DerivedColumn(RecordNode):
    __fields__ = ['value', 'alias']

    parser = (
        (ValueExpression.get_parser() >> named('value')) +
        (
            optional(
                skip(optional(as_)) +
                (name >> get_value)
            ) >> named('alias')
        )
    )


class QualifiedAsterisk(Node):
    parser = failing()


class SelectSublist(ListNode):
    item_parser = (
        DerivedColumn.get_parser() |
        QualifiedAsterisk.get_parser()
    )
    separator_parser = comma


class SelectList(TransparentNode):
    parser = Asterisk.get_parser() | SelectSublist.get_parser()


class TableName(RecordNode):
    __fields__ = ['table', 'schema', 'alias']

    schema = (token(Tokens.Name) >> get_value) + skip(period)
    table = token(Tokens.Name) >> get_value
    alias = skip(token(Tokens.Keyword, 'AS')) + (token(Tokens.Name) >> get_value)

    parser = (
        (maybe(schema) >> named('schema')) +
        (table >> named('table')) +
        (maybe(alias) >> named('alias'))
    )


class Join(RecordNode):
    __fields__ = ['how', 'table', 'on']

    how = (
        (
            skip(one_of(Tokens.Keyword, ['LEFT OUTER JOIN', 'LEFT JOIN'])) +
            pure('LEFT')
        ) |
        (
            skip(one_of(Tokens.Keyword, ['RIGHT OUTER JOIN', 'RIGHT JOIN'])) +
            pure('RIGHT')
        ) |
        (
            skip(one_of(Tokens.Keyword, ['INNER JOIN', 'JOIN'])) +
            pure('INNER')
        )
    )

    on = skip(token(Tokens.Keyword, 'ON')) + ValueExpression.get_parser()

    parser = (
        (how >> named('how')) +
        (TableName.get_parser() >> named('table')) +
        (on >> named('on'))
    )


class CrossJoin(RecordNode):
    __fields__ = ['table']
    parser = skip(token(Tokens.Keyword, 'CROSS JOIN')) + TableName.get_parser()

    @classmethod
    def from_parsed(cls, table):
        return cls(table)


class JoinedTable(RecordNode):
    __fields__ = ['table', 'joins']

    join = Join.get_parser() | CrossJoin.get_parser()

    parser = (
        (TableName.get_parser() >> named('table')) +
        (join + many(join) >> concat >> named('joins'))
    )


class TableReferenceList(ListNode):
    subquery = skip(paren_open) + Select.get_parser() + skip(paren_close)

    item_parser = subquery | JoinedTable.get_parser() | TableName.get_parser()
    separator_parser = comma


class FromClause(TransparentNode):
    parser = (
        skip(token(Tokens.Keyword, 'FROM')) +
        TableReferenceList.get_parser()
    )


class WhereCaluse(TransparentNode):
    parser = (
        skip(token(Tokens.Keyword, 'WHERE')) +
        ValueExpression.get_parser()
    )


class GroupByClause(ListNode):
    # TODO: allow grouping by numeric column indices
    prefix_parser = token(Tokens.Keyword, 'GROUP') + token(Tokens.Keyword, 'BY')
    item_parser = ColumnReference.get_parser()
    separator_parser = comma


class HavingClause(TransparentNode):
    parser = (
        skip(token(Tokens.Keyword, 'HAVING')) +
        ValueExpression.get_parser()
    )


class OrderByItem(RecordNode):
    __fields__ = ["value", "order"]

    parser = (
        (
            (ColumnReference.get_parser() >> named('value')) +
            (token(Tokens.Order, 'ASC') >> get_value >> named('order'))
        ) |
        (
            (ColumnReference.get_parser() >> named('value')) +
            (token(Tokens.Order, 'DESC') >> get_value >> named('order'))
        ) |
        (
            (ColumnReference.get_parser() >> named('value')) +
            (pure('DESC') >> named('order'))
        )
    )


class OrderByClause(ListNode):
    prefix_parser = (
        token(Tokens.Keyword, 'ORDER') +
        token(Tokens.Keyword, 'BY')
    )

    item_parser = OrderByItem.get_parser()
    separator_parser = comma


def as_int(val):
    return int(val.value)


class LimitClause(RecordNode):
    __fields__ = ['offset', 'limit']
    parser = skip(token(Tokens.Keyword, 'LIMIT')) + (
        (
            (Integer.get_parser() >> as_int >> named('offset')) +
            skip(comma) +
            (Integer.get_parser() >> as_int >> named('limit'))
        ) |
        (
            (Integer.get_parser() >> as_int >> named('limit')) +
            skip(token(Tokens.Keyword, 'OFFSET')) +
            (Integer.get_parser() >> as_int >> named('offset'))
        ) |
        (
            (Integer.get_parser() >> as_int >> named('limit')) +
            (pure(0) >> named('offset'))
        )
    )


class CommonTableExpression(RecordNode):
    __fields__ = ['name', 'select']

    parser = (
        (token(Tokens.Name) >> get_value >> named('name')) +
        skip(as_ + paren_open) +
        (Select.get_parser() >> named('select')) +
        skip(paren_close)
    )


class CommonTableExpressions(ListNode):
    prefix_parser = token(Tokens.CTE, 'WITH')
    item_parser = CommonTableExpression.get_parser()
    separator_parser = comma


@Select.define
def define_select(cls):
    select_ = token(Tokens.DML, 'SELECT')

    set_quantifier = (
        (token(Tokens.Keyword, 'ALL') >> get_value) |
        (token(Tokens.Keyword, 'DISTINCT') >> get_value) |
        pure('ALL')
    )

    cls.define(
        (optional(CommonTableExpressions.get_parser()) >> named('common_table_expressions')) +
        skip(select_) +
        (set_quantifier >> named('set_quantifier')) +
        (SelectList.get_parser() >> named('select_list')) +
        (FromClause.get_parser() >> named('from_clause')) +
        (optional(WhereCaluse.get_parser()) >> named('where_clause')) +
        (optional(GroupByClause.get_parser()) >> named('group_by_clause')) +
        (optional(HavingClause.get_parser()) >> named('having_clause')) +
        (optional(OrderByClause.get_parser()) >> named('order_by_clause')) +
        (optional(LimitClause.get_parser()) >> named('limit_clause'))
    )


def get_selected_column_name(node):
    if isinstance(node, ColumnReference):
        return node.value[-1]

    if not isinstance(node, DerivedColumn):
        return None

    if node.alias is not None:
        return node.alias

    return get_selected_column_name(node.value)
