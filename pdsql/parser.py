from __future__ import print_function, division, absolute_import

import operator

from funcparserlib.parser import some, maybe, skip, finished, forward_decl, many, pure

from .tokenize import Tokens
from ._base import Node, ForwardDecl, Unvalued, TransparentNode, ListNode, RecordNode
from ._util.grammar import optional, token, failing


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
        t.ttype is ttype and any(t.value == name for name in names)
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


class ValueExpression(TransparentNode, ForwardDecl):
    pass


class IdentifierChain(ListNode):
    item_parser = token(Tokens.Name) >> get_value
    separator_parser = period


class SetQuantifier(Node):
    parser = failing()


class BinaryExpression(RecordNode):
    __fields__ = ['operator', 'left', 'right']

    @classmethod
    def add(cls, left, right):
        return BinaryExpression('+', left, right)

    @classmethod
    def sub(cls, left, right):
        return BinaryExpression('-', left, right)

    @classmethod
    def mul(cls, left, right):
        return BinaryExpression('*', left, right)

    @classmethod
    def div(cls, left, right):
        return BinaryExpression('/', left, right)

    @classmethod
    def from_list(cls, parts):
        parts = list(parts)
        assert parts

        current, parts = parts[0], parts[1:]

        while parts:

            op, right, parts = parts[0], parts[1], parts[2:]
            current = BinaryExpression(op, current, right)

        return current


class NumericValueExpression(TransparentNode):
    # TODO: support signed expressions
    factor = (
        Integer.get_parser()
    )

    mul_op = (one_of(Tokens.Operator, '*/') >> get_value)
    add_op = (one_of(Tokens.Operator, '+-') >> get_value)

    term = (factor + many(mul_op + factor)) >> flatten >> BinaryExpression.from_list
    parser = (term + many(add_op + term)) >> flatten >> BinaryExpression.from_list


class CommonValueExpression(TransparentNode):
    parser = (
        NumericValueExpression.get_parser()
    )


class BooleanValueExpression(Node):
    parser = failing()


class ColumnReference(Node):
    # simplify grammar: alias for <identifier chain>
    parser = IdentifierChain.get_parser()


class GeneralSetFunction(RecordNode):
    __fields__ = ['function', 'value', 'quantifier']

    known_set_functions = [
        'AVG', 'MAX', 'MIN', 'SUM', 'EVERY', 'ANY', 'SOME'
        'COUNT', 'STDDEV_POP', 'STDDEV_SAMP', 'VAR_SAMP', 'VAR_POP',
        'COLLECT', 'FUSION', 'INTERSECTION'
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


class RowValueExpression(TransparentNode):
    # simplify grammar: alias for <nonparenthesized value expression primary>
    # TODO: add missing cases
    parser = (
        # <set function specification>
        CountAll.get_parser() |
        GeneralSetFunction.get_parser() |

        ColumnReference.get_parser()
    )


ValueExpression.define(
    (skip(paren_open) + ValueExpression.get_parser() + skip(paren_close)) |

    CommonValueExpression.get_parser() |
    BooleanValueExpression.get_parser() |
    RowValueExpression.get_parser()
)


class AsClause(Node):
    parser = (
        skip(as_) + name
    )

    @classmethod
    def from_parsed(cls, val):
        return val.value


class DerivedColumn(RecordNode):
    __fields__ = ['value', 'alias']

    parser = (
        (ValueExpression.get_parser() >> named('value')) +
        (optional(AsClause.get_parser()) >> named('alias'))
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


class TableName(Node):
    parser = token(Tokens.Name)

    @classmethod
    def from_parsed(cls, val):
        return cls(val.value)


TablePrimary = TableName


class JoinedTable(Node):
    parser = failing()


class TableReference(TransparentNode):
    parser = TableName.get_parser() | JoinedTable.get_parser()


class TableReferenceList(ListNode):
    item_parser = TableReference.get_parser()
    separator_parser = comma


class FromClause(TransparentNode):
    parser = (
        skip(token(Tokens.Keyword, 'FROM')) +
        TableReferenceList.get_parser()
    )


class WhereCaluse(Node):
    parser = failing()


class GroupByClause(ListNode):
    # TODO: allow grouping by numeric column indices
    prefix_parser = token(Tokens.Keyword, 'GROUP') + token(Tokens.Keyword, 'BY')
    item_parser = ColumnReference.get_parser()
    separator_parser = comma


class HavingClause(Node):
    parser = failing()


class Select(RecordNode):
    select_ = token(Tokens.DML, 'SELECT')

    parser = (
        skip(select_) +
        (optional(SetQuantifier.get_parser()) >> named('set_quantifier')) +
        (SelectList.get_parser() >> named('select_list')) +
        (FromClause.get_parser() >> named('from_clause')) +
        (optional(WhereCaluse.get_parser()) >> named('where_clause')) +
        (optional(GroupByClause.get_parser()) >> named('group_by_clause')) +
        (optional(HavingClause.get_parser()) >> named('having_clause')) +
        skip(finished)
    )

    __fields__ = [
        'set_quantifier', 'select_list', 'from_clause', 'where_clause',
        'group_by_clause', 'having_clause', 'window_clause',
    ]
