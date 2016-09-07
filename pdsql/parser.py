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


def named(name):
    return lambda value: (name, value)


def get_value(obj):
    return obj.value


class Asterisk(Unvalued):
    parser = token(Tokens.Wildcard)


class IdentifierChain(ListNode):
    item_parser = token(Tokens.Name) >> get_value
    separator_parser = period


class SetQuantifier(Node):
    parser = failing()


class CommonValueExpression(Node):
    parser = failing()


class BooleanValueExpression(Node):
    parser = failing()


class ColumnReference(Node):
    # simplify grammar: alias for <identifier chain>
    parser = IdentifierChain.get_parser()


class RowValueExpression(TransparentNode):
    # simplify grammar: alias for <nonparenthesized value expression primary>
    # TODO: add missing cases
    parser = (
        ColumnReference.get_parser()
    )


class ValueExpression(TransparentNode):
    parser = (
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


class SelectList(TransparentNode    ):
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


class GroupByClause(Node):
    parser = failing()


class HavingClause(Node):
    parser = failing()


class WindowClause(Node):
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
        (optional(WindowClause.get_parser()) >> named('window_clause')) +
        skip(finished)
    )

    __fields__ = [
        'set_quantifier', 'select_list', 'from_clause', 'where_clause',
        'group_by_clause', 'having_clause', 'window_clause',
    ]
