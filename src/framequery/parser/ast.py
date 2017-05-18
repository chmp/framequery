from __future__ import print_function, division, absolute_import

from framequery.util._record import Record


class Select(Record):
    __fields__ = [
        'columns', 'from_clause', 'where_clause', 'group_by_clause',
        'having_clause', 'order_by_clause', 'limit_clause', 'quantifier',
    ]


class FromClause(Record):
    __fields__ = ['tables']


class TableRef(Record):
    __fields__ = ['name', 'schema', 'alias']


class Column(Record):
    __fields__ = ['value', 'alias']


class Name(Record):
    __fields__ = ['name']


class InternalName(Record):
    __fields__ = ['name']


class WildCard(Record):
    __fields__ = ['table']


class Integer(Record):
    __fields__ = ['value']


class String(Record):
    __fields__ = ['value']


class BinaryOp(Record):
    __fields__ = ['op', 'left', 'right']


class UnaryOp(Record):
    __fields__ = ['op', 'arg']


class Call(Record):
    __fields__ = ['func', 'args']


class CallSetFunction(Record):
    __fields__ = ['func', 'args', 'quantifier']
    __types__ = [str, tuple, str]


class CallAnalyticsFunction(Record):
    __fields__ = ['call', 'order_by', 'partition_by']


class OrderBy(Record):
    __fields__ = ['value', 'order']


class PartitionByClause(Record):
    __fields__ = []


class Show(Record):
    __fields__ = ['args']
    __types__ = [tuple]


class Cast(Record):
    __fields__ = ['value', 'type']


class CopyFrom(Record):
    __fields__ = ['name', 'filename', 'options']
