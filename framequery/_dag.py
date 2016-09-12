"""Classes representing transformation of dataframes.
"""
from __future__ import print_function, division, absolute_import

from ._base import Record


class Literal(object):
    """Marker node to tag literal values.
    """
    def __init__(self, value):
        self.value = value


class GetTable(Record):
    __fields__ = ['table', 'alias']


class Join(object):
    def __init__(self, left, right, how, on):
        self.left = left
        self.right = right
        self.how = how
        self.on = on


class Transform(Record):
    """Generate a new dataframe by transforming the input columns.
    """
    __fields__ = ['table', 'columns']


class Aggregate(Record):
    """Aggregate the dataframe.
    """
    __fields__ = ['table', 'columns', 'group_by']


class Filter(Record):
    """Filter a dataframe to the matching rows.
    """
    __fields__ = ['table', 'filter']


class Limit(Record):
    """Limit the dataframe to the given rows.
    """
    __fields__ = ['table', 'offset', 'limit']
