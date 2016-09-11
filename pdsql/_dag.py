"""Classes representing transformation of dataframes.
"""
from __future__ import print_function, division, absolute_import


class Literal(object):
    """Marker node to tag literal values.
    """
    def __init__(self, value):
        self.value = value


class GetTable(object):
    def __init__(self, table, alias=None):
        self.table = table
        self.alias = alias


class Join(object):
    def __init__(self, left, right, how, on):
        self.left = left
        self.right = right
        self.how = how
        self.on = on


class Transform(object):
    """Generate a new dataframe by transforming the input columns.
    """
    def __init__(self, columns, table):
        self.columns = columns
        self.table = table


class Aggregate(object):
    """Aggregate the dataframe.
    """
    def __init__(self, columns, table, group_by=None):
        self.columns = columns
        self.table = table
        self.group_by = group_by


class Filter(object):
    """Filter a dataframe to the matching rows.
    """
    def __init__(self, filter, table):
        self.filter = filter
        self.table = table
