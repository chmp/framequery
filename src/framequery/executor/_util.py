from __future__ import print_function, division, absolute_import

import itertools as it
from ..util._misc import Matcher, UnpackResult, walk


def column_match(col, internal_col):
    col_table, col = _split_table_column(col, '.')
    internal_col_table, internal_col = _split_table_column(internal_col)

    if col_table is None:
        return col == internal_col

    return internal_col_table == col_table and internal_col == col


def column_set_table(column, table):
    """Given a string column, possibly containing a table, set the table.

        >>> column_set_table('foo', 'bar')
        'bar/@/foo'

        >>> column_set_table('foo/@/bar', 'baz')
        'baz/@/bar'
    """
    return column_from_parts(table, column_get_column(column))


def column_get_table(column):
    table, _ = _split_table_column(column)
    return table


def column_get_column(column):
    """Given a string column, possibly containing a table, extract the column.

        >>> column_get_column('foo')
        'foo'

        >>> column_get_column('foo/@/bar')
        'bar'
    """
    _, column = _split_table_column(column)
    return column


def column_from_parts(table, column):
    """Given string parts, construct the full column name.

        >>> column_from_parts('foo', 'bar')
        'foo/@/bar'

    """
    if table is None:
        return column

    return '{}/@/{}'.format(table, column)


def normalize_col_ref(ref, columns):
    ref = ref.split('.')
    ref = ref[-2:]

    if len(ref) == 2:
        table, column = ref
        return column_from_parts(table=table, column=column)

    ref_column = ref[0]

    candidates = [
        candidate
        for candidate in columns
        if column_get_column(candidate) == ref_column
    ]

    if len(candidates) == 0:
        raise ValueError("column {} not found in {}".format(ref, columns))

    if len(candidates) > 1:
        raise ValueError(
            "column {} is ambigious among {}".format(ref, columns)
        )

    return candidates[0]


def _split_table_column(obj, sep='/@/'):
    parts = obj.split(sep, 1)

    if len(parts) == 1:
        return None, parts[0]

    return tuple(parts)


class InternalColumnMatcher(Matcher):
    def __init__(self, internal_columns, group=None):
        self.internal_columns = internal_columns
        self.group = group

    def unpack(self, obj):
        for icol in self.internal_columns:
            if column_match(obj, icol):
                return UnpackResult.make(True, self.group, obj)

        return UnpackResult(False)


class Unique(object):
    def __hash__(self):
        return hash(id(self))


def all_unique(obj):
    return [child for child in walk(obj) if isinstance(child, Unique)]


class UniqueNameGenerator(object):
    def __init__(self, names=None, fixed=False):
        if names is None:
            names = {}

        self.names = dict(names)

        if not fixed:
            self.ids = iter(it.count())

        else:
            self.ids = None

    def get(self, obj):
        if not isinstance(obj, Unique):
            return obj

        if obj not in self.names:
            if self.ids is None:
                raise RuntimeError('cannot request unknown unique from a fixed generator')
            self.names[obj] = 'unique-{}'.format(next(self.ids))

        return self.names[obj]

    def fix(self, objs=()):
        for obj in objs:
            self.get(obj)

        return UniqueNameGenerator(self.names, fixed=True)
