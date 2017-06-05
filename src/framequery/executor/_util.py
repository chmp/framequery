from __future__ import print_function, division, absolute_import

import itertools as it
from ..util._record import walk
from ..util import _monadic as m
from ..parser import ast as a

try:
    from enum import Enum as origin_base

except ImportError:
    origin_base = object


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


def to_internal_col(ref):
    if isinstance(ref, Unique):
        return ref

    ref = split_quoted_name(ref)
    ref = ref[-2:]

    if len(ref) == 2:
        table, column = ref
        return column_from_parts(table, column)

    else:
        column, = ref
        return column


def normalize_col_ref(ref, columns, optional=False):
    ref = split_quoted_name(ref)
    ref = ref[-2:]

    if len(ref) == 2:
        table, column = ref
        candidate = column_from_parts(table=table, column=column)

        candidates = [candidate] if candidate in columns else []

    else:
        column, = ref

        candidates = [
            candidate
            for candidate in columns
            if column_get_column(candidate) == column
        ]

    if len(candidates) == 0:
        if optional is True:
            return None

        raise ValueError("column {} not found in {}".format(ref, columns))

    if len(candidates) > 1:
        raise ValueError(
            "column {} is ambigious among {}".format(ref, columns)
        )

    return candidates[0]


def split_quoted_name(name):
    parts = []
    current = ''

    in_string = False
    after_quote = False

    for c in name:
        if after_quote:
            current += c
            after_quote = False

        elif in_string and c != '"':
            current += c

        elif c == '"':
            in_string = not in_string

        elif c == '\\':
            after_quote = True

        elif c == '.':
            parts.append(current)
            current = ''

        else:
            current += c

    parts.append(current)

    return parts


def _split_table_column(obj, sep='/@/'):
    parts = obj.split(sep, 1)

    if len(parts) == 1:
        return None, parts[0]

    return tuple(parts)


def internal_column(internal_columns):
    def internal_column_impl(obj):
        for icol in internal_columns:
            if column_match(obj, icol):
                return [obj], None, {}

        return None, obj, {}

    return m.one(internal_column_impl)


class Unique(object):
    def __hash__(self):
        return hash(id(self))

    def __repr__(self):
        return '<Unique %s>' % id(self)


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
            self.names[obj] = 'unique_{}'.format(next(self.ids))

        return self.names[obj]

    def fix(self, objs=()):
        for obj in objs:
            self.get(obj)

        return UniqueNameGenerator(self.names, fixed=True)


def eval_string_literal(value, quote="'"):
    if value[:1] != quote:
        raise ValueError('unquoted string')

    value = value[1:-1]
    value = value.replace(quote + quote, quote)

    return str(value)


def as_pandas_join_condition(left_columns, right_columns, condition, name_generator):
    flat_condition = _flatten_join_condition(condition, name_generator)

    left = []
    right = []

    for aa, bb in flat_condition:
        a_is_left, aa = _is_left(left_columns, right_columns, aa)
        b_is_left, bb = _is_left(left_columns, right_columns, bb)

        if a_is_left == b_is_left:
            raise ValueError("cannot join a table to itslef ({}, {})".format(aa, bb))

        if a_is_left:
            left.append(aa)
            right.append(bb)

        else:
            right.append(aa)
            left.append(bb)

    return left, right


def _is_left(left_columns, right_columns, ref):
    left_ref = normalize_col_ref(ref, left_columns, optional=True)
    right_ref = normalize_col_ref(ref, right_columns, optional=True)

    if (left_ref is None) == (right_ref is None):
        raise ValueError('col ref {} is ambigious'.format(ref))

    return (left_ref is not None), left_ref if left_ref is not None else right_ref


def _flatten_join_condition(condition, name_generator):
    if not isinstance(condition, a.BinaryOp):
        raise ValueError("can only handle equality joins")

    if condition.op == 'AND':
        return it.chain(
            _flatten_join_condition(condition.left),
            _flatten_join_condition(condition.right),
        )

    elif condition.op == '=':
        if not (
            isinstance(condition.left, a.Name) and
            isinstance(condition.right, a.Name)
        ):
            raise ValueError("requires column references")

        return [(name_generator.get(condition.left.name), name_generator.get(condition.right.name))]

    else:
        raise ValueError("can only handle equality joins")


def prepare_join(op, left_columns, right_columns):
    """Prepare a join condition for execution.

    Return a tuple of

    - ``left_transforms``: a list of transformations to be applied to the left table
    - ``left_filter``: a filter expression to be applied to the left table
    - ``right_transforms``: a list of transformations to be applied to the right table
    - ``right_filter``: a filter expression to be applied to the right table
    - ``new_eq`` a single equality condition, possibly depending on transformed columns
    - ``neq``: a single non-equality condition. If non empty, an equality condition that creates a cross-join is added.
        Possibly none.

    """
    eq = []
    neq = []

    left_filter = []
    right_filter = []

    # split any expressions joined by ands into equality or other expressions
    for op in flatten_ands(op):
        origin = determine_origin(op, left_columns, right_columns)

        if origin in {Origin.left, Origin.right}:
            by_origin(origin, left_filter, right_filter).append(op)

        elif isinstance(op, a.BinaryOp) and op.op == '=':
            eq.append(op)

        else:
            neq.append(op)

    if neq:
        eq.append(a.BinaryOp(op='=', left=a.Integer('1'), right=a.Integer('1')))

    # generate transforms for equality expressions
    new_eq = []

    left_transforms = []
    right_transforms = []

    for expr in eq:
        left_origin = determine_origin(expr.left, left_columns, right_columns)
        right_origin = determine_origin(expr.right, left_columns, right_columns)

        if Origin.ambigious in {left_origin, right_origin}:
            raise ValueError('ambigious join')

        # note only both values can be unkown. Other case is removed above.
        if left_origin is Origin.unknown and right_origin is Origin.unknown:
            left = Unique()
            right = Unique()
            left_transforms.append(a.Column(expr.left, left))
            right_transforms.append(a.Column(expr.right, right))

            new_eq.append(expr.update(left=a.Name(left), right=a.Name(right)))

        else:
            if not isinstance(expr.left, a.Name):
                unique = Unique()
                by_origin(left_origin, left_transforms, right_transforms).append(a.Column(expr.left, unique))
                expr = expr.update(left=a.Name(unique))

            if not isinstance(expr.right, a.Name):
                unique = Unique()
                by_origin(right_origin, left_transforms, right_transforms).append(a.Column(expr.right, unique))
                expr = expr.update(right=a.Name(unique))

            new_eq.append(expr)

    return (
        left_transforms, and_join(left_filter),
        right_transforms, and_join(right_filter),
        and_join(new_eq), and_join(neq),
    )


def by_origin(origin, left, right):
    if origin is Origin.left:
        return left

    elif origin is Origin.right:
        return right

    else:
        raise ValueError()


def and_join(values):
    if not values:
        return None

    if len(values) == 1:
        return values[0]

    head, tail = values[0], values[1:]

    return a.BinaryOp(op='and', left=head, right=and_join(tail))


def flatten_ands(op):
    if isinstance(op, a.BinaryOp) and op.op != 'and':
        return [op]

    return flatten_ands(op.left) + flatten_ands(op.right)


def determine_origin(expr, left_columns, right_columns):
    if isinstance(expr, a.BinaryOp):
        return (
            determine_origin(expr.left, left_columns, right_columns) &
            determine_origin(expr.right, left_columns, right_columns)
        )

    elif isinstance(expr, a.UnaryOp):
        return determine_origin(expr.arg, left_columns, right_columns)

    elif isinstance(expr, a.Name):
        left = normalize_col_ref(expr.name, left_columns, optional=True)
        right = normalize_col_ref(expr.name, right_columns, optional=True)

        if left is None and right is None:
            return Origin.unknown

        elif left is None and right is not None:
            return Origin.right

        elif left is not None and right is None:
            return Origin.left

        else:
            return Origin.ambigious

    elif isinstance(expr, (a.Integer, a.String, a.Bool, a.Float)):
        return Origin.unknown

    else:
        raise NotImplementedError('unknown expression: %r' % type(expr))


class Origin(int, origin_base):
    unknown = 0
    left = 1
    right = 2
    ambigious = 3

    def __and__(self, other):
        other = Origin(other)

        if other == self.ambigious or self == self.ambigious:
            return self.ambigious

        if other == self.unknown:
            return self

        if self == self.unknown:
            return other

        if self != other:
            return self.ambigious

        return self

    def __rand__(self, other):
        cls = type(self)
        return cls.__and__(cls(other), self)
