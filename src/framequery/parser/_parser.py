from __future__ import print_function, division, absolute_import

import re
import string as _string

from framequery.util import _monadic as m
from . import ast as a


def tokenize(query):
    parts, rest, d = splitter(query)

    if rest != '':
        raise ValueError('extra tokens: {}, {}'.format(rest, d))

    return parts


def parse(query, what=None):
    if what is not None:
        used_parser = constructors[what]

    else:
        used_parser = parser

    tokens = tokenize(query)
    ast, rest, debug = used_parser(tokens)

    if rest:
        raise ValueError('extra tokens: {}\n{}'.format(tokens, '\n'.join(m.format_debug(debug))))

    if len(ast) != 1:
        raise RuntimeError('internal parser error')

    return ast[0]


def verbatim_token(*p):
    return m.one(m.verbatim(*p))


def regex_token(p):
    return m.one(m.regex(p))


def svtok(*p):
    return m.ignore(m.one(m.verbatim(*p)))


def full_word(matcher):
    @m._delegate(matcher, where='full_word')
    def full_word_impl(matches, s, d, seq):
        if not s or s[0] not in _string.ascii_letters:
            return matches, s, d

        return None, seq, dict(d, status=m.Status.failure)

    return full_word_impl


def base_string(quote="'"):
    def base_string_impl(seq):
        if not seq:
            return None, seq, m.Status.fail(where='base_string', message='no value')

        s = seq[0]
        if s[0] != quote or s[-1] != quote:
            return None, seq, m.Status.fail(where='base_string', message='%r is not a string' % seq)

        # TODO: remove quotes

        return [s], seq[1:], m.Status.succeed()

    return base_string_impl


def build_binary_tree(seq):
    "transform `[[a, b, c, d, e]]` -> `[BinaryOp(b, a, BinaryOp(d, c, e))]`"
    def _impl(seq):
        assert len(seq) % 2 == 1

        if len(seq) == 1:
            return seq[0]

        else:
            return a.BinaryOp(seq[1], seq[0], _impl(seq[2:]))

    assert len(seq) == 1
    return [_impl(seq[0])]


def binary_op(value, *ops):
    return m.transform(build_binary_tree, m.list_of(verbatim_token(*ops), value))


def unary_op(value, *ops):
    return m.any(
        m.construct(a.UnaryOp, m.keyword(op=verbatim_token(*ops)), m.keyword(arg=value)),
        value,
    )


def compound_token(*parts):
    return m.transform(
        lambda s: [' '.join(s)],
        m.sequence(*[verbatim_token(p) for p in parts]),
    )


integer_format = r'\d+'
name_format = r'\w+'

keywords = {
    'select', 'as', 'from', 'cast', 'copy',
    'not', 'and', 'or', 'like', 'in',
    'count', 'having', 'distinct', 'all',
    'order', 'from', 'by', 'group', 'show',
    'options', 'create', 'table', 'with', 'drop', 'to',
}

operators = {
    ',', '.', '(', ')',
    '*', '/', '%',
    '+', '-',
    '||',
    '+', '-', '&', '|', '^',
    '=', '!=', '>', '<', '>=', '<=', '<>', '!>', '!<',
}

integer = m.construct(a.Integer, m.keyword(value=regex_token(integer_format)))

string = m.construct(a.String, m.keyword(value=base_string()))

base_name = m.any(
    m.pred(lambda v: v not in keywords and re.match(name_format, v)),
    base_string('"'),
)

name = m.transform(
    lambda *parts: [a.Name('.'.join(*parts))],
    m.sequence(
        m.optional(m.sequence(base_name, svtok('.'))),
        m.optional(m.sequence(base_name, svtok('.'))),
        base_name,
    )
)


@m.define
def value(value):
    value = m.any(
        m.sequence(svtok('('), value, svtok(')')),
        cast_expression, count_all, call_analytics_function, call_set_function, call,
        integer, string, name
    )

    value = unary_op(value, '+', '-')
    value = binary_op(value, '*', '/', '%')
    value = binary_op(value, '||')
    value = binary_op(value, '+', '-', '&', '|', '^')
    value = binary_op(value, '=', '!=', '>', '<', '>=', '<=', '<>', '!>', '!<')
    value = unary_op(value, 'not')
    value = binary_op(value, 'and')

    value = m.transform(
        build_binary_tree,
        m.list_of(
            m.any(
                compound_token('not', 'like'),
                compound_token('not', 'in'),
                verbatim_token('in', 'or', 'like'),
            ),
            value
        )
    )

    return value


cast_expression = m.construct(
    a.Cast,
    svtok('cast'), svtok('('),
    m.keyword(value=value),
    svtok('as'),
    m.keyword(type=value),
    svtok(')')
)

call_set_function = m.construct(
    a.CallSetFunction,
    m.keyword(func=verbatim_token(
        'avg', 'max', 'min', 'sum', 'every', 'any', 'some'
        'count', 'stddev_pop', 'stddev_samp', 'var_samp', 'var_pop',
        'collect', 'fusion', 'intersection', 'count', 'first_value',
    )),
    svtok('('),
    m.optional(m.keyword(quantifier=verbatim_token('distinct', 'all'))),
    m.keyword(args=m.transform(lambda t: [t], value)),
    svtok(')'),
)

count_all = m.construct(
    a.Call,
    m.keyword(func=verbatim_token('count')),
    svtok('('),
    m.keyword(args=m.transform(lambda t: [t], verbatim_token('*'))),
    svtok(')'),
)

call = m.construct(
    a.Call,
    m.keyword(func=base_name),
    svtok('('),
    m.any(
        m.keyword(args=m.list_of(svtok(','), value)),
        m.keyword(args=m.literal([]))
    ),
    svtok(')'),
)

order_by_item = m.construct(
    a.OrderBy,
    m.keyword(value=value),
    m.keyword(order=m.any(verbatim_token('desc', 'asc'), m.literal('desc'))),
)

order_by_clause = m.sequence(svtok('order'), svtok('by'), m.list_of(svtok(','), order_by_item))
partition_by_clause = m.sequence(svtok('partition'), svtok('by'), m.list_of(svtok(','), value))

call_analytics_function = m.construct(
    a.CallAnalyticsFunction,
    m.keyword(call=call),
    svtok('over'), svtok('('),
    m.optional(m.keyword(partition_by=partition_by_clause)),
    m.optional(m.keyword(order_by=order_by_clause)),
    svtok(')')
)

alias = m.sequence(
    m.optional(svtok('as')),
    base_name,
)

column = m.construct(
    a.Column,
    m.keyword(value=value),
    m.optional(m.keyword(alias=alias)),
)

table_ref = m.construct(
    a.TableRef,
    m.sequence(
        m.optional(m.sequence(m.keyword(schema=base_name), svtok('.'))),
        m.keyword(name=base_name),
        m.optional(m.keyword(alias=alias)),
    )
)

from_clause = m.construct(
    a.FromClause,
    svtok('from'),
    m.keyword(tables=m.list_of(m.ignore(svtok(',')), table_ref))
)

select = m.construct(
    a.Select,
    svtok('select'),
    m.optional(m.keyword(quantifier=verbatim_token('distinct', 'all'))),
    m.keyword(columns=m.list_of(svtok(','), m.any(
        m.construct(
            a.WildCard,
            m.optional(m.sequence(m.keyword(table=base_name), svtok('.'))),
            m.ignore(verbatim_token('*'))
        ),
        column
    ))),
    m.optional(m.keyword(from_clause=from_clause)),
    m.optional(m.keyword(where_clause=m.sequence(svtok('where'), value))),
    m.optional(m.keyword(group_by_clause=m.sequence(
        svtok('group'), svtok('by'), m.list_of(svtok(','), value),
    ))),
    m.optional(m.keyword(having_clause=m.sequence(svtok('having'), value))),
    m.optional(m.keyword(order_by_clause=m.sequence(
        svtok('order'), svtok('by'), m.list_of(svtok(','), order_by_item),
    )))
)

name_value_pair = m.construct(
    lambda name, value: (name, value), m.keyword(name=name), m.keyword(value=value)
)

copy_from = m.construct(
    a.CopyFrom,
    svtok('copy'),
    m.keyword(name=name),
    svtok('from'),
    m.keyword(filename=value),
    svtok('with'),
    m.keyword(options=m.list_of(svtok(','), name_value_pair))
)

copy_to = m.construct(
    a.CopyTo,
    svtok('copy'),
    m.keyword(name=name),
    svtok('to'),
    m.keyword(filename=value),
    svtok('with'),
    m.keyword(options=m.list_of(svtok(','), name_value_pair))
)

drop_tabe = m.construct(
    a.DropTable,
    svtok('drop'), svtok('table'),
    m.keyword(names=m.list_of(svtok(','), name)),
)

create_table_as = m.construct(
    a.CreateTableAs,
    svtok('create'), svtok('table'),
    m.keyword(name=name),
    svtok('as'),
    m.keyword(query=select),
)


def show_option(seq):
    if seq[:1] != ['show']:
        return None, seq, {}

    return [a.Show(seq[1:])], [], {}


parser = m.any(
    select,
    copy_from,
    copy_to,
    drop_tabe,
    create_table_as,
    show_option,
)

constructors = {
    constructor.cls: constructor
    for constructor in [
        select, from_clause, column, integer, table_ref, call, call_set_function,
    ]
}

constructors[a.Name] = name

splitter = m.repeat(
    m.any(
        # NOTE: do not use str.lower, due to py2 compat
        full_word(m.map_verbatim(lambda s: s.lower(), *keywords)),
        m.map_verbatim(lambda s: s.lower(), *operators),
        m.regex(name_format),
        m.ignore(m.regex(r'\s+')),
        m.regex(integer_format),
        m.string('\''),
        m.string('"')
    )
)
