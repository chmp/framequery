from __future__ import print_function, division, absolute_import

import string as _string

from framequery.util import _monadic as m
from . import ast as a


def tokenize(query):
    parts, rest, d = splitter(query)

    if rest != '':
        raise ValueError('extra tokens: {}, {}'.format(rest, d))

    return parts


def parse(query, what=a.Select):
    parser = constructors[what]

    tokens = tokenize(query)
    ast, rest, debug = parser(tokens)

    if rest:
        raise ValueError('extra tokens: {}\n{}'.format(tokens, '\n'.join(_format_debug(debug))))

    if len(ast) != 1:
        raise RuntimeError('internal parser error')

    return ast[0]


def _format_debug(debug, indent=0):
    status = debug['status']
    message = debug['message'] or ''
    children = debug['children']
    where = debug.get('where', '<unknown>')

    yield('{}{}: {} in {}'.format(' ' * indent,  status.value, message, where))

    for d in children:
        for msg in _format_debug(d, indent=indent + 1):
            yield msg


def verbatim_token(*p):
    return m.token(m.verbatim(*p))


def regex_token(p):
    return m.token(m.regex(p))


def svtok(*p):
    return m.skip(m.token(m.verbatim(*p)))


def full_word(matcher):
    @m._delegate(matcher, where='full_word')
    def full_word_impl(matches, s, d, seq):
        if not s or s[0] not in _string.ascii_letters:
            return matches, s, d

        return None, seq, dict(d, status=m.Status.failure)

    return full_word_impl


def base_name(seq):
    "hand crafted name parser to ensure names do not pass as a name"
    if not seq:
        return None, seq, m.Status.fail(message='no input', where='basename')

    if seq[0].lower() in keywords:
        return None, seq, m.Status.fail(message='string is a keyword', where='basename')

    return regex_token(name_format)(seq)


def base_string(seq):
    if not seq:
        return None, seq

    s = seq[0]
    if s[0] != "'" or s[-1] != "'":
        return None, seq, m.Status.fail(where='string', message='%r is not a string' % seq)

    return [s], seq[1:], m.Status.succeed()


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
        m.flat_sequence(*[verbatim_token(p) for p in parts]),
    )


integer_format = r'\d+'
name_format = r'\w+'

keywords = {
    'select', 'as', 'from',
    'not','and', 'or', 'like', 'in',
    'count', 'having', 'distinct', 'all',
    'order', 'from', 'by', 'group'
}

operators = {
    ',', '.', '(', ')',
    '*', '/', '%',
    '+', '-',
    '||',
    '+', '-', '&', '|', '^',
    '=', '!=', '>', '<', '>=', '<=', '<>', '!>', '!<',
}

integer = m.construct(
    a.Integer,
    m.keyword(value=regex_token(integer_format)),
)

string = m.construct(
    a.String,
    m.keyword(value=base_string)
)

name = m.construct(
    a.Name,
    m.keyword(name=base_name),
)


@m.define
def value(value):
    value = m.any(
        m.flat_sequence(svtok('('), value, svtok(')')),
        count_all, call_analytics_function, call_set_function, call,
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

order_by_clause = m.flat_sequence(svtok('order'), svtok('by'), m.list_of(svtok(','), order_by_item))
partition_by_clause = m.flat_sequence(svtok('partition'), svtok('by'), m.list_of(svtok(','), value))

call_analytics_function = m.construct(
    a.CallAnalyticsFunction,
    m.keyword(call=call),
    svtok('over'), svtok('('),
    m.optional(m.keyword(partition_by=partition_by_clause)),
    m.optional(m.keyword(order_by=order_by_clause)),
    svtok(')')
)

alias = m.flat_sequence(
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
    m.flat_sequence(
        m.optional(m.flat_sequence(m.keyword(schema=base_name), svtok('.'))),
        m.keyword(name=base_name),
        m.optional(m.keyword(alias=alias)),
    )
)

from_clause = m.construct(
    a.FromClause,
    svtok('from'),
    m.keyword(tables=m.list_of(m.skip(svtok(',')), table_ref))
)

select = m.construct(
    a.Select,
    svtok('select'),
    m.optional(m.keyword(quantifier=verbatim_token('distinct', 'all'))),
    m.keyword(columns= m.list_of(svtok(','), m.any(verbatim_token('*'), column))),
    m.optional(m.keyword(from_clause=from_clause)),
    m.optional(m.keyword(where_clause=m.flat_sequence(svtok('where'), value))),
    m.optional(m.keyword(group_by_clause=m.flat_sequence(
        svtok('group'), svtok('by'), m.list_of(svtok(','), value),
    ))),
    m.optional(m.keyword(having_clause=m.flat_sequence(svtok('having'), value))),
    m.optional(m.keyword(order_by_clause=m.flat_sequence(
        svtok('order'), svtok('by'), m.list_of(svtok(','), order_by_item),
    )))
)

constructors = {
    constructor.cls: constructor
    for constructor in [
        select, from_clause, column, integer, table_ref, name, call, call_set_function,
    ]
}

parser = select

splitter = m.many(
    m.any(
        full_word(m.map_verbatim(str.lower, *keywords)),
        m.map_verbatim(str.lower, *operators),
        m.regex(name_format),
        m.skip(m.regex(r'\s+')),
        m.regex(integer_format),
        m.string(),
    )
)
