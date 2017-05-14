from __future__ import print_function, division, absolute_import

import pytest

from framequery.parser.ast import *
from framequery.parser import parse

examples = [
    ('select * from test', Select(['*'], FromClause([TableRef('test')]))),
    ('select * from test order by a', Select(
        ['*'], FromClause([TableRef('test')]),
            order_by_clause=[OrderBy(Name('a'), 'desc')],
    )),
    ('select 1', Select([Column(Integer('1'))])),
    ('select 1, 2', Select([Column(Integer('1')), Column(Integer('2'))])),
    ('select 1 as b', Select([Column(Integer('1'), 'b')])),
    ('select 1 as a, 2 as b', Select([Column(Integer('1'), 'a'), Column(Integer('2'), 'b')])),
    ('select 1 a, 2 b', Select([Column(Integer('1'), 'a'), Column(Integer('2'), 'b')])),
    ('select 2 from test', Select([Column(Integer('2'))], FromClause([TableRef('test')]))),
    ('select foo from test', Select([Column(Name('foo'))], FromClause([TableRef('test')]))),
    ("select 'foo bar'", Select([Column(String("'foo bar'"))])),
    ('select foo + bar from test', Select(
        [Column(BinaryOp('+', Name('foo'), Name('bar')))],
        FromClause([TableRef('test')]),
    )),

    ('select +foo', Select([Column(UnaryOp('+', Name('foo')))])),
    ('select +foo * -bar', Select([
        Column(
            BinaryOp('*',
                 UnaryOp('+', Name('foo')),
                 UnaryOp('-', Name('bar'))
            )
        )
    ])),
    ('select foo and bar', Select([Column(BinaryOp('and', Name('foo'), Name('bar')))])),
    ('select (foo and bar) or baz', Select([
        Column(
            BinaryOp('or',
                BinaryOp('and', Name('foo'), Name('bar')),
                Name('baz'),
            ),
        )
    ])),
    ('select foo not like bar', Select([Column(BinaryOp('not like', Name('foo'), Name('bar')))])),
    ('select foo not in bar', Select([Column(BinaryOp('not in', Name('foo'), Name('bar')))])),
    ('select not foo = bar', Select([Column(UnaryOp('not', BinaryOp('=', Name('foo'), Name('bar'))))])),

    ('select count(*)', Select([Column(Call('count', ['*']))])),

    ('select sum(foo) over()', Select([
        Column(
            CallAnalyticsFunction(Call('sum', [Name('foo')]))
        )
    ])),

    ('select sum(foo) over(order by bar, baz)', Select([
        Column(
            CallAnalyticsFunction(
                call=Call('sum', [Name('foo')]),
                order_by=[
                    OrderBy(Name('bar'), 'desc'),
                    OrderBy(Name('baz'), 'desc'),
                ]
            )
        )
    ])),

    ('select sum(foo) over(partition by hello, world order by bar, baz)', Select([
        Column(
            CallAnalyticsFunction(
                Call('sum', [Name('foo')]),
                partition_by=[Name('hello'), Name('world')],
                order_by=[
                    OrderBy(Name('bar'), 'desc'),
                    OrderBy(Name('baz'), 'desc'),
                ],
            )
        )
    ])),

]


@pytest.mark.parametrize('query,ast', examples)
def test_parse_exaples(query, ast):
    assert parse(query) == ast


def test_parse_table_ref():
    assert parse('test', TableRef) == TableRef('test')
    assert parse('public.test', TableRef) == TableRef('test', 'public')
    assert parse('public.test as foo', TableRef) == TableRef('test', 'public', 'foo')


def test_parse_from_clause():
    assert parse('from test', FromClause) == FromClause([
        TableRef('test')
    ])

    assert parse('from public.test, public.test as foo', FromClause) == FromClause([
        TableRef('test', 'public'),
        TableRef('test', 'public', 'foo'),
    ])


def test_parse_call():
    assert parse('test()', Call) == Call('test', [])
    assert parse('test(1, bar)', Call) == Call('test', [Integer('1'), Name('bar')])
    assert parse('test(1, bar(2, 3))', Call) == Call('test', [
        Integer('1'),
        Call('bar', [Integer('2'), Integer('3')]),
    ])


def test_parse_call_set_function():
    assert parse('min(foo)', CallSetFunction) == CallSetFunction('min', [Name('foo')])
