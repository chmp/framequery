from __future__ import print_function, division, absolute_import

import re

import pytest

from framequery.parser import ast as a, parse
from framequery.parser import _parser as p

examples = [
    ('select * from test', a.Select([a.WildCard()], a.FromClause([a.TableRef('test')]))),
    ('select * from test order by a', a.Select(
        [a.WildCard()], a.FromClause([a.TableRef('test')]),
        order_by_clause=[a.OrderBy(a.Name('a'), 'desc')],
    )),
    ('select 1', a.Select([a.Column(a.Integer('1'))])),
    ('select 1, 2', a.Select([a.Column(a.Integer('1')), a.Column(a.Integer('2'))])),
    ('select 1 as b', a.Select([a.Column(a.Integer('1'), 'b')])),
    ('select 1 as a, 2 as b', a.Select([a.Column(a.Integer('1'), 'a'), a.Column(a.Integer('2'), 'b')])),
    ('select 1 a, 2 b', a.Select([a.Column(a.Integer('1'), 'a'), a.Column(a.Integer('2'), 'b')])),
    ('select 2 from test', a.Select([a.Column(a.Integer('2'))], a.FromClause([a.TableRef('test')]))),
    ('select foo from test', a.Select([a.Column(a.Name('foo'))], a.FromClause([a.TableRef('test')]))),
    ("select 'foo bar'", a.Select([a.Column(a.String("'foo bar'"))])),
    ('select foo + bar from test', a.Select(
        [a.Column(a.BinaryOp('+', a.Name('foo'), a.Name('bar')))],
        a.FromClause([a.TableRef('test')]),
    )),

    ('select +foo', a.Select([a.Column(a.UnaryOp('+', a.Name('foo')))])),
    ('select +foo * -bar', a.Select([
        a.Column(
            a.BinaryOp('*', a.UnaryOp('+', a.Name('foo')), a.UnaryOp('-', a.Name('bar')))
        )
    ])),
    ('select foo and bar', a.Select([a.Column(a.BinaryOp('and', a.Name('foo'), a.Name('bar')))])),
    ('select (foo and bar) or baz', a.Select([
        a.Column(
            a.BinaryOp(
                'or',
                a.BinaryOp('and', a.Name('foo'), a.Name('bar')),
                a.Name('baz'),
            ),
        )
    ])),
    ('select foo not like bar', a.Select([a.Column(a.BinaryOp('not like', a.Name('foo'), a.Name('bar')))])),
    ('select foo not in bar', a.Select([a.Column(a.BinaryOp('not in', a.Name('foo'), a.Name('bar')))])),
    ('select not foo = bar', a.Select([a.Column(a.UnaryOp('not', a.BinaryOp('=', a.Name('foo'), a.Name('bar'))))])),

    ('select count(*)', a.Select([a.Column(a.Call('count', ['*']))])),

    ('select sum(foo) over()', a.Select([
        a.Column(
            a.CallAnalyticsFunction(a.Call('sum', [a.Name('foo')]))
        )
    ])),

    ('select sum(foo) over(order by bar, baz)', a.Select([
        a.Column(
            a.CallAnalyticsFunction(
                call=a.Call('sum', [a.Name('foo')]),
                order_by=[
                    a.OrderBy(a.Name('bar'), 'desc'),
                    a.OrderBy(a.Name('baz'), 'desc'),
                ]
            )
        )
    ])),

    ('select sum(foo) over(partition by hello, world order by bar, baz)', a.Select([
        a.Column(
            a.CallAnalyticsFunction(
                a.Call('sum', [a.Name('foo')]),
                partition_by=[a.Name('hello'), a.Name('world')],
                order_by=[
                    a.OrderBy(a.Name('bar'), 'desc'),
                    a.OrderBy(a.Name('baz'), 'desc'),
                ],
            )
        )
    ])),

]


@pytest.mark.parametrize('query,ast', examples)
def test_parse_exaples(query, ast):
    assert parse(query) == ast


def test_parse_table_ref():
    assert parse('test', a.TableRef) == a.TableRef('test')
    assert parse('public.test', a.TableRef) == a.TableRef('test', 'public')
    assert parse('public.test as foo', a.TableRef) == a.TableRef('test', 'public', 'foo')


def test_parse_from_clause():
    assert parse('from test', a.FromClause) == a.FromClause([
        a.TableRef('test')
    ])

    assert parse('from public.test, public.test as foo', a.FromClause) == a.FromClause([
        a.TableRef('test', 'public'),
        a.TableRef('test', 'public', 'foo'),
    ])


def test_parse_call():
    assert parse('test()', a.Call) == a.Call('test', [])
    assert parse('test(1, bar)', a.Call) == a.Call('test', [a.Integer('1'), a.Name('bar')])
    assert parse('test(1, bar(2, 3))', a.Call) == a.Call('test', [
        a.Integer('1'),
        a.Call('bar', [a.Integer('2'), a.Integer('3')]),
    ])


def test_parse_call_set_function():
    assert parse('min(foo)', a.CallSetFunction) == a.CallSetFunction('min', [a.Name('foo')])


def test_parse_quoted_strings():
    assert parse(r"'{''foo'':''bar'', ''hello'': ''world''}'", a.String) == a.String(
        r"'{''foo'':''bar'', ''hello'': ''world''}'"
    )


@pytest.mark.parametrize('s', [
    '3.5',
    '4.',
    '.001',
    '5e2',
    '1.925e-3',
])
def test_float_format(s):
    assert re.match(p.float_format, s).group(0) == s
