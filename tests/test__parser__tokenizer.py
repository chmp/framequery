from __future__ import print_function, division, absolute_import

import pytest
from framequery.parser import tokenize


examples = [
    ('select 1', ['select', '1']),
    ('select 1 as a, 2 as b', ['select', '1', 'as', 'a', ',', '2', 'as', 'b']),
    ("select 'foo bar' ", ['select', "'foo bar'"]),
    (r"select 'foo''bar' ", ['select', r"'foo''bar'"]),
    ('select a + b from public.test', ['select', 'a', '+', 'b', 'from', 'public', '.', 'test']),
    ('select a+b from public.test', ['select', 'a', '+', 'b', 'from', 'public', '.', 'test']),
    ('select a>=b from public.test', ['select', 'a', '>=', 'b', 'from', 'public', '.', 'test']),
    ('select (a + b) from public.test', ['select', '(', 'a', '+', 'b', ')', 'from', 'public', '.', 'test']),

    # NOTE: keywords are lower cased
    ('SELECT (a + b) FrOM Public.Test', ['select', '(', 'a', '+', 'b', ')', 'from', 'Public', '.', 'Test']),

    ('select +foo * -bar', ['select', '+', 'foo', '*', '-', 'bar']),
    ('SELECT', ['select']),
]


@pytest.mark.parametrize('query, parts', examples)
def test_split_examples(query, parts):
    assert tokenize(query) == parts
