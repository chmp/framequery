from __future__ import print_function, division, absolute_import

from framequery.executor import query

import pandas as pd
import pandas.util.testing as pdt

import pytest


scope = dict(
    example=pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6], 'g': [0, 0, 1]}),
)


def test_example():
    actual = query('select * from example', scope=scope)
    expected = scope['example'].copy()

    pdt.assert_frame_equal(actual, expected, check_dtype=False)


def test_example2():
    actual = query('select * from example order by a desc', scope=scope)
    expected = scope['example'].copy().sort_values('a', ascending=False)

    pdt.assert_frame_equal(actual, expected, check_dtype=False)


def test_example3():
    actual = query('select a + b as c from example', scope=scope)
    expected = scope['example'].copy()
    expected = pd.DataFrame({'c': expected['a'] + expected['b']})

    print(actual)
    pdt.assert_frame_equal(actual, expected, check_dtype=False)


def test_example4():
    actual = query('''
        select
            g, sum(a) as a, min(b) as b
        from example
        group by g
    ''', scope=scope)

    expected = pd.DataFrame([
        [0, 3, 4],
        [1, 3, 6]
    ], columns=['g', 'a', 'b'])

    pdt.assert_frame_equal(actual, expected, check_dtype=False)


def test_example5():
    actual = query('''
        select
            2 * g as gg, sum(a) as a, min(b) as b
        from example
        group by gg
    ''', scope=scope)

    expected = pd.DataFrame([
        [0, 3, 4],
        [2, 3, 6]
    ], columns=['gg', 'a', 'b'])

    pdt.assert_frame_equal(actual, expected, check_dtype=False)


def test_example6():
    actual = query('''
        select
            2 * g, sum(a) as a, min(b) as b
        from example
        group by 2 * g
    ''', scope=scope)

    expected = pd.DataFrame([
        [0, 3, 4],
        [2, 3, 6]
    ], columns=['g', 'a', 'b'])

    assert actual.columns[0].startswith('unique')
    assert list(actual.columns[1:]) == ['a', 'b']
    actual.columns = expected.columns

    pdt.assert_frame_equal(actual, expected, check_dtype=False)