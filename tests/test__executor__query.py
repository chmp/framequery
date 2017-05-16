from __future__ import print_function, division, absolute_import

import framequery as fq

import pandas as pd
import pandas.util.testing as pdt

import pytest


scope = dict(
    example=pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6], 'g': [0, 0, 1]}),
)

examples = [
    ('select * from example', lambda: scope['example'].copy()),
    (
        'select * from example order by a desc',
        lambda: scope['example'].copy().sort_values('a', ascending=False),
    ),
    (
        'select a + b as c from example',
        lambda: pd.DataFrame({'c': scope['example']['a'] + scope['example']['b']}),
    ),
    (
        '''
            select
                g, sum(a) as a, min(b) as b
            from example
            group by g
        ''',
        lambda: pd.DataFrame([
            [0, 3, 4],
            [1, 3, 6]
        ], columns=['g', 'a', 'b']),
    ),
    (
        '''
            select
                2 * g as gg, sum(a) as a, min(b) as b
            from example
            group by gg
        ''',
        lambda: pd.DataFrame([
            [0, 3, 4],
            [2, 3, 6]
        ], columns=['gg', 'a', 'b']),
    ),
    (
        '''
            select
                2 * g, sum(a) as a, min(b) as b
            from example
            group by 2 * g
        ''',
        lambda: pd.DataFrame([
            [0, 3, 4],
            [2, 3, 6]
        ], columns=['', 'a', 'b'])
    )
]


@pytest.mark.parametrize('query, expected', examples)
def test_example(query, expected):
    actual = fq.execute(query, scope=scope)
    expected = expected()

    # set empty columns in expected to the ones in actual
    expected.columns= [e or a for a, e in zip(actual.columns, expected.columns)]

    pdt.assert_frame_equal(actual, expected, check_dtype=False)
