from __future__ import print_function, division, absolute_import

import framequery as fq

import dask.dataframe as dd
import pandas as pd
import pandas.util.testing as pdt

import pytest


scope = dict(
    example=pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6], 'g': [0, 0, 1]}),
)


examples = [
    ('select * from example', lambda: scope['example'].copy()),
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
    ),
    (
        'select * from example order by a desc',
        lambda: scope['example'].copy().sort_values('a', ascending=False),
    ),
]

examples_dask_fail = []

examples = (
    [('pandas',) + spec for spec in examples + examples_dask_fail] +
    [('dask',) + spec for spec in examples] +
    [pytest.mark.xfail(reason='not supported by dask')(('dask',) + spec)
     for spec in examples_dask_fail
     ]
)


@pytest.mark.parametrize('model, query, expected', examples)
def test_example(query, expected, model):
    if model == 'dask':
        sc = {k: dd.from_pandas(df, npartitions=3) for k, df in scope.items()}
        actual = fq.execute(query, scope=sc, model=model)
        actual = actual.compute()

    else:
        actual = fq.execute(query, scope=scope, model=model)

    expected = expected()

    # set empty columns in expected to the ones in actual
    expected.columns = [e or a for a, e in zip(actual.columns, expected.columns)]

    actual = actual.reset_index(drop=True)
    expected = actual.reset_index(drop=True)

    pdt.assert_frame_equal(actual, expected, check_dtype=False)


def test_readme_example():
    stores = pd.DataFrame({
        'country': [0, 0, 1, 1],
        'id': [1, 2, 3, 4],
    })

    sales = pd.DataFrame({
        'store_id': [1, 2, 3, 4],
        'sales': [5, 6, 7, 8]
    })

    # usage to prevent flake8 message
    print("shapes: ", stores.shape, sales.shape)

    import framequery as fq

    sales_by_country = fq.execute("""
        SELECT country, sum(sales) as sales

        FROM sales

        JOIN stores
        ON sales.store_id = stores.id

        GROUP BY country
    """)

    pdt.assert_frame_equal(
        sales_by_country,
        pd.DataFrame({
            'country': [0, 1],
            'sales': [11, 15],
        }),
    )
