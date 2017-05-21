from __future__ import print_function, division, absolute_import

import dask.dataframe as dd
import pandas as pd
import pandas.util.testing as pdt

import framequery as fq

q = """
    SELECT country, sum(sales) as sales

    FROM sales

    JOIN stores
    ON sales.store_id = stores.id

    GROUP BY country
"""


def test_dask_convert_dataframes():
    """The dask executor auto-converts any pandas dataframes."""
    stores = pd.DataFrame({
        'country': [0, 0, 1, 1],
        'id': [1, 2, 3, 4],
    })

    sales = pd.DataFrame({
        'store_id': [1, 2, 3, 4],
        'sales': [5, 6, 7, 8]
    })

    sales_by_country = fq.execute(q, scope={'stores': stores, 'sales': sales}, model="dask")

    assert type(sales_by_country) is dd.DataFrame
    sales_by_country = sales_by_country.compute()

    pdt.assert_frame_equal(
        sales_by_country,
        pd.DataFrame({
            'country': [0, 1],
            'sales': [11, 15],
        }),
    )
