from __future__ import print_function, division, absolute_import

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from framequery.util import dask_sort_values


@pytest.mark.parametrize('npartitions', [1, 10])
def test_dask_sort(npartitions):
    vals = list(reversed(range(100)))
    df = pd.DataFrame({'val': vals})
    ddf = dd.from_pandas(df, npartitions=npartitions)

    np.testing.assert_allclose(
        dask_sort_values(ddf, 'val').val.compute(),
        df.sort_values('val').val,
    )
