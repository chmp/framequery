from __future__ import print_function, division, absolute_import

import collections

import pandas as pd
import pandas.util.testing as pdt

from framequery._dask_util import scalar_from_python, dataframe_from_scalars


def test_scalar_from_python():
    s = scalar_from_python(5)
    assert s.compute() == 5


def test_dataframe_from_scalars():
    df = dataframe_from_scalars(collections.OrderedDict([
        ('a', scalar_from_python(1)),
        ('b', scalar_from_python(2)),
    ]))

    pdt.assert_frame_equal(
        df.compute(),
        pd.DataFrame({
            'a': 1,
            'b': 2,
        }, index=[0], columns=['a', 'b'])
    )
