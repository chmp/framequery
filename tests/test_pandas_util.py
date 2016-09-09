from pdsql._pandas_util import *

import pandas as pd
import pandas.util.testing as pdt


def test_ensure_table_columns():
    actual = ensure_table_columns("my_table", pd.DataFrame({
        "a": [1],
        "b": [2],
    }, index=[4]))

    expected = pd.DataFrame({
        ("my_table", "a"): [1],
        ("my_table", "b"): [2],
    }, index=[4])

    pdt.assert_frame_equal(actual, expected)


def test_strip_table_name_from_columns():
    actual = strip_table_name_from_columns(pd.DataFrame({
        ("my_table", "a"): [1],
        ("my_table", "b"): [2],
    }, index=[4]))

    expected = pd.DataFrame({
        "a": [1],
        "b": [2],
    }, index=[4])

    pdt.assert_frame_equal(actual, expected)
