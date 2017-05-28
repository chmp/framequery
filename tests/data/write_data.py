from __future__ import print_function, division, absolute_import

import os.path

import pandas as pd

df = pd.DataFrame({
    'g': [0, 0, 0, 1, 1, 2],
    'i': [1, 2, 3, 4, 5, 6],
    'f': [7.0, 8.0, 9.0, 0.0, 1.0, 2.0],
})

df.to_csv(
    os.path.join(os.path.dirname(__file__), 'test.csv'),
    sep=';',
    index=False,
)
