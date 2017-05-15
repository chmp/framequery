from __future__ import print_function, division, absolute_import

import re

import pandas as pd


def like(s, pattern):
    """Execute a SQL ``like`` expression against a str-series."""
    pattern = re.escape(pattern)
    pattern = pattern.replace(r'\%', '.*')
    pattern = pattern.replace(r'\_', '.')
    pattern = '^' + pattern + '$'

    # sqlite is case insenstive, is this always the case?
    return pd.Series(s).str.contains(pattern, flags=re.IGNORECASE)


def not_like(s, pattern):
    """Execute a SQL ``not like`` expression against a str-series."""
    res = like(s, pattern)
    # handle inversion with missing numbers
    res = (1 - res).astype(res.dtype)
    return res
