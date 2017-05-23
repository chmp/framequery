from __future__ import print_function, division, absolute_import

import collections
import functools as ft
import json
import operator as op
import os.path
import re

import pandas as pd
from pandas.core.dtypes.api import is_scalar


def escape_parameters(params):
    if isinstance(params, dict):
        return {k: escape(v) for k, v in params.items()}

    elif isinstance(params, tuple):
        return tuple(escape(v) for v in params)

    else:
        raise NotImplementedError('cannot escape parameters of type %s' % type(params))


def escape(val):
    if val is None:
        return 'null'

    elif isinstance(val, str):
        return "'" + val.replace("'", "''") + "'"

    elif isinstance(val, (int, bool, float)):
        return json.dumps(val)

    else:
        raise NotImplementedError()


def like(s, pattern):
    """Execute a SQL ``like`` expression against a str-series."""
    pattern = re.escape(pattern)
    pattern = pattern.replace(r'\%', '.*')
    pattern = pattern.replace(r'\_', '.')
    pattern = '^' + pattern + '$'

    # sqlite is case insenstive, is this always the case?
    if is_scalar(s):
        return re.match(pattern, s) is not None

    else:
        return s.str.contains(pattern)


def not_like(s, pattern):
    """Execute a SQL ``not like`` expression against a str-series."""
    res = like(s, pattern)

    if is_scalar(s):
        return not res

    else:
        # handle inversion with missing numbers
        return (1 - res).astype(res.dtype)


def trim(what, characters, s):
    s = _str_funcs(s)

    if what == 'leading':
        return s.lstrip(characters)

    elif what == 'trailing':
        return s.rstrip(characters)

    elif what == 'both':
        return s.strip(characters)

    raise ValueError('unknown trim mode %s' % what)


def position(needle, haystack):
    return _str_funcs(haystack).find(needle) + 1


def upper(s):
    return _str_funcs(s).upper()


def lower(s):
    return _str_funcs(s).lower()


def concat(head, *tail):
    strings = [head] + list(tail)
    strings = [_fillna(s, '') for s in strings]
    return ft.reduce(op.add, strings)


def _str_funcs(s):
    return s if is_scalar(s) else pd.Series(s).str


def _fillna(obj, missing):
    if is_scalar(obj):
        return obj if obj is not None else missing

    return pd.Series(obj).fillna(missing)


def cast_json(obj):
    if not isinstance(obj, str):
        raise ValueError('cannot cast %r to json' % obj)

    return json.loads(obj)


def copy_from(filename, *args):
    options = dict(zip(args[:-1:2], args[1::2]))

    format = options.pop('format', 'csv')

    if format == 'csv':
        filename = os.path.abspath(filename)

        if 'delimiter' in options:
            options['sep'] = options.pop('delimiter')

        return pd.read_csv(filename, **options)

    else:
        raise RuntimeError('unknown format %s' % format)


def json_each(obj):
    if not obj:
        return pd.DataFrame(columns=['key', 'value'])

    if not isinstance(obj, collections.Mapping):
        raise ValueError('cannot expand non-mapping value: %r' % obj)

    items = list(obj.items())

    return pd.DataFrame({
        'key': [key for key, _ in items],
        'value': [val for _, val in items],
    }, columns=['key', 'value'])


def json_array_elements(obj):
    if not obj:
        return pd.DataFrame(columns='value')

    if not isinstance(obj, collections.Sequence) or isinstance(obj, str):
        raise ValueError('cannot get array elements from %r' % obj)

    return pd.DataFrame({
        'value': [val for val in obj]
    })


def generate_series(start, stop, step=1):
    """Similar to python's range, but with the end-point included.

    See: [series generating functions](https://www.postgresql.org/docs/9.6/static/functions-srf.html).
    """
    values = [] if None in {start, stop, step} else list(range(start, stop + step, step))
    return pd.DataFrame({'generate_series': values})


def make_meta(items):
    d = collections.OrderedDict()
    for k, t in items:
        d[k] = pd.Series([], dtype=t)

    return pd.DataFrame(d, index=[])
