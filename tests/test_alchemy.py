from __future__ import print_function, division, absolute_import

import os.path

import pandas as pd
from sqlalchemy import create_engine


def test_create_engine_connect():
    engine = create_engine('framequery://')

    with engine.connect():
        pass


def test_add_dataframe_query():
    engine = create_engine('framequery://')
    engine.execute('!update', foo=pd.DataFrame({'foo': [0, 1, 2]}))

    assert engine.execute('select * from foo').fetchall() == [(0,), (1,), (2,)]


def test_add_dataframe_query__transaction():
    engine = create_engine('framequery://')
    engine.execute('!update', foo=pd.DataFrame({'foo': [0, 1, 2]}))

    with engine.begin() as conn:
        assert conn.execute('select * from foo').fetchall() == [(0,), (1,), (2,)]


def test_scope_files():
    fname = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'scope.json'))
    engine = create_engine('framequery:///' + fname)

    assert engine.table_names() == ['foo']

    with engine.begin() as conn:
        actual = conn.execute('select g, sum(i) from foo group by g').fetchall()
        actual = sorted(actual)

        assert actual == [(0, 6), (1, 9), (2, 6)]
