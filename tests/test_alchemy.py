from __future__ import print_function, division, absolute_import

import pandas as pd
from sqlalchemy import create_engine


def test_create_engine_connect():
    engine = create_engine('framequery+pandas:///')

    with engine.connect():
        pass


def test_add_dataframe_query():
    engine = create_engine('framequery+pandas:///')
    engine.execute('!update', foo=pd.DataFrame({'foo': [0, 1, 2]}))

    assert engine.execute('select * from foo').fetchall() == [(0,), (1,), (2,)]


def test_add_dataframe_query__transaction():
    engine = create_engine('framequery+pandas:///')
    engine.execute('!update', foo=pd.DataFrame({'foo': [0, 1, 2]}))

    with engine.begin() as conn:
        assert conn.execute('select * from foo').fetchall() == [(0,), (1,), (2,)]
