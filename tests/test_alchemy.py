from __future__ import print_function, division, absolute_import

from sqlalchemy import create_engine


def test_create_engine_connect():
    engine = create_engine('framequery+pandas:///')

    with engine.connect():
        pass
