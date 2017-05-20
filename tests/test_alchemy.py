from __future__ import print_function, division, absolute_import

import os.path

import pandas as pd
import pytest
from sqlalchemy import create_engine

from framequery import util


@pytest.mark.parametrize('qs', ['', '?model=dask'])
def test_create_engine_connect(qs):
    engine = create_engine('framequery:///' + qs)

    with engine.connect():
        pass


def test_add_dataframe_query():
    engine = create_engine('framequery:///')
    engine.executor.update(foo=pd.DataFrame({'foo': [0, 1, 2]}))

    assert engine.execute('select * from foo').fetchall() == [(0,), (1,), (2,)]


def test_add_dataframe_query__transaction():
    engine = create_engine('framequery:///')
    engine.executor.update(foo=pd.DataFrame({'foo': [0, 1, 2]}))

    with engine.begin() as conn:
        assert conn.execute('select * from foo').fetchall() == [(0,), (1,), (2,)]


@pytest.mark.parametrize('qs', ['', '?model=dask'])
def test_scope_files(qs):
    fname = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'scope.json'))
    engine = create_engine('framequery:///' + fname + qs)

    assert engine.table_names() == ['foo']

    with engine.begin() as conn:
        actual = conn.execute('select g, sum(i) from foo group by g').fetchall()
        actual = sorted(actual)

        assert actual == [(0, 6), (1, 9), (2, 6)]


@pytest.mark.parametrize('qs', [
    '',
    pytest.mark.xfail(reason='copy to not yet supported')('?model=dask'),
])
def test_scope_load_and_save(tmpdir, qs):
    source = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'test.csv'))
    target = os.path.join(str(tmpdir), 'test.csv')

    engine = create_engine('framequery:///' + qs)

    for q in [
        "COPY foo FROM '{}' WITH delimiter ';', format 'csv'  ".format(source),
        "CREATE TABLE bar AS select g, sum(i) from foo group by g",
        "COPY bar TO '{}' WITH delimiter ';', format 'csv'".format(target),
        "DROP TABLE bar",
    ]:
        engine.execute(q)

    assert engine.table_names() == ['foo']
    actual = pd.read_csv(target, sep=";")
    actual = sorted(tuple(row) for _, row in actual.iterrows())

    assert actual == [(0, 6), (1, 9), (2, 6)]


@pytest.mark.parametrize('qs', ['', '?model=dask'])
def test_scope_table_valued(qs):
    source = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'test.csv'))

    q = '''
        select g, sum(i)
        from copy_from('{}', 'delimiter', ';', 'format', 'csv')
        group by g
    '''.format(source)

    engine = create_engine('framequery:///' + qs)
    actual = engine.execute(q).fetchall()
    actual = sorted(actual)

    assert actual == [(0, 6), (1, 9), (2, 6)]


@pytest.mark.parametrize('val', [
    'foo',
    "bar'baz",
    1,
    4,
    -42.0,
    None, False, True,
])
def test_escape_roundtrib(val):
    """test query binding + escaping"""
    engine = create_engine('framequery:///')
    assert engine.execute('select %s', util.escape(val)).scalar() == val
