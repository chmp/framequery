from __future__ import print_function, division, absolute_import

import dask.dataframe as dd
import pandas as pd
import pandas.util.testing as pdt

import framequery as fq

import pytest

ddl = [
    'drop table if exists test',
    'create table test (c1 int, c2 int)'
]

columns = [('c1', 'int'), ('c2', 'int')]

data = [
    (0, 1),
    (1, 2),
    (0, 3),
    (1, 4),
    (0, 5),
    (1, 6),
]


@pytest.fixture
def setup(database):
    for q in ddl:
        database.execute(q)
    database.execute('insert into public.test (c1, c2) values(%s, %s)', *data)

    scope = {'test': pd.DataFrame(data, columns=['c1', 'c2'])}
    return database, scope


examples = [
    'select c1, count(1) as cnt, sum(c2) from test group by c1',
    'select c1 as a, c2 as b, c1 + c2 from test',
    'select test.* from test',
    'select test.c1, 2 * test.c2 from test',
    'select "c1", "test"."c2" from test',

    # test case sensitivity
    r'''select 'Foo' like '%oo' ''',
    r'''select 'Foo' like '%OO' ''',
    r'''select upper('Foo') like '%OO' ''',
    r'''select 'Foo' like lower('%OO') ''',
]

dask_xfail_examples = [
    r"""select * from json_each(cast('{"foo": "bar", "hello": "world"}' as json)) """,
    r"""select * from json_each('{"foo": "bar", "hello": "world"}' :: json)""",
]

examples = (
    [('pandas', q) for q in examples] +
    [('dask', q) for q in examples] +
    [('pandas', q) for q in dask_xfail_examples] +
    [pytest.mark.xfail()(('dask', q)) for q in dask_xfail_examples]
)


@pytest.mark.parametrize('model, query', examples)
def test_select(setup, model, query):
    db, scope = setup

    if model == 'dask':
        scope = {k: dd.from_pandas(df, npartitions=3) for (k, df) in scope.items()}

    actual = fq.execute(query, scope, model=model)

    expected = _norm_result(db.execute(query.replace('%', '%%')).fetchall())
    actual = _norm_result(row for _, row in actual.iterrows())

    pdt.assert_frame_equal(actual, expected, check_dtype=False)


def _norm_result(iterable):
    return pd.DataFrame(sorted(list(row) for row in iterable))
