from __future__ import print_function, division, absolute_import

import numpy as np
import pandas as pd
import framequery as fq

import pytest

ddl = [
    'drop table if exists test',
    '''
        create table test (
            c1 int,
            c2 int
        )
    '''
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

    df = pd.DataFrame(data, columns=['c1', 'c2'])
    scope = {'test': df}

    return database, scope


examples = [
    dict(
        query='select c1, count(1) as cnt, sum(c2) from test group by c1',
    ),
]


@pytest.mark.parametrize('case', examples)
def test_select(setup, case):
    db, scope = setup

    q = case['query']

    expected = sorted(list(row) for row in db.execute(q).fetchall())

    actual = fq.query(q, scope=scope)
    actual = sorted(list(t) for _, t in actual.iterrows())

    np.testing.assert_allclose(actual, expected)
