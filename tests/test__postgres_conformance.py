"""Tests to ensure that framequery and postgres understand the same SQL"""
from __future__ import print_function, division, absolute_import

import decimal

import dask.dataframe as dd
import pandas as pd
import pandas.util.testing as pdt

import framequery as fq
import framequery.util as util


import pytest

data = {
    'test': dict(
        columns=['c1', 'c2'],
        types=['int', 'int'],
        data=[
            (0, 1),
            (1, 2),
            (0, 3),
            (1, 4),
            (0, 5),
            (1, 6),
        ],
    ),
    'other': dict(
        columns=['c3', 'c4'],
        types=['int', 'int'],
        data=[
            (0, 7),
            (1, 8),
            (0, 9),
            (1, 0),
            (0, 1),
            (1, 2),
        ],
    ),
    'names': dict(
        columns=['name', 'id'],
        types=['text', 'int'],
        data=[
            ('foo', 0),
            ('bar', 1),
            ('baz', 2),
        ],
    ),
    'ages': dict(
        columns=['age', 'id'],
        types=['int', 'int'],
        data=[
            (20, 0),
            (30, 1),
            (40, 2),
        ],
    ),
}


@pytest.fixture(scope='module')
def setup(database):
    for name, desc in data.items():
        database.execute('drop table if exists %s' % name)

        coldef = ', '.join('%s %s' % (n, t) for n, t in zip(desc['columns'], desc['types']))
        database.execute('create table %s(%s)' % (name, coldef))

        cols = ', '.join(desc['columns'])
        database.execute('insert into %s (%s) values(%%s, %%s)' % (name, cols), *desc['data'])

    scope = {
        name: pd.DataFrame(desc['data'], columns=desc['columns'])
        for name, desc in data.items()
    }

    return database, scope


examples = [
    'select c1, count(1) as cnt, sum(c2) from test group by c1',
    'select c1, count(1) as cnt, sum(c2) from test group by 1',
    'select c1 as a, c2 as b, c1 + c2 from test',
    'select c1 as a, c2 as b, c1 + c2 from test where c1 = 0',
    '''
        with temp as (
            select
                c1 as a,
                c2 as b
            from test
        )

        select a + b from temp
    ''',
    'select test.* from test',
    'select count(*) from test group by 1 = 1',
    'select count(*) from test',
    'select test.c1, 2 * test.c2 from test',
    '''
        select
            c1, count(1) as cnt, sum(c2)
        from (
                select c1, 2 * c2 as c2
                from test
            ) sq
        group by c1
    ''',
    '''-- simple join
        select c2, c4
        from test
        join other
        on c1 = c3
    ''',
    '''-- joins as filters (left filter)
        select c2, c4
        from test
        join other
        on c1 = c3 and c1 = 0
    ''',
    '''-- joins as filters (right filter)
        select c2, c4
        from test
        join other
        on c1 = c3 and c3 = 0
    ''',
    '''-- join with transforms (left transform)
        select c2, c4
        from test
        join other
        on (c1 + 1) % 2 = c3
    ''',
    '''-- join with transforms (right transform)
        select c2, c4
        from test
        join other
        on c1 = (c3 + 1) % 2
    ''',
    '''-- cross-join with filter
        select c2, c4
        from test, other
        where c1 = (c3 + 1) % 2
    ''',
    '''-- join with inequality
        select c2, c4
        from test
        join other
        on c1 <= c3
    ''',
    '''
        select *
        from test
        left join other
        on c1 < c3
    ''',
    '''
        select *
        from other
        right join test
        on c1 < c3
    ''',
    '''
        select c2, c4
        from test
        left join other
        on c1 = (c3 + 1) % 2
    ''',
    '''
        select sum(c2), avg(c4)
        from test
        join other
        on c1 = c3
        group by c1
    ''',
    'select "c1", "test"."c2" from test',

    # test case sensitivity
    r'''select 'Foo' like '%oo' ''',
    r'''select 'Foo' like '%OO' ''',
    r'''select upper('Foo') like '%OO' ''',
    r'''select 'Foo' like lower('%OO') ''',
    r'''select concat('foo', null, 'bar')''',

    r"""select * from json_each(cast('{"foo": "bar", "hello": "world"}' as json)) """,
    r"""select * from json_each('{"foo": "bar", "hello": "world"}' :: json)""",
    r"""
        select b.key
        from
            json_array_elements('[{"foo": "bar"}]' :: json),
            lateral json_each(value) as b
        order by key
    """,
    """select trim(both 'xyz' from 'yxTomxx')""",
    """select position('f' in 'foo'), position('b' in 'foo')""",
    """select 'Post' || 'greSQL' """,

    """select true and false, true or false, true and not false """,

    """select 0 <> 1, 0 < 1, 1 > 2, 1 <= 2, 3 >= 4, 3 = 3, 4 != 4 """,

    '''
         select
             2 + 3, 2 - 3, 2 * 3, 4 / 2,
             5 % 4, 2.0 ^ 3.0, 91 & 15,
             32 | 3, 17 # 5, ~1, 1 << 4,
             8 >> 2
    ''',
    # '''-- not yet supported numeric operators
    #     select
    #         |/ 25.0, ||/ 27.0,
    #         5 !, !! 5, @ -5.0,
    # ''',
    'select * from test limit 3',
    'select * from test limit 3 offset 2',
    'select * from test offset 3',

    'select distinct c1 from test',
    'select all c1 from test',
    '''
        SELECT c2,
           CASE WHEN c2 = 1 THEN 'one'
                WHEN c2 = 2 THEN 'two'
                ELSE 'other'
           END
        FROM test
    ''',
    '''
        SELECT c2,
           CASE c2
               WHEN 1 THEN 'one'
               WHEN 2 THEN 'two'
               ELSE 'other'
           END
        FROM test
    ''',
    '''select false and true or true''',
    '''select c1, c2 from test order by 1, c2''',
    '''select * from test as a, test as b''',

    # TODO: extend operator precedence test
    '''
        select
            (2.0 + 3.0) ^ 2.0 + (1.0 - 2.0) ^ 2.0,
            2.0 * 3.0 ^ 2.0,
            2.0 * 4.0 + 3.0
    ''',

    '''
        select names.id, avg(age)
        from names
        join ages
        on names.id = ages.id
        group by 1
    ''',
]

dask_xfail_examples = []

xfail_examples = [
    '''
        select distinct on (c1)
            c1,
            c2,
            -c2 ^ c1 as c3
        from test
        order by c1, c2
    ''',
    '''
        -- order-by with a transformed column
        select *
        from test
        order by 4 * c1
    ''',
]

examples = (
    [('pandas', q) for q in examples] +
    [pytest.mark.xfail()(('pandas', q)) for q in xfail_examples] +
    [('pandas', q) for q in dask_xfail_examples] +

    [('dask', q) for q in examples] +
    [pytest.mark.xfail()(('dask', q)) for q in xfail_examples] +
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

    print('expected', expected, 'actual', actual, sep='\n')
    pdt.assert_frame_equal(actual, expected, check_dtype=False, check_less_precise=True)


def _norm_result(iterable):
    return (
        pd.DataFrame(
            list(_norm_value(v) for v in row)
            for row in iterable
        )
        .pipe(lambda df: df.sort_values(list(df.columns)))
        .reset_index(drop=True)
    )


def _norm_value(v):
    if isinstance(v, decimal.Decimal):
        return float(v)
    return v


@pytest.mark.parametrize('val', [
    'foo',
    "bar'baz",
    1, 4,
    -42.0,
    None, False, True,
])
def test_escape_roundtrib(database, val):
    """test the escape function"""
    assert database.execute('select ' + util.escape(val)).scalar() == val
