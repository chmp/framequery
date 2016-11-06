import framequery as fq
from framequery import make_context
from framequery._dask import DaskExecutor
from framequery._pandas import PandasExecutor

import dask.dataframe as dd
import pandas as pd
import pandas.util.testing as pdt

import pytest


@pytest.mark.parametrize('executor', [
    'pandas',
    pytest.mark.xfail(reason="dask does not support the dual table")('dask'),
])
def test_dual(executor):
    assert_eq(
        _context(executor=executor).select('SELECT * FROM DUAL'),
        pd.DataFrame(),
    )


@pytest.mark.parametrize('executor', [
    'pandas',
    pytest.mark.xfail(reason="dask does not support the dual table")('dask'),
])
def test_dual_two(executor):
    assert_eq(
        _context(executor=executor).select('SELECT 42 as a FROM DUAL'),
        pd.DataFrame({
            '$0.a': [42],
        }),
    )


@pytest.mark.parametrize('executor', [
    'pandas',
    pytest.mark.xfail(reason="dask does not support the dual table")('dask'),
])
@pytest.mark.parametrize('expr,expected', [
    ("'foo'", 'foo'),
    ("UPPER('foo')", 'FOO'),
    ("LOWER('FOO')", 'foo'),
    ("CONCAT('foo', 'BAR', 'baz')", 'fooBARbaz'),
    ("MID('abcdef', 2, 3)", 'bcd'),
    ("MID('abcdef', 2)", 'bcdef'),
    ("'foo' || 'bar'", 'foobar'),
    ("'foo' LIKE '%oo'", True),
    ("'foo' NOT LIKE '%oo'", False),
])
def test_string(executor, expr, expected):
    assert_eq(
        _context(executor=executor).select('''SELECT {} as a FROM DUAL'''.format(expr)),
        pd.DataFrame({
            '$0.a': [expected],
        }),
    )


@pytest.mark.parametrize('executor', [
    'pandas',
    pytest.mark.xfail(reason="dask does not support the dual table")('dask'),
])
def test_dual_no_as(executor):
    assert_eq(
        _context(executor=executor).select('SELECT 42 a FROM DUAL'),
        pd.DataFrame({
            '$0.a': [42],
        }),
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_simple_select(executor):
    assert_eq(
        _context(executor=executor).select('SELECT * FROM my_table'),
        pd.DataFrame({
            'my_table.a': [1, 2, 3] * 5,
            'my_table.b': [4, 5, 6] * 5,
            'my_table.c': [7, 8, 9] * 5,
            'my_table.g': [0, 0, 1] * 5,
            'my_table.one': [1, 1, 1] * 5,
        }),
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_simple_sum_cte(executor):
    assert_eq(
        _context(executor=executor).select('''
            WITH
                foo AS (
                    SELECT
                        a + b as a,
                        c + g as b
                    FROM my_table
                ),
                bar AS (
                    SELECT a + b as c
                    FROM foo
                )

            SELECT sum(c) as d FROM bar
        '''),
        pd.DataFrame({
            '$4.d': [(1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 1) * 5],
        }),
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_simple_subquery(executor):
    assert_eq(
        _context(executor=executor).select('SELECT * FROM (SELECT * FROM my_table)'),
        pd.DataFrame({
            'my_table.a': [1, 2, 3] * 5,
            'my_table.b': [4, 5, 6] * 5,
            'my_table.c': [7, 8, 9] * 5,
            'my_table.g': [0, 0, 1] * 5,
            'my_table.one': [1, 1, 1] * 5,
        }),
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_simple_filter(executor):
    assert_eq(
        _context(executor=executor).select('SELECT * FROM my_table WHERE g = 0'),
        pd.DataFrame({
            'my_table.a': [1, 2] * 5,
            'my_table.b': [4, 5] * 5,
            'my_table.c': [7, 8] * 5,
            'my_table.g': [0, 0] * 5,
            'my_table.one': [1, 1] * 5,
        }),
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
@pytest.mark.parametrize('agg_func,expected', [
    ('SUM', [45, 30]),
    ('MIN', [4, 6]),
    ('MAX', [5, 6]),
    ('FIRST_VALUE', [4, 6]),
    ('COUNT', [10, 5]),
    ('AVG', [4.5, 6]),
])
def test_evaluate_aggregation_grouped_no_as__separate(executor, agg_func, expected):
    q = 'SELECT g, {}(b) a FROM my_table GROUP BY g'.format(agg_func)
    assert_eq(
        _context(executor=executor).select(q),
        pd.DataFrame({
        '$2.g': [0, 1],
        '$2.a': expected,
        }, columns=['$2.g', '$2.a']),
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_evaluate_aggregation_grouped(executor):
    assert_eq(
        _context(executor=executor).select('SELECT g, SUM(b) as a, FIRST_VALUE(a) as fa FROM my_table GROUP BY g'),
        pd.DataFrame({
            '$2.g': [0, 1],
            '$2.a': [45, 30],
            '$2.fa': [1, 3],
        })[['$2.g', '$2.a', '$2.fa']],
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_evaluate_aggregation_grouped_no_as(executor):
    assert_eq(
        _context(executor=executor).select('SELECT g, SUM(b) a FROM my_table GROUP BY g'),
        pd.DataFrame({
            '$2.g': [0, 1],
            '$2.a': [45, 30],
        })[['$2.g', '$2.a']],
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
@pytest.mark.xfail(reason="not yet supported")
def test_evaluate_aggregation_grouped_no_as__transform(executor):
    assert_eq(
        _context(executor=executor).select('SELECT SUM(b) a FROM my_table GROUP BY g + g'),
        pd.DataFrame({
            '$2.a': [9, 6],
        })[['$2.a']],
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
@pytest.mark.xfail(reason="not yet supported")
def test_evaluate_aggregation_grouped_no_as__transform_alias(executor):
    assert_eq(
        _context(executor=executor).select('SELECT 2 * g as h, SUM(b) a FROM my_table GROUP BY h'),
        pd.DataFrame({
            '$2.h': [0, 2],
            '$2.a': [9, 6],
        })[['$2.h', '$2.a']],
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
@pytest.mark.xfail(reason="not yet supported")
def test_evaluate_aggregation_grouped__numeric(executor):
    assert_eq(
        _context(executor=executor).select('SELECT g, SUM(b) as a, FIRST_VALUE(a) as fa FROM my_table GROUP BY 1'),
        pd.DataFrame({
            '$2.g': [0, 1],
            '$2.a': [9, 6],
            '$2.fa': [1, 3],
        })[['$2.g', '$2.a', '$2.fa']],
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
@pytest.mark.xfail(reason="not yet supported")
def test_evaluate_aggregation_grouped__numeric_no_alias(executor):
    assert_eq(
        _context(executor=executor).select('SELECT 2 * g, SUM(b) as a, FIRST_VALUE(a) as fa FROM my_table GROUP BY 1'),
        pd.DataFrame({
            '$2.g': [0, 1],
            '$2.a': [9, 6],
            '$2.fa': [1, 3],
        })[['$2.g', '$2.a', '$2.fa']],
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_select_column(executor):
    assert_eq(
        _context(executor=executor).select('SELECT a as a FROM my_table'),
        pd.DataFrame({
            '$0.a': [1, 2, 3] * 5,
        }),
    )


@pytest.mark.parametrize('executor', [
    'pandas',
    pytest.mark.xfail(reason="dask does not support analytic functions")('dask'),
])
def test_select_column_analytics_function_sum(executor):
    assert_eq(
        _context(executor=executor).select('SELECT a as a, sum(a) OVER() as b FROM my_table'),
        pd.DataFrame({
            '$1.a': [1, 2, 3] *5,
            '$1.b': [30, 30, 30] * 5,
        }),
    )


@pytest.mark.parametrize('executor', [
    'pandas',
    pytest.mark.xfail(reason="dask does not support analytic functions")('dask'),
])
def test_select_column_analytics_function_sum_partition_by(executor):
    assert_eq(
        _context(executor=executor).select('SELECT a as a, sum(a) OVER(PARTITION BY g) as b FROM my_table'),
        pd.DataFrame({
            '$1.a': [1, 2, 3] * 5,
            '$1.b': [15, 15, 15] * 5,
        }),
    )


@pytest.mark.parametrize('executor', [
    'pandas',
    pytest.mark.xfail(reason='dask does not support analytic functions')('dask'),
])
def test_select_column_analytics_function_avg(executor):
    assert_eq(
        _context(executor=executor).select('SELECT a as a, avg(a) OVER() as b FROM my_table'),
        pd.DataFrame({
            '$1.a': [1, 2, 3] * 5,
            '$1.b': [2.0, 2.0, 2.0] * 5,
        }),
    )


@pytest.mark.parametrize('executor', [
    'pandas',
    pytest.mark.xfail(reason='dask does not support analytic functions')('dask'),
])
def test_select_column_analytics_function_avg_partition_by(executor):
    assert_eq(
        _context(executor=executor).select('SELECT a as a, avg(a) OVER(PARTITION BY g) as b FROM my_table'),
        pd.DataFrame({
            '$1.a': [1, 2, 3] * 5,
            '$1.b': [1.5, 1.5, 3] * 5,
        }),
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_select_column_without_rename(executor):
    assert_eq(
        _context(executor=executor).select('SELECT a FROM my_table'),
        pd.DataFrame({
            '$0.a': [1, 2, 3] * 5
        }),
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_simple_select_distinct(executor):
    assert_eq(
        _context(executor=executor).select('SELECT DISTINCT g, one FROM my_table'),
        pd.DataFrame({
            '$0.g': [0, 1],
            '$0.one': [1, 1],
        }),
    )


@pytest.mark.parametrize('executor', [
    'pandas',
    pytest.mark.xfail(reason='dask does not support limit')('dask'),
])
def test_select_column_without_rename_limit(executor):
    assert_eq(
        _context(executor=executor).select('SELECT a FROM my_table LIMIT 2'),
        pd.DataFrame({
            '$0.a': [1, 2]
        }),
    )

    assert_eq(
        _context(executor=executor).select('SELECT a FROM my_table LIMIT 1, 2'),
        pd.DataFrame({
            '$0.a': [2, 3]
        }),
    )

    assert_eq(
        _context(executor=executor).select('SELECT a FROM my_table LIMIT 2 OFFSET 1'),
        pd.DataFrame({
            '$0.a': [2, 3]
        }),
    )


@pytest.mark.parametrize('executor', [
    'pandas',
    pytest.mark.xfail(reason='dask does not support ordering')('dask'),
])
def test_order_by(executor):
    assert_eq(
        _context(executor=executor).select('SELECT a FROM my_table ORDER BY g, a ASC'),
        pd.DataFrame({
            '$0.a': [3] * 5 +  [1] * 5 + [2] * 5,
        }),
        sort=False,
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_simple_arithmetic(executor):
    assert_eq(
        _context(executor=executor).select('SELECT 2 * a as a FROM my_table'),
        pd.DataFrame({
            '$0.a': [2, 4, 6] * 5,
        }),
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_simple_arithmetic_v2(executor):
    assert_eq(
        _context(executor=executor).select('''
            SELECT
                2 * a as a, a + b as b, (a < b) AND (b > a) as c
            FROM my_table
        '''),
        pd.DataFrame({
            '$0.a': [2, 4, 6] * 5,
            '$0.b': [5, 7, 9] * 5,
            '$0.c': [True, True, True] * 5,
        }),
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_simple_arithmetic_v3(executor):
    assert_eq(
        _context(executor=executor).select('SELECT - a + + b as a FROM my_table'),
        pd.DataFrame({
            '$0.a': [4 - 1, 5 - 2, 6 - 3] * 5,
        }),
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_simple_arithmetic_function_calls(executor):
    assert_eq(
        _context(executor=executor).select('SELECT ABS(a - 4 * g) as a FROM my_table'),
        pd.DataFrame({
            '$0.a': [1, 2, 1] * 5,
        }),
    )


@pytest.mark.parametrize('executor', [
    'pandas',
    pytest.mark.xfail(reason='first-value yet not supported')('dask')
])
def test_evaluate_aggregation(executor):
    assert_eq(
        _context(executor=executor).select('''
            SELECT
                SUM(a) as s, AVG(a) as a, MIN(a) as mi, MAX(a) as ma, FIRST_VALUE(a) as fa
            FROM my_table
        '''),
        pd.DataFrame({
            '$2.a': [2.0],
            '$2.s': [30],
            '$2.mi': [1],
            '$2.ma': [3],
            '$2.fa': [1]
        })[['$2.s', '$2.a', '$2.mi', '$2.ma', '$2.fa']],
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
@pytest.mark.parametrize('agg_func,expected', [
    ('AVG', [2.0]),
    ('SUM', [30]),
    ('MIN', [1]),
    ('MAX', [3]),
    ('FIRST_VALUE', [1]),
])
def test_evaluate_aggregation__separate(executor, agg_func, expected):
    if executor == 'dask' and agg_func == 'FIRST_VALUE':
        pytest.xfail("first value in dask not yet supported")

    q = 'SELECT {}(a) as value FROM my_table'.format(agg_func)
    assert_eq(
        _context(executor=executor).select(q),
        pd.DataFrame({'$2.value': expected}),
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_evaluate_aggregation_expession(executor):
    assert_eq(
        _context(executor=executor).select('''
            SELECT SUM(a) - 15 * AVG(a) as a
            FROM my_table
        '''),
        pd.DataFrame({
            '$2.a': [0.0],
        }),
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_evaluate_join(executor):
    expected = pd.DataFrame({
        '$0.a': [1, 2, 3] * 25,
        '$0.d': [10, 10, 11] * 25,
    })

    def _compare(q):
        assert_eq(_context(executor=executor).select(q), expected)

    _compare('SELECT a, d FROM my_table JOIN my_other_table ON g = h')
    _compare('SELECT a, d FROM my_table INNER JOIN my_other_table ON g = h')

    # TODO: add proper tests for outer joins
    _compare('SELECT a, d FROM my_table LEFT JOIN my_other_table ON g = h')
    _compare('SELECT a, d FROM my_table LEFT OUTER JOIN my_other_table ON g = h')

    _compare('SELECT a, d FROM my_table RIGHT JOIN my_other_table ON g = h')
    _compare('SELECT a, d FROM my_table RIGHT OUTER JOIN my_other_table ON g = h')


@pytest.mark.parametrize('executor', [
    'pandas',
    pytest.mark.xfail(reason='cross-join filter not yet supported by dask')('dask'),
])
def test_evaluate_cross_join_filter(executor):
    assert_eq(
        _context(executor=executor).select('''
            SELECT a, d
            FROM my_table, my_other_table
            WHERE g = h
        '''),
        pd.DataFrame({
            '$0.a': [1, 2, 3] * 25,
            '$0.d': [10, 10, 11] * 25,
        }),
    )


@pytest.mark.parametrize('executor', [
    'pandas',
    pytest.mark.xfail(reason='non-equality joins not yet supported')('dask'),
])
def test_evaluate_non_equality_join(executor):
    assert_eq(
        _context(executor=executor).select('''
            SELECT a, d
            FROM my_table
            INNER JOIN my_other_table
            ON g != h
        '''),
        pd.DataFrame({
            '$0.a': [1, 2, 3] * 25,
            # NOTE: d is exactyl reversed due to the inequality condition
            '$0.d': [11, 11, 10] * 25,
        }),
    )


@pytest.mark.parametrize('executor', [
    'pandas',
    pytest.mark.xfail(reason='cross join not yet supported')('dask')
])
def test_evaluate_explicit_cross_join_filter(executor):
    assert_eq(
        _context(executor=executor).select('''
            SELECT a, d
            FROM my_table
            CROSS JOIN my_other_table
            WHERE g = h
        '''),
        pd.DataFrame({
            '$0.a': [1, 2, 3] * 25,
            '$0.d': [10, 10, 11] * 25,
        }),
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_where(executor):
    assert_eq(
        _context(executor=executor).select('''
            SELECT a
            FROM my_table
            WHERE g < one
        '''),
        pd.DataFrame({
            '$0.a': [1, 2] * 5,
        }),
    )


@pytest.mark.parametrize('executor', ['pandas', 'dask'])
def test_where_two(executor):
    assert_eq(
        _context(executor=executor).select('''
            SELECT a
            FROM (
                SELECT a, 2 * a as c
                FROM my_table
            )
            WHERE c >= 4
        '''),
        pd.DataFrame({
            '$1.a': [2, 3] * 5,
        }),
    )


def test_introspection_support():
    my_table = pd.DataFrame({
        'a': [1, 2, 3],
        'b': [4, 5, 6],
        'c': [7, 8, 9],
        'g': [0, 0, 1],
        'one': [1, 1, 1],
    })

    # usage to prevent flake8 message
    print("shape: ", my_table.shape)

    assert_eq(
        fq.select('''
            SELECT a
            FROM my_table
            WHERE g < one
        '''),
        pd.DataFrame({
            '$0.a': [1, 2],
        }),
    )


def test_readme_example():
    stores = pd.DataFrame({
        'country': [0, 0, 1, 1],
        'id': [1, 2, 3, 4],
    })

    sales = pd.DataFrame({
        'store_id': [1, 2, 3, 4],
        'sales': [5, 6, 7, 8]
    })

    # usage to prevent flake8 message
    print("shapes: ", stores.shape, sales.shape)

    import framequery as fq

    sales_by_country = fq.select("""
        SELECT country, sum(sales) as sales

        FROM sales

        JOIN stores
        ON sales.store_id = stores.id

        GROUP BY country
    """)

    assert_eq(
        sales_by_country,
        pd.DataFrame({
            '$2.country': [0, 1],
            '$2.sales': [11, 15],
        }),
    )


def assert_eq(a, b, sort=True):
    if hasattr(a, 'dask'):
        a = a.compute()

    if sort and len(a.columns):
        a = a.sort_values(list(a.columns))
    a = a.reset_index(drop=True)

    if hasattr(b, 'dask'):
        b = a.compute()

    if sort and len(b.columns):
        b = b.sort_values(list(b.columns))
    b = b.reset_index(drop=True)

    pdt.assert_frame_equal(a, b)


def _context(executor='pandas'):
    if executor == 'dask':
        as_df = lambda x: dd.from_pandas(x, npartitions=5)
        executor_factory = DaskExecutor

    elif executor == 'pandas':
        as_df = lambda x: x
        executor_factory = PandasExecutor

    else:
        raise ValueError("unkonw executor {}".format(executor))

    return make_context({
        'my_table': as_df(
            pd.DataFrame({
                'a': [1, 2, 3] * 5,
                'b': [4, 5, 6] * 5,
                'c': [7, 8, 9] * 5,
                'g': [0, 0, 1] * 5,
                'one': [1, 1, 1] * 5,
            })
        ),
        'my_other_table': as_df(
            pd.DataFrame({
                'h': [0, 1] * 5,
                'd': [10, 11] * 5,
            })
        ),
    }, executor_factory=executor_factory)
