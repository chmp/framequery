import framequery as fq
from framequery import make_context

import pandas as pd
import pandas.util.testing as pdt

import pytest


def test_dual():
    pdt.assert_frame_equal(
        _context().select('SELECT * FROM DUAL'),
        pd.DataFrame(),
    )


def test_dual_two():
    pdt.assert_frame_equal(
        _context().select('SELECT 42 as a FROM DUAL'),
        pd.DataFrame({
            '$0.a': [42],
        }),
    )


def test_dual_no_as():
    pdt.assert_frame_equal(
        _context().select('SELECT 42 a FROM DUAL'),
        pd.DataFrame({
            '$0.a': [42],
        }),
    )


def test_simple_select():
    pdt.assert_frame_equal(
        _context().select('SELECT * FROM my_table'),
        pd.DataFrame({
            'my_table.a': [1, 2, 3],
            'my_table.b': [4, 5, 6],
            'my_table.c': [7, 8, 9],
            'my_table.g': [0, 0, 1],
            'my_table.one': [1, 1, 1],
        }),
    )


def test_simple_sum_cte():
    pdt.assert_frame_equal(
        _context().select('''
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
            '$4.d': [1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 1],
        }),
    )


def test_simple_subquery():
    pdt.assert_frame_equal(
        _context().select('SELECT * FROM (SELECT * FROM my_table)'),
        pd.DataFrame({
            'my_table.a': [1, 2, 3],
            'my_table.b': [4, 5, 6],
            'my_table.c': [7, 8, 9],
            'my_table.g': [0, 0, 1],
            'my_table.one': [1, 1, 1],
        }),
    )


def test_simple_filter():
    pdt.assert_frame_equal(
        _context().select('SELECT * FROM my_table WHERE g = 0'),
        pd.DataFrame({
            'my_table.a': [1, 2],
            'my_table.b': [4, 5],
            'my_table.c': [7, 8],
            'my_table.g': [0, 0],
            'my_table.one': [1, 1],
        }),
    )


def test_evaluate_aggregation_grouped():
    pdt.assert_frame_equal(
        _context().select('SELECT g, SUM(b) as a, FIRST_VALUE(a) as fa FROM my_table GROUP BY g'),
        pd.DataFrame({
            '$2.g': [0, 1],
            '$2.a': [9, 6],
            '$2.fa': [1, 3],
        })[['$2.g', '$2.a', '$2.fa']],
    )


def test_evaluate_aggregation_grouped_no_as():
    pdt.assert_frame_equal(
        _context().select('SELECT g, SUM(b) a FROM my_table GROUP BY g'),
        pd.DataFrame({
            '$2.g': [0, 1],
            '$2.a': [9, 6],
        })[['$2.g', '$2.a']],
    )


@pytest.mark.xfail(reason="not yet supported")
def test_evaluate_aggregation_grouped_no_as__transform():
    pdt.assert_frame_equal(
        _context().select('SELECT SUM(b) a FROM my_table GROUP BY g + g'),
        pd.DataFrame({
            '$2.a': [9, 6],
        })[['$2.a']],
    )


@pytest.mark.xfail(reason="not yet supported")
def test_evaluate_aggregation_grouped_no_as__transform_alias():
    pdt.assert_frame_equal(
        _context().select('SELECT 2 * g as h, SUM(b) a FROM my_table GROUP BY h'),
        pd.DataFrame({
            '$2.h': [0, 2],
            '$2.a': [9, 6],
        })[['$2.h', '$2.a']],
    )


@pytest.mark.xfail(reason="not yet supported")
def test_evaluate_aggregation_grouped__numeric():
    pdt.assert_frame_equal(
        _context().select('SELECT g, SUM(b) as a, FIRST_VALUE(a) as fa FROM my_table GROUP BY 1'),
        pd.DataFrame({
            '$2.g': [0, 1],
            '$2.a': [9, 6],
            '$2.fa': [1, 3],
        })[['$2.g', '$2.a', '$2.fa']],
    )


@pytest.mark.xfail(reason="not yet supported")
def test_evaluate_aggregation_grouped__numeric_no_alias():
    pdt.assert_frame_equal(
        _context().select('SELECT 2 * g, SUM(b) as a, FIRST_VALUE(a) as fa FROM my_table GROUP BY 1'),
        pd.DataFrame({
            '$2.g': [0, 1],
            '$2.a': [9, 6],
            '$2.fa': [1, 3],
        })[['$2.g', '$2.a', '$2.fa']],
    )


def test_select_column():
    pdt.assert_frame_equal(
        _context().select('SELECT a as a FROM my_table'),
        pd.DataFrame({
            '$0.a': [1, 2, 3]
        }),
    )


def test_select_column_analytics_function_sum():
    pdt.assert_frame_equal(
        _context().select('SELECT a as a, sum(a) OVER() as b FROM my_table'),
        pd.DataFrame({
            '$1.a': [1, 2, 3],
            '$1.b': [6, 6, 6],
        }),
    )


def test_select_column_analytics_function_sum_partition_by():
    pdt.assert_frame_equal(
        _context().select('SELECT a as a, sum(a) OVER(PARTITION BY g) as b FROM my_table'),
        pd.DataFrame({
            '$1.a': [1, 2, 3],
            '$1.b': [3, 3, 3],
        }),
    )


def test_select_column_analytics_function_avg():
    pdt.assert_frame_equal(
        _context().select('SELECT a as a, avg(a) OVER() as b FROM my_table'),
        pd.DataFrame({
            '$1.a': [1, 2, 3],
            '$1.b': [2.0, 2.0, 2.0],
        }),
    )


def test_select_column_analytics_function_avg_partition_by():
    pdt.assert_frame_equal(
        _context().select('SELECT a as a, avg(a) OVER(PARTITION BY g) as b FROM my_table'),
        pd.DataFrame({
            '$1.a': [1, 2, 3],
            '$1.b': [1.5, 1.5, 3],
        }),
    )


def test_select_column_without_rename():
    pdt.assert_frame_equal(
        _context().select('SELECT a FROM my_table'),
        pd.DataFrame({
            '$0.a': [1, 2, 3]
        }),
    )


def test_simple_select_distinct():
    pdt.assert_frame_equal(
        _context()
        .select('SELECT DISTINCT g, one FROM my_table')
        .reset_index(drop=True),
        pd.DataFrame({
            '$0.g': [0, 1],
            '$0.one': [1, 1],
        }),
    )


def test_select_column_without_rename_limit():
    pdt.assert_frame_equal(
        _context().select('SELECT a FROM my_table LIMIT 2'),
        pd.DataFrame({
            '$0.a': [1, 2]
        }),
    )

    pdt.assert_frame_equal(
        _context().select('SELECT a FROM my_table LIMIT 1, 2'),
        pd.DataFrame({
            '$0.a': [2, 3]
        }),
    )

    pdt.assert_frame_equal(
        _context().select('SELECT a FROM my_table LIMIT 2 OFFSET 1'),
        pd.DataFrame({
            '$0.a': [2, 3]
        }),
    )


def test_order_by():
    pdt.assert_frame_equal(
        _context().select('SELECT a FROM my_table ORDER BY g, a ASC'),
        pd.DataFrame({
            '$0.a': [3, 1, 2],
        }),
    )


def test_simple_arithmetic():
    pdt.assert_frame_equal(
        _context().select('SELECT 2 * a as a FROM my_table'),
        pd.DataFrame({
            '$0.a': [2, 4, 6],
        }),
    )


def test_simple_arithmetic_v2():
    pdt.assert_frame_equal(
        _context().select('''
            SELECT
                2 * a as a, a + b as b, (a < b) AND (b > a) as c
            FROM my_table
        '''),
        pd.DataFrame({
            '$0.a': [2, 4, 6],
            '$0.b': [5, 7, 9],
            '$0.c': [True, True, True],
        }),
    )


def test_simple_arithmetic_v3():
    pdt.assert_frame_equal(
        _context().select('SELECT - a + + b as a FROM my_table'),
        pd.DataFrame({
            '$0.a': [4 - 1, 5 - 2, 6 - 3]
        }),
    )


def test_simple_arithmetic_function_calls():
    pdt.assert_frame_equal(
        _context().select('SELECT ABS(a - 4 * g) as a FROM my_table'),
        pd.DataFrame({
            '$0.a': [1, 2, 1],
        }),
    )


def test_evaluate_aggregation():
    pdt.assert_frame_equal(
        _context().select('''
            SELECT
                SUM(a) as s, AVG(a) as a, MIN(a) as mi, MAX(a) as ma, FIRST_VALUE(a) as fa
            FROM my_table
        '''),
        pd.DataFrame({
            '$2.a': [2.0],
            '$2.s': [6],
            '$2.mi': [1],
            '$2.ma': [3],
            '$2.fa': [1]
        })[['$2.s', '$2.a', '$2.mi', '$2.ma', '$2.fa']],
    )


def test_evaluate_aggregation_expession():
    pdt.assert_frame_equal(
        _context().select('''
            SELECT SUM(a) - 3 * AVG(a) as a
            FROM my_table
        '''),
        pd.DataFrame({
            '$2.a': [0.0],
        }),
    )


def test_evaluate_join():
    expected = pd.DataFrame({
        '$0.a': [1, 2, 3],
        '$0.d': [10, 10, 11],
    })

    def _compare(q):
        pdt.assert_frame_equal(_context().select(q), expected)

    _compare('SELECT a, d FROM my_table JOIN my_other_table ON g = h')
    _compare('SELECT a, d FROM my_table INNER JOIN my_other_table ON g = h')

    # TODO: add proper tests for outer joins
    _compare('SELECT a, d FROM my_table LEFT JOIN my_other_table ON g = h')
    _compare('SELECT a, d FROM my_table LEFT OUTER JOIN my_other_table ON g = h')

    _compare('SELECT a, d FROM my_table RIGHT JOIN my_other_table ON g = h')
    _compare('SELECT a, d FROM my_table RIGHT OUTER JOIN my_other_table ON g = h')


def test_evaluate_cross_join_filter():
    pdt.assert_frame_equal(
        _context().select('''
            SELECT a, d
            FROM my_table, my_other_table
            WHERE g = h
        '''),
        pd.DataFrame({
            '$0.a': [1, 2, 3],
            '$0.d': [10, 10, 11],
        }),
    )


def test_evaluate_non_equality_join():
    pdt.assert_frame_equal(
        _context().select('''
            SELECT a, d
            FROM my_table
            INNER JOIN my_other_table
            ON g != h
        '''),
        pd.DataFrame({
            '$0.a': [1, 2, 3],
            # NOTE: d is exactyl reversed due to the inequality condition
            '$0.d': [11, 11, 10],
        }),
    )


def test_evaluate_explicit_cross_join_filter():
    pdt.assert_frame_equal(
        _context().select('''
            SELECT a, d
            FROM my_table
            CROSS JOIN my_other_table
            WHERE g = h
        '''),
        pd.DataFrame({
            '$0.a': [1, 2, 3],
            '$0.d': [10, 10, 11],
        }),
    )


def test_where():
    pdt.assert_frame_equal(
        _context().select('''
            SELECT a
            FROM my_table
            WHERE g < one
        '''),
        pd.DataFrame({
            '$0.a': [1, 2],
        }),
    )


def test_where_two():
    pdt.assert_frame_equal(
        _context().select('''
            SELECT a
            FROM (
                SELECT a, 2 * a as c
                FROM my_table
            )
            WHERE c >= 4
        '''),
        pd.DataFrame({
            '$1.a': [2, 3],
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

    pdt.assert_frame_equal(
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

    pdt.assert_frame_equal(
        sales_by_country,
        pd.DataFrame({
            '$2.country': [0, 1],
            '$2.sales': [11, 15],
        }),
    )


def _context():
    return make_context({
        'my_table': pd.DataFrame({
            'a': [1, 2, 3],
            'b': [4, 5, 6],
            'c': [7, 8, 9],
            'g': [0, 0, 1],
            'one': [1, 1, 1],
        }),
        'my_other_table': pd.DataFrame({
            'h': [0, 1],
            'd': [10, 11],
        }),
    })
