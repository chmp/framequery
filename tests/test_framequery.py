import framequery as fq
from framequery import make_context

import pandas as pd
import pandas.util.testing as pdt


def test_dual():
    pdt.assert_frame_equal(
        _context().select('SELECT * FROM DUAL'),
        pd.DataFrame(),
    )


def test_dual():
    pdt.assert_frame_equal(
        _context().select('SELECT 42 as a FROM DUAL'),
        pd.DataFrame({
            ('$0', 'a'): [42],
        }),
    )


def test_simple_select():
    pdt.assert_frame_equal(
        _context().select('SELECT * FROM my_table'),
        pd.DataFrame({
            ('my_table', 'a'): [1, 2, 3],
            ('my_table', 'b'): [4, 5, 6],
            ('my_table', 'c'): [7, 8, 9],
            ('my_table', 'g'): [0, 0, 1],
            ('my_table', 'one'): [1, 1, 1],
        }),
    )


def test_simple_filter():
    pdt.assert_frame_equal(
        _context().select('SELECT * FROM my_table WHERE g = 0'),
        pd.DataFrame({
            ('my_table', 'a'): [1, 2],
            ('my_table', 'b'): [4, 5],
            ('my_table', 'c'): [7, 8],
            ('my_table', 'g'): [0, 0],
            ('my_table', 'one'): [1, 1],
        }),
    )


def test_evaluate_aggregation_grouped():
    pdt.assert_frame_equal(
        _context().select('SELECT g, SUM(b) as a FROM my_table GROUP BY g'),
        pd.DataFrame({
            ('$2', 'g'): [0, 1],
            ('$2', 'a'): [9, 6],
        })[[('$2', 'g'), ('$2', 'a')]],
    )


def test_select_column():
    pdt.assert_frame_equal(
        _context().select('SELECT a as a FROM my_table'),
        pd.DataFrame({
            ('$0', 'a'): [1, 2, 3]
        }),
    )


def test_select_column_without_rename():
    pdt.assert_frame_equal(
        _context().select('SELECT a FROM my_table'),
        pd.DataFrame({
            ('$0', 'a'): [1, 2, 3]
        }),
    )


def test_simple_arithmetic():
    pdt.assert_frame_equal(
        _context().select('SELECT 2 * a as a FROM my_table'),
        pd.DataFrame({
            ('$0', 'a'): [2, 4, 6],
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
            ('$0', 'a'): [2, 4, 6],
            ('$0', 'b'): [5, 7, 9],
            ('$0', 'c'): [True, True, True],
        }),
    )


def test_evaluate_aggregation():
    pdt.assert_frame_equal(
        _context().select('''
            SELECT
                SUM(a) as s, AVG(a) as a, MIN(a) as mi, MAX(a) as ma
            FROM my_table
        '''),
        pd.DataFrame({
            ('$2', 'a'): [2.0],
            ('$2', 's'): [6],
            ('$2', 'mi'): [1],
            ('$2', 'ma'): [3],
        })[[('$2', 's'), ('$2', 'a'), ('$2', 'mi'), ('$2', 'ma')]],
    )


def test_where():
    pdt.assert_frame_equal(
        _context().select('''
            SELECT a
            FROM my_table
            WHERE g < one
        '''),
        pd.DataFrame({
            ('$0', 'a'): [1, 2],
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

    pdt.assert_frame_equal(
        fq.select('''
            SELECT a
            FROM my_table
            WHERE g < one
        '''),
        pd.DataFrame({
            ('$0', 'a'): [1, 2],
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
    })
