from pdsql.evaluate import *

import functools

import pandas as pd
import pandas.util.testing as pdt


def make_test(func):
    @functools.wraps(func)
    def impl():
        args = func()

        index = [0] if args.get('scalar') is True else None
        actual = pd.DataFrame(
            evaluate(args['query'], args.get('scope')),
            index=index
        )
        expected = args['expected']

        pdt.assert_frame_equal(actual, expected)

    return impl


@make_test
def test_evaluate_no_table():
    return dict(
        query='SELECT 42 as b FROM DUAL',
        expected=pd.DataFrame({'b': [42]}),
        scalar=True
    )


@make_test
def test_evaluate_column():
    return dict(
        scope={
            'tbl': pd.DataFrame({
                'a': [1, 2, 3]
            })
        },
        query='SELECT a FROM tbl',
        expected=pd.DataFrame({
            'a': [1, 2, 3]
        })
    )


@make_test
def test_evaluate_simple_arithmetic():
    return dict(
        scope={
            'tbl': pd.DataFrame({
                'a': [1, 2, 3],
                'b': [4, 5, 6]
            })
        },
        query='SELECT 2 * a as a FROM tbl',
        expected=pd.DataFrame({
            'a': [2, 4, 6],
        })
    )


@make_test
def test_evaluate_simple_arithmetic_v2():
    return dict(
        scope={
            'tbl': pd.DataFrame({
                'a': [1, 2, 3],
                'b': [4, 5, 6]
            })
        },
        query='SELECT 2 * a as a, a + b as b FROM tbl',
        expected=pd.DataFrame({
            'a': [2, 4, 6],
            'b': [5, 7, 9],
        })
    )
