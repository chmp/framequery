from framequery._pandas import PandasExecutor
from framequery._dag import (
    Aggregate,
    GetTable,
    Join,
    Literal,
    Transform,
)
from framequery._parser import ColumnReference, DerivedColumn, ValueExpression

import pandas as pd
import pandas.util.testing as pdt


def test_get_table_simple():
    scope = {
        "foo": pd.DataFrame({
            "a": [0, 1, 2],
            "b": [3, 4, 5],
        })
    }

    ex = PandasExecutor()
    actual = ex.evaluate(GetTable('foo'), scope)

    expected = pd.DataFrame({
        "foo.a": [0, 1, 2],
        "foo.b": [3, 4, 5],
    })
    pdt.assert_frame_equal(actual, expected)


def test_get_table_alias():
    scope = {
        "foo": pd.DataFrame({
            "a": [0, 1, 2],
            "b": [3, 4, 5],
        })
    }

    ex = PandasExecutor()
    actual = ex.evaluate(GetTable('foo', alias="bar"), scope)

    expected = pd.DataFrame({
        "bar.a": [0, 1, 2],
        "bar.b": [3, 4, 5],
    })
    pdt.assert_frame_equal(actual, expected)


def test_literal():
    ex = PandasExecutor()
    assert ex.evaluate(Literal("foo-bar-bar"), None) == "foo-bar-bar"


def test_join_simple():
    left = pd.DataFrame({
        'left.a': [0, 0, 1, 1],
        'left.c': [1, 2, 3, 4]
    })

    right = pd.DataFrame({
        'right.b': [0, 1],
        'right.d': [10, 20]
    })

    expected = pd.DataFrame({
        'left.a': [0, 0, 1, 1],
        'left.c': [1, 2, 3, 4],
        'right.b': [0, 0, 1, 1],
        'right.d': [10, 10, 20, 20],
    })

    def perform(expr):
        ex = PandasExecutor()
        condition = ValueExpression.parse(expr)
        return ex.evaluate(Join(Literal(left), Literal(right), how="inner", on=condition), None)

    pdt.assert_frame_equal(perform("a = b"), expected)
    pdt.assert_frame_equal(perform("left.a = b"), expected)
    pdt.assert_frame_equal(perform("a = right.b"), expected)
    pdt.assert_frame_equal(perform("left.a = right.b"), expected)


def test_inner_join_missing_values_simple():
    """In standard pandas joins, NANs are retained. With strict mode they are skipped.
    """
    left = pd.DataFrame({
        'left.a': [0, 0, None, 1],
        'left.c': [1, 2, 3, 4]
    })

    right = pd.DataFrame({
        'right.b': [0, 1, None],
        'right.d': [10, 20, 30]
    })

    expected = pd.DataFrame({
        'left.a': [0.0, 0.0, 1.0],
        'left.c': [1, 2, 4],
        'right.b': [0.0, 0.0, 1.0],
        'right.d': [10, 10, 20],
    })

    def perform(expr):
        ex = PandasExecutor(strict=True)
        condition = ValueExpression.parse(expr)
        return ex.evaluate(Join(Literal(left), Literal(right), how="inner", on=condition), None)

    pdt.assert_frame_equal(perform("a = b"), expected)
    pdt.assert_frame_equal(perform("left.a = b"), expected)
    pdt.assert_frame_equal(perform("a = right.b"), expected)
    pdt.assert_frame_equal(perform("left.a = right.b"), expected)


def test_transform():
    df = pd.DataFrame({
        'df.a': [0, 1, 2, 3],
        'df.b': [4, 5, 6, 7],
    })

    def perform(q):
        ex = PandasExecutor()
        node = Transform(Literal(df), [DerivedColumn.parse(q)])
        return ex.evaluate(node, None)

    expected = pd.DataFrame({
        '$0.c': [4, 6, 8, 10],
    })

    pdt.assert_frame_equal(perform('a + b as c'), expected)
    pdt.assert_frame_equal(perform('a + df.b as c'), expected)
    pdt.assert_frame_equal(perform('df.a + b as c'), expected)
    pdt.assert_frame_equal(perform('df.a + df.b as c'), expected)


def test_aggregate_no_groups():
    df = pd.DataFrame({
        'df.g': [0, 0, 1, 1],
        'df.a': [4, 5, 6, 7],
    })

    def _scalar_df(values):
        return pd.DataFrame(values, index=[0])

    def perform(q):
        node = Aggregate(Literal(df), [DerivedColumn.parse(q)])

        ex = PandasExecutor()
        return ex.evaluate(node, None)

    pdt.assert_frame_equal(perform('SUM(a) as c'), _scalar_df({'$0.c': 22}))
    pdt.assert_frame_equal(perform('AVG(a) as c'), _scalar_df({'$0.c': 22 / 4.0}))
    pdt.assert_frame_equal(perform('MIN(a) as c'), _scalar_df({'$0.c': 4}))
    pdt.assert_frame_equal(perform('MAX(a) as c'), _scalar_df({'$0.c': 7}))
    pdt.assert_frame_equal(perform('COUNT(a) as c'), _scalar_df({'$0.c': 4}))


def test_aggregate_with_groups():
    df = pd.DataFrame({
        'df.g': [0, 0, 1, 1],
        'df.a': [4, 5, 6, 7],
    })

    def _scalar_df(values):
        return pd.DataFrame(values, index=[0])

    def perform(q):
        node = Aggregate(
            Literal(df), [DerivedColumn.parse(q)],
            group_by=[ColumnReference(['g'])]
        )

        ex = PandasExecutor()
        return ex.evaluate(node, None).sort_index(axis=1)

    pdt.assert_frame_equal(
        perform('SUM(a) as c'),
        pd.DataFrame({
            'df.g': [0, 1],
            '$0.c': [9, 13],
        })
    )

    pdt.assert_frame_equal(
        perform('MIN(a) as c'),
        pd.DataFrame({
            'df.g': [0, 1],
            '$0.c': [4, 6],
        })
    )

    pdt.assert_frame_equal(
        perform('MAX(a) as c'),
        pd.DataFrame({
            'df.g': [0, 1],
            '$0.c': [5, 7],
        })
    )

    pdt.assert_frame_equal(
        perform('AVG(a) as c'),
        pd.DataFrame({
            'df.g': [0, 1],
            '$0.c': [4.5, 6.5],
        })
    )

    pdt.assert_frame_equal(
        perform('COUNT(a) as c'),
        pd.DataFrame({
            'df.g': [0, 1],
            '$0.c': [2, 2],
        })
    )
