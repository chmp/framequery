# framequery API

.. autofunction:: framequery.execute

.. autoclass:: framequery.Executor

.. automethod:: framequery.Executor.add_lateral_function

.. autoclass:: framequery.DaskModel

.. autoclass:: framequery.PandasModel

.. autofunction:: framequery.parser.parse

.. autofunction:: framequery.parser.tokenize

.. autofunction:: framequery.alchemy.get_executor

.. automodule:: framequery.parser.ast

.. autoclass:: framequery.parser.ast.Select(columns, from_clause, where_clause, group_by_clause, having_clause, order_by_clause, limit_clause, quantifier, cte)

.. autoclass:: framequery.parser.ast.SubQuery(query, alias)
