# framequery's SQL support

**TODO: update this list**

The following operations are supported:

- Select using where, group-by, having, order-by, limit
- Inner and outer joins using equality conditions
- Inner joins with in-equality conditions (currently with high performance
  costs)
- Cross joins (currently, with high performance costs)
- lateral joins
- Subqueries
- Common table expressions
- Numeric expressions

The following limitations do exist:

- no support for non-numeric expressions
- no support for outer joins with inequality conditions
- no support for over-clauses
- no support for non select statements (update, insert, ...)
- many, many more, SQL is crazy complex. The topics listed explicitly, however,
  are on the agenda to be fixed.

See the tests, in particular `tests/test_framequery.py`, for examples of
supported queries.


## Supported functions

- `concat(*strings)`
- `lower(string)`
- `position(... in ...)`
- `trim(... from ...)`
- `upper(string)`
