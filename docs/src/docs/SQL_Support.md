# framequery's SQL support

**TODO: update this list**

The following operations are supported:

- Select using where, group-by, having, order-by, limit, offset
- Inner and outer joins using equality conditions
- Cross joins (currently, with high performance costs)
- lateral joins
- Subqueries
- Common table expressions
- Numeric expressions

The following limitations do exist:

- no support for in-equality joins
- no support for over-clauses
- no support for non select statements (update, insert, ...)
- no support for set operations on queries (`UNION`, `INTERSECT`, `EXCEPT`)
- no support for subquery expressions (`operator (select ...)`) 
- many, many more, SQL is crazy complex. The topics listed explicitly, however,
  are on the agenda to be fixed.

## Supported functions

- `concat(*strings)`
- `lower(string)`
- `position(... in ...)`
- `trim(... from ...)`
- `upper(string)`
