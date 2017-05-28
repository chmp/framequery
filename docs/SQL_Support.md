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

|Name | Supported|
|-----|----------|
|[&#x7C;&#x7C;](https://www.postgresql.org/docs/9.6/static/functions-string.html)|✓|
|[ascii](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[bit_length](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[btrim](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[char_length](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[character_length](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[chr](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[concat](https://www.postgresql.org/docs/9.6/static/functions-string.html)|✓|
|[concat_ws](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[convert](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[convert_from](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[convert_to](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[decode](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[encode](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[format](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[initcap](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[left](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[length](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[lower](https://www.postgresql.org/docs/9.6/static/functions-string.html)|✓|
|[lpad](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[ltrim](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[md5](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[octet_length](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[overlay](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[parse_ident](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[position](https://www.postgresql.org/docs/9.6/static/functions-string.html)|✓|
|[pg_client_encoding](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[quote_ident](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[quote_literal](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[quote_nullable](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[regexp_matches](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[regexp_replace](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[regexp_split_to_array](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[regexp_split_to_table](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[repeat](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[replace](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[reverse](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[right](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[rpad](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[split_part](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[strpos](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[substr](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[substring](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[to_ascii](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[to_hex](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[trim](https://www.postgresql.org/docs/9.6/static/functions-string.html)|✓|
|[translate](https://www.postgresql.org/docs/9.6/static/functions-string.html)|x|
|[upper](https://www.postgresql.org/docs/9.6/static/functions-string.html)|✓|

|Name | Supported|
|-----|----------|
|[case](https://www.postgresql.org/docs/9.6/static/functions-conditional.html#FUNCTIONS-CASE)|✓|
|[coalesce](https://www.postgresql.org/docs/9.6/static/functions-conditional.html#FUNCTIONS-COALESCE-NVL-IFNULL)|x|
|[nullif](https://www.postgresql.org/docs/9.6/static/functions-conditional.html#FUNCTIONS-NULLIF)|x|
|[greatest](https://www.postgresql.org/docs/9.6/static/functions-conditional.html#FUNCTIONS-GREATEST-LEAST)|x|
|[least](https://www.postgresql.org/docs/9.6/static/functions-conditional.html#FUNCTIONS-GREATEST-LEAST)|x|