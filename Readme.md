# framequery - SQL on dataframes

## Usage

```python
import pandas as pd

stores = pd.read_csv('stores.csv')
sales = pd.read_csv('sales.csv')

import framequery as fq

sales_by_country = fq.select("""
    SELECT country, sum(sales)

    FROM sales

    JOIN stores
    ON sales.store_id = stores.id

    GROUP BY country
""")
```

The dataframes to use, can be specified by explicitly passing a dictionary as
the `scope` parameter. The example would read
`fq.select(..., scope={'stores': stores, 'sales': sales})`.

## Limitations

- support for only numeric expressions
- support for joins only with equality conditions
- no support for over-clauses
- no subqueries
- no support for non select statements (update, insert, ...)
- many, many more, SQL is crazy complex. The topics listed explicitly, however,
  are on the agenda to be fixed.

See the tests, in particular `tests/test_framequery.py`, for examples of
supported queries.

## Internals

`framequery` executes SQL statements via a multi-step process:

1. The query is parsed and an SQL AST assembled.
2. The AST is compiled into a DAG reprsenting transformation of dataframes.
3. The generated DAG is finally interpreted and the transformations are applied
   to the supplied dataframes.

The AST classes can be found inside `framequery._parser`, whereas the DAG
classes are found inside `framequery._dag`.

## Running tests

Inside the project root, execute

```
    pip install tox
    tox
```

## License

>  The MIT License (MIT)
>  Copyright (c) 2016 Christopher Prohm
>
>  Permission is hereby granted, free of charge, to any person obtaining a copy
>  of this software and associated documentation files (the "Software"), to
>  deal in the Software without restriction, including without limitation the
>  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
>  sell copies of the Software, and to permit persons to whom the Software is
>  furnished to do so, subject to the following conditions:
>
>  The above copyright notice and this permission notice shall be included in
>  all copies or substantial portions of the Software.
>
>  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
>  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
>  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
>  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
>  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
>  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
>  DEALINGS IN THE SOFTWARE.