[![Build Status](https://api.travis-ci.org/chmp/framequery.svg?branch=master)](https://travis-ci.org/chmp/framequery)

# framequery - SQL on dataframes 

framequery allows to query dataframes with SQL. Currently it targets both 
[pandas][] and [dask][], while aiming for [PostgreSQL][postgres] compatibility.
framequery is also integrated with [sqlalchemy][].

[dask]: dask.pydata.org
[pandas]: pandas.pydata.org
[postgres]: https://www.postgresql.org/
[sqlalchemy]: http://www.sqlalchemy.org/

## Getting started

Install framequery with `pip install framequery` and use `framequery.execute` 
to run queries against dataframes in your scope:

```python
import framequery as fq
import pandas as pd

stores = pd.read_csv('data/stores.csv')
sales = pd.read_csv('data/sales.csv')

sales_by_country = fq.execute("""
    SELECT country, sum(sales) as total_sales

    FROM sales
    JOIN stores
    ON sales.store_id = stores.id

    GROUP BY country
""")

print(sales_by_country)
```

For a details usage see the [usage guide](docs/Usage.md) and the 
[API reference](docs/API.md).

## Changelog

### Development

- refactored code
- aim for postgres compatibility
- first-class dask support
- sqlalchemy support

### 0.1.0

- initial release


## License

>  The MIT License (MIT)
>  Copyright (c) 2016-2017 Christopher Prohm
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
