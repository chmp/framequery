# framequery - SQL on dataframes

## Example

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

## Limitations

- support for only numeric expressions
- support for joins only with equality conditions
- no support for over-clauses
- many, many more, SQL is crazy complex


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
