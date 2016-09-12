# framequery - SQL on dataframes

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
