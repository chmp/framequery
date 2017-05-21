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