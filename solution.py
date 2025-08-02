import sqlite3
import pandas as pd

# Connect to the SQLite database file
conn = sqlite3.connect("Data Engineer_ETL Assignment.db")

#Solution1
sql_query = """
SELECT  c.customer_id AS Customer,
        c.age AS Age,
        i.item_name AS Item,
        SUM(o.quantity) AS Quantity
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
JOIN orders o ON s.sales_id = o.sales_id
JOIN items i ON o.item_id = i.item_id
WHERE c.age BETWEEN 18 AND 35
AND o.quantity IS NOT NULL
GROUP BY c.customer_id, c.age, i.item_name
HAVING SUM(o.quantity) > 0
ORDER BY c.customer_id, i.item_name;
"""


df_sql = pd.read_sql_query(sql_query, conn)
print(df_sql)

df_sql.to_csv("output_sql_solution.csv", sep=';', index=False)
print("output written to 'output_sql_solution.csv'")


#Solution2
customers = pd.read_sql_query("SELECT * FROM customers", conn)
sales = pd.read_sql_query("SELECT * FROM sales", conn)
orders = pd.read_sql_query("SELECT * FROM orders", conn)
items = pd.read_sql_query("SELECT * FROM items", conn)

# Merge all tables
merged = customers.merge(sales, on="customer_id") \
                  .merge(orders, on="sales_id") \
                  .merge(items, on="item_id")
# print(merged)

# Filter age and quantity
filtered = merged[(merged["age"].between(18, 35)) & (merged["quantity"].notnull())]

# Group and aggregate
result = filtered.groupby(["customer_id", "age", "item_name"], as_index=False)["quantity"].sum()
result = result[result["quantity"] > 0]

# Rename columns and sort
result.rename(columns={
    "customer_id": "Customer",
    "age": "Age",
    "item_name": "Item",
    "quantity": "Quantity"
}, inplace=True)

result.sort_values(by=["Customer", "Item"], inplace=True)

# Convert Quantity to be integer as per question
result["Quantity"] = result["Quantity"].astype(int)
print(result)

result.to_csv("output_pandas_solution.csv", sep=';', index=False)
print("solution written to 'output_pandas_solution.csv'")

conn.close()
