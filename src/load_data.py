import duckdb

con = duckdb.connect('metrics.duckdb')
con.execute("CREATE OR REPLACE TABLE customers AS SELECT * FROM read_csv_auto('data/customers.csv')")
con.execute("CREATE OR REPLACE TABLE orders AS SELECT * FROM read_csv_auto('data/orders.csv')")
con.execute("CREATE OR REPLACE TABLE order_items AS SELECT * FROM read_csv_auto('data/order_items.csv')")