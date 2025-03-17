import duckdb
from datetime import datetime


con = duckdb.connect()

base_path = "data/tpch_sf10"

customer = con.read_parquet(f"{base_path}/customer.parquet")
orders = con.read_parquet(f"{base_path}/orders.parquet")
lineitem = con.read_parquet(f"{base_path}/lineitem.parquet")

start = datetime.now()

res = con.sql("""
    SELECT
        o_orderpriority,
        count(*) AS order_count
    FROM
        orders
    WHERE
        o_orderdate >= CAST('1993-07-01' AS date)
        AND o_orderdate < CAST('1993-10-01' AS date)
        AND EXISTS (
            SELECT
                *
            FROM
                lineitem
            WHERE
                l_orderkey = o_orderkey
                AND l_commitdate < l_receiptdate)
    GROUP BY
        o_orderpriority
    ORDER BY
        o_orderpriority;
""")

res.show()

end = datetime.now()
print(f"Duration: {end-start}")

