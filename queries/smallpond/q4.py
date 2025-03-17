import smallpond
from datetime import datetime


sp = smallpond.init()

base_path = "data/tpch_sf100"

orders = sp.read_parquet(f"{base_path}/orders.parquet")
lineitem = sp.read_parquet(f"{base_path}/lineitem.parquet")

start = datetime.now()

res = sp.partial_sql("""
    SELECT
        o_orderpriority,
        count(*) AS order_count
    FROM
        {0}
    WHERE
        o_orderdate >= CAST('1993-07-01' AS date)
        AND o_orderdate < CAST('1993-10-01' AS date)
        AND EXISTS (
            SELECT
                *
            FROM
                {1}
            WHERE
                l_orderkey = o_orderkey
                AND l_commitdate < l_receiptdate)
    GROUP BY
        o_orderpriority
""", orders, lineitem)

res = res.partial_sort(by = ['o_orderpriority']).take_all()

end = datetime.now()

print(res)
print(f"Duration: {end-start}")
