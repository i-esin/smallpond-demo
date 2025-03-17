import daft
from datetime import datetime


base_path = "data/tpch_sf100"
customer = daft.read_parquet(f"{base_path}/customer.parquet")
orders = daft.read_parquet(f"{base_path}/orders.parquet")
lineitem = daft.read_parquet(f"{base_path}/lineitem.parquet")

start = datetime.now()

res = daft.sql("""
    SELECT
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) AS revenue,
        o_orderdate,
        o_shippriority
    FROM
        customer,
        orders,
        lineitem
    WHERE
        c_mktsegment = 'BUILDING'
        AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND o_orderdate < CAST('1995-03-15' AS date)
        AND l_shipdate > CAST('1995-03-15' AS date)
    GROUP BY
        l_orderkey,
        o_orderdate,
        o_shippriority
    ORDER BY
        revenue DESC,
        o_orderdate
    LIMIT 10;
""")
res.show()

end = datetime.now()
print(f"Duration: {end-start}")
