import smallpond
from datetime import datetime


sp = smallpond.init()

base_path = "data/tpch_sf100"

customer = sp.read_parquet(f"{base_path}/customer.parquet")
orders = sp.read_parquet(f"{base_path}/orders.parquet")
lineitem = sp.read_parquet(f"{base_path}/lineitem.parquet")

start = datetime.now()

res = sp.partial_sql("""
    SELECT
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) AS revenue,
        o_orderdate,
        o_shippriority
    FROM
        {0},
        {1},
        {2}
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
""", customer, orders, lineitem)

res = res.partial_sort(by = ['revenue desc', 'o_orderdate']).take(10)

end = datetime.now()

print(res)
print(f"Duration: {end-start}")
