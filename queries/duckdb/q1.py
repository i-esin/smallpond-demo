import duckdb
from datetime import datetime


con = duckdb.connect()

start = datetime.now()

lineitem = con.read_parquet("data/tpch_sf10/lineitem.parquet")

res = con.sql("""
    SELECT
        l_returnflag,
        l_linestatus,
        sum(l_quantity) AS sum_qty,
        sum(l_extendedprice) AS sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        avg(l_quantity) AS avg_qty,
        avg(l_extendedprice) AS avg_price,
        avg(l_discount) AS avg_disc,
        count(*) AS count_order
    FROM
        lineitem
    WHERE
        l_shipdate <= CAST('1998-09-02' AS date)
    GROUP BY
        l_returnflag,
        l_linestatus
    ORDER BY
        l_returnflag,
        l_linestatus;
""")

res.show()

end = datetime.now()
print(f"Duration: {end-start}")
