import daft
from datetime import datetime


base_path = "data/tpch_sf100"
lineitem = daft.read_parquet(f"{base_path}/lineitem.parquet")

start = datetime.now()

res = daft.sql("""
    SELECT
        l_returnflag,
        l_linestatus,
        sum(l_quantity) AS sum_qty,
        sum(l_extendedprice) AS sum_base_price,
        sum(CAST(l_extendedprice AS DOUBLE) * (1 - CAST(l_discount AS DOUBLE))) AS sum_disc_price,
        sum(CAST(l_extendedprice AS DOUBLE) * (1 - CAST(l_discount AS DOUBLE)) * (1 + CAST(l_tax AS DOUBLE))) AS sum_charge,
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
