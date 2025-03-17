import smallpond
from datetime import datetime

sp = smallpond.init()

lineitem = sp.read_parquet("data/tpch_sf100/lineitem.parquet")
lineitem = lineitem.repartition(2)

start = datetime.now()

res = sp.partial_sql("""
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
        {0}
    WHERE
        l_shipdate <= CAST('1998-09-02' AS date)
    GROUP BY
        l_returnflag,
        l_linestatus
""", lineitem)

res = res.partial_sort(by = ['l_returnflag', 'l_linestatus']).take_all()

end = datetime.now()

print(f"Duration: {end-start}")
print(res)