import daft
from datetime import datetime


base_path = "data/tpch_sf100"
part = daft.read_parquet(f"{base_path}/part.parquet")
supplier = daft.read_parquet(f"{base_path}/supplier.parquet")
partsupp = daft.read_parquet(f"{base_path}/partsupp.parquet")
nation = daft.read_parquet(f"{base_path}/nation.parquet")
region = daft.read_parquet(f"{base_path}/region.parquet")

start = datetime.now()

res = daft.sql("""
    SELECT
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
    FROM
        part,
        supplier,
        partsupp,
        nation,
        region
    WHERE
        p_partkey = ps_partkey
        AND s_suppkey = ps_suppkey
        AND p_size = 15
        AND p_type LIKE '%BRASS'
        AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey
        AND r_name = 'EUROPE'
        AND ps_supplycost = (
            SELECT
                min(ps_supplycost)
            FROM
                partsupp,
                supplier,
                nation,
                region
            WHERE
                p_partkey = ps_partkey
                AND s_suppkey = ps_suppkey
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'EUROPE')
    ORDER BY
        s_acctbal DESC,
        n_name,
        s_name,
        p_partkey
    LIMIT 100;
""")
res.show()

end = datetime.now()
print(f"Duration: {end-start}")
