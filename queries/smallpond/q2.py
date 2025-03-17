import smallpond
from datetime import datetime


sp = smallpond.init()

base_path = "data/tpch_sf100"

part = sp.read_parquet(f"{base_path}/part.parquet").repartition(2)
supplier = sp.read_parquet(f"{base_path}/supplier.parquet").repartition(2)
partsupp = sp.read_parquet(f"{base_path}/partsupp.parquet").repartition(2)
nation = sp.read_parquet(f"{base_path}/nation.parquet").repartition(2)
region = sp.read_parquet(f"{base_path}/region.parquet").repartition(2)

start = datetime.now()

res = sp.partial_sql("""
    SELECT
        s.s_acctbal,
        s.s_name,
        n.n_name,
        p.p_partkey,
        p.p_mfgr,
        s.s_address,
        s.s_phone,
        s.s_comment
    FROM {0} p
    JOIN {1} ps ON p.p_partkey = ps.ps_partkey
    JOIN {2} s ON s.s_suppkey = ps.ps_suppkey
    JOIN {3} n ON s.s_nationkey = n.n_nationkey
    JOIN {4} r ON n.n_regionkey = r.r_regionkey
    WHERE
        p.p_size = 15
        AND p.p_type LIKE '%BRASS'
        AND r.r_name = 'EUROPE'
        AND ps.ps_supplycost = (
            SELECT MIN(ps_inner.ps_supplycost)
            FROM {1} ps_inner
            JOIN {2} s_inner ON s_inner.s_suppkey = ps_inner.ps_suppkey
            JOIN {3} n_inner ON s_inner.s_nationkey = n_inner.n_nationkey
            JOIN {4} r_inner ON n_inner.n_regionkey = r_inner.r_regionkey
            WHERE
                ps_inner.ps_partkey = p.p_partkey
                AND r_inner.r_name = 'EUROPE'
        )
""", part, partsupp, supplier, nation, region)

res = res.partial_sort(by = ['s_acctbal desc', 'n_name', 's_name', 'p_partkey']).take(100)

end = datetime.now()

print(res)
print(f"Duration: {end-start}")
