import duckdb


con = duckdb.connect()
con.sql("""
    install tpch;
    load tpch;
""")

res = con.sql("from tpch_queries();")
con.sql("copy res to 'data/queries.csv' (format csv)")
