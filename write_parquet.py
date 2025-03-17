import duckdb


con = duckdb.connect("data/tpch-sf100.db")
table_names = con.sql("from duckdb_tables()").df()["table_name"].to_list()

for table_name in table_names:
    filepath = f"./data/tpch_sf100/{table_name}.parquet"
    print(f"Writing to {filepath}")
    con.sql(f"copy {table_name} to '{filepath}' (format parquet, compression zstd)")

print("Finished")
