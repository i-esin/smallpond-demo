import marimo

__generated_with = "0.11.12"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    # import duckdb
    return (mo,)


@app.cell
def _(duckdb):
    c = duckdb.connect("taxi_trips.ddb")
    return (c,)


@app.cell
def _(c):
    c.sql("select * from duckdb_settings() where name = 'threads';")
    return


@app.cell
def _(c):
    c.sql("SET enable_progress_bar = true;")
    return


@app.cell
def _():
    # Azure storage access info
    blob_account_name = "azureopendatastorage"
    blob_container_name = "nyctlc"
    blob_relative_path = "yellow"
    url_path = f"az://{blob_account_name}.blob.core.windows.net/{blob_container_name}/{blob_relative_path}/**/*.parquet"

    print(url_path) # 'az://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/**/*.parquet'
    return blob_account_name, blob_container_name, blob_relative_path, url_path


@app.cell
def _(c, mo, null, url_path):
    _df = mo.sql(
        f"""
        -- select * from 'az://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/**/*.parquet'
        -- select * from read_parquet('az://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/**/*.parquet')
        select * from '{url_path}'
        """,
        engine=c
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Smallpond""")
    return


@app.cell
def _():
    import smallpond
    import graphviz
    return graphviz, smallpond


@app.cell
def _(smallpond):
    sp = smallpond.init()
    return (sp,)


@app.cell
def _(sp):
    sp.config
    return


@app.cell
def _(sp, url_path):
    taxi_trips = sp.read_parquet(url_path)
    return (taxi_trips,)


@app.cell
def _(taxi_trips):
    df = taxi_trips.repartition(10000)
    return (df,)


@app.cell
def _(df):
    df.count()
    return


@app.cell
def _(df, sp):
    query = "select count(*) from taxi_trips"
    count_trips = sp.partial_sql(query, df)
    return count_trips, query


if __name__ == "__main__":
    app.run()
