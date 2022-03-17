set dotenv-load := true

install-dependencies:
    pip install docopt contexttimer psycopg2-binary connectorx sqlalchemy

compile-dask:
    cd dask && pip install ".[dataframe,distributed]"

postgres num="1" +args="":
    python tpch-dask.py {{num}} --conn $POSTGRES_URL {{args}}