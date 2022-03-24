"""
Usage:
  tpch-dask.py <num> [--conn=<conn>] [--index=<idx>] [--enable-cx]

Options:
  --conn=<conn>          The connection url to use [default: POSTGRES_URL].
  --index=<idx>          The connection url to use [default: l_orderkey].
  --enable-cx               Enable connectorx instead of pandas for read_sql.
  -h --help     Show this screen.
  --version     Show version.

Drivers:
  PostgreSQL: postgresql, postgresql+psycopg2
  MySQL: mysql, mysql+mysqldb, mysql+pymysql
  Redshift: postgresql, redshift, redshift+psycopg2
"""

import os

import dask.dataframe as dd
from contexttimer import Timer
from docopt import docopt
from dask.distributed import Client, LocalCluster

if __name__ == "__main__":
    args = docopt(__doc__, version="Naval Fate 2.0")
    index_col = args["--index"]
    conn = args["--conn"]
    table = "lineitem"
    npartition = int(args["<num>"])
    enable_cx = args["--enable-cx"]

    cluster = LocalCluster(n_workers=npartition, scheduler_port=0, memory_limit="230G")
    client = Client(cluster)

    with Timer() as timer:
        if enable_cx:
            df = dd.read_sql_cx(
                f"select * from {table}",
                conn,
                index_col,
                npartitions=npartition,
                limits=None,
            ).compute()
        else:
            df = dd.read_sql(
                table,
                conn,
                index_col,
                npartitions=npartition,
                limits=None,
                parse_dates=[
                    "l_shipdate",
                    "l_commitdate",
                    "l_receiptdate",
                    "L_SHIPDATE",
                    "L_COMMITDATE",
                    "L_RECEIPTDATE",
                ],
            ).compute()

    print(f"[Total] {timer.elapsed:.2f}s")

    print(df.head())
    print(len(df))
    print(df.dtypes)
