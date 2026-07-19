"""Local experiment: seed partitioned parquet into MiniDFS, serve via the Python
package, and read it back with polars / duckdb / pandas over the S3 gateway.

Prereqs (run from bash before this):
    mvn -q compile exec:java -f target/debug/build/hdfs-native-*/out/minidfs/pom.xml
    export HADOOP_CONF_DIR=$PWD/target/test
    hdfs dfs -mkdir -p /data
    uv run --project python python scripts/experiment_parquet.py seed

Then:
    uv run --project python python scripts/experiment_parquet.py
"""

import os
import shutil
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NN_URI = "hdfs://127.0.0.1:9000"
HDFS_ROOT = "/data"
BUCKET = "hdfs"


def seed_hdfs():
    import pyarrow as pa
    import pyarrow.parquet as pq

    local = "/tmp/seed_parquet"
    shutil.rmtree(local, ignore_errors=True)
    os.makedirs(local)

    # Partition columns (country, year) are encoded ONLY in the directory layout
    # (country=us/year=2021/...), NOT duplicated inside the parquet files. Writing
    # them as in-file columns too makes pyarrow's dataset reader derive them a second
    # time from the path as dictionary-encoded types and then fail to merge the two
    # (ArrowTypeError: string vs dictionary). Keeping them path-only lets every engine
    # recover the partition columns cleanly.
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("value", pa.float64()),
        ]
    )
    for country in ("us", "eu", "asia"):
        for year in (2021, 2022, 2023):
            rows = 50
            ids = list(range(rows))
            table = pa.table(
                {
                    "id": pa.array(ids, pa.int64()),
                    "value": pa.array([float(i) * 1.5 for i in ids], pa.float64()),
                },
                schema=schema,
            )
            part_dir = os.path.join(local, f"country={country}", f"year={year}")
            os.makedirs(part_dir, exist_ok=True)
            pq.write_table(table, os.path.join(part_dir, "data.parquet"))

    env = {**os.environ, "HADOOP_CONF_DIR": os.path.join(REPO, "target/test")}
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_ROOT], env=env, check=True)
    # `hdfs dfs -put <localdir> <dest>` copies the whole directory tree (the glob is
    # not expanded by the Java CLI, so we pass the directory itself).
    subprocess.run(
        ["hdfs", "dfs", "-put", "-f", local, HDFS_ROOT + "/"],
        env=env,
        check=True,
    )
    print("Seeded HDFS at", HDFS_ROOT)


def main():
    from hdfs_s3_gateway import serving

    with serving(
        namenode_uri=NN_URI,
        hdfs_root=HDFS_ROOT,
        bucket_name=BUCKET,
        listen_addr="127.0.0.1:0",
    ) as gw:
        endpoint = f"http://{gw.address}"
        print("Gateway at", endpoint)
        os.environ["AWS_ENDPOINT_URL"] = endpoint
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_ACCESS_KEY_ID"] = "dummy"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "dummy"

        # ---- polars ----
        import polars as pl

        df = pl.read_parquet(
            f"{endpoint}/{BUCKET}/seed_parquet/country=*/year=*/*.parquet",
            storage_options={"anon": "true"},
        )
        print("polars rows:", df.height, "cols:", df.columns)

        # ---- duckdb ----
        import duckdb

        ddf = duckdb.sql(
            f"SELECT count(*) AS n, count(DISTINCT country) AS c "
            f"FROM read_parquet('{endpoint}/{BUCKET}/seed_parquet/country=*/year=*/*.parquet', "
            f"storage_options={{'anon': 'true'}})"
        ).df()
        print("duckdb:", ddf.to_dict("records"))

        # ---- pandas ----
        import pandas as pd

        pdf = pd.read_parquet(
            f"{endpoint}/{BUCKET}/seed_parquet/country=*/year=*/*.parquet",
            storage_options={"anon": "true"},
        )
        print("pandas rows:", len(pdf), "cols:", list(pdf.columns))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "seed":
        seed_hdfs()
    else:
        main()
