# hdfs-s3-gateway (Python)

Python bindings for the HDFS → S3 read-only gateway. This package wraps the Rust
gateway so it can be driven from Python either as a foreground blocking call or as
an in-process background server.

## Usage

```python
from hdfs_s3_gateway import Gateway

# Background server (no subprocess — runs on its own native thread + tokio runtime).
gw = Gateway(namenode_uri="hdfs://localhost:8020", hdfs_root="/data", bucket_name="hdfs")
gw.start()
print(gw.address)   # e.g. "127.0.0.1:8080"
# ... point any S3 client at the endpoint ...
gw.stop()           # graceful shutdown

# Or as a context manager:
with Gateway(config_path="gateway.toml") as gw:
    print(gw.address)
    # ... use the endpoint ...
```

The gateway performs **no authentication by default** — it must run behind network-level
access control. See the top-level project README for the full security posture. You can
optionally enable SigV4 auth by passing `auth_secret="..."` to `Gateway(...)` (or the
`--auth-secret` CLI flag / `auth_secret` TOML key). When set, a valid signature is **required**
(unsigned requests are rejected, wrong secret → `403 SignatureDoesNotMatch`), so clients use
normal signing (any access-key id + that secret) instead of `anonymous` / `aws_skip_signature`.
When unset (the default), the gateway accepts both anonymous and signed requests.

## Reading data through the gateway

Once the gateway is running, point any S3 client at its endpoint. The examples below
assume the gateway was started with `hdfs_root="/data"` and `bucket_name="hdfs"`, exposing
a hive-partitioned table at `s3://hdfs/seed_parquet/country=us/year=2021/data.parquet`.

By default all clients use **anonymous / unsigned** access (the gateway has no auth). The
endpoint is `http://<gw.address>` — e.g. `http://127.0.0.1:39111`. If you started the
gateway with `auth_secret`, drop the `anonymous` / `aws_skip_signature` flags and pass any
access-key id plus that secret instead.

### polars

```python
import polars as pl

ep = f"http://{gw.address}"
so = {
    "aws_skip_signature": "true",
    "aws_endpoint_url": ep,
    "aws_allow_http": "true",
}
# Whole directory tree (hive partition columns are recovered automatically):
df = pl.read_parquet(f"s3://hdfs/seed_parquet/", storage_options=so)
# Or a recursive glob (polars expands it client-side into S3 listings):
df = pl.read_parquet(f"s3://hdfs/seed_parquet/country=*/year=*/*.parquet", storage_options=so)
```

### DuckDB

```python
import duckdb

ep = f"http://{gw.address}"
host = ep.replace("http://", "")
duckdb.sql("INSTALL httpfs; LOAD httpfs;")
duckdb.sql(f"SET s3_endpoint='{host}';")
duckdb.sql("SET s3_use_ssl=false;")
duckdb.sql("SET s3_access_key_id='';")
duckdb.sql("SET s3_secret_access_key='';")
duckdb.sql("SET s3_session_token='';")
duckdb.sql("SET s3_url_style='path';")

# DuckDB's httpfs reads a directory tree via a recursive glob:
df = duckdb.sql("SELECT * FROM read_parquet('s3://hdfs/seed_parquet/**')").df()
```

### pandas (pyarrow engine)

```python
import pandas as pd

ep = f"http://{gw.address}"
so = {
    "anon": "true",
    "endpoint_url": ep,
    "client_kwargs": {"endpoint_url": ep},
}
# pandas delegates s3:// URIs to fsspec/s3fs, not pyarrow's native S3 filesystem.
df = pd.read_parquet("s3://hdfs/seed_parquet/", storage_options=so)
```

### pandas (s3fs / fsspec engine)

```python
import pandas as pd

ep = f"http://{gw.address}"
so = {
    "anon": "true",
    "endpoint_url": ep,
    "client_kwargs": {"endpoint_url": ep},
}
df = pd.read_parquet("s3://hdfs/seed_parquet/", storage_options=so)
```

### pyarrow (native `S3FileSystem`)

```python
import pyarrow.dataset as ds
import pyarrow.fs as pafs

ep = f"http://{gw.address}"
host = ep.replace("http://", "")
s3 = pafs.S3FileSystem(anonymous=True, endpoint_override=host, scheme="http")
# pyarrow wants a "bucket/key" path, not a full s3:// URI.
dataset = ds.dataset("hdfs/seed_parquet", format="parquet", filesystem=s3, partitioning="hive")
table = dataset.to_table()
```

> **Note on path forms.** Only polars and DuckDB expand glob patterns (`country=*/year=*`)
> client-side into real S3 listings; the other engines pass the glob literally to the
> gateway, which cannot match it. For a whole partitioned table, use the directory form
> (`s3://hdfs/seed_parquet/`).

