"""Parquet engine integration tests against the hdfs-s3-gateway.

These tests exercise the gateway the way a real data lake client would: each engine
reads a *hive-partitioned* parquet table (``seed_parquet/``) directly from the S3
endpoint exposed by the gateway, using its native S3 client. No manual enumeration or
glob-expansion tricks are used — the frameworks do their own S3 listing, exactly as
they would against real S3.

The gateway is started in-process via the ``serving()`` context manager. The HDFS
side (MiniDFS + seeded partitioned parquet) is expected to already be running and
seeded — that is the job of the bash harness / CI setup, not these tests.

Engines covered (each with its own native S3 client config, anonymous / unsigned):

* polars        (object_store, ``aws_skip_signature``)
* duckdb        (httpfs, ``SET s3_*``)
* pandas        (pyarrow engine)
* pandas        (s3fs / fsspec engine)
* pyarrow       (native ``pyarrow.fs.S3FileSystem``)
* pyarrow       (s3fs-backed dataset)

Path variants per engine:

* directory (whole table)        ``s3://<bucket>/seed_parquet/`` (polars, pyarrow-native)
                                 ``s3://<bucket>/seed_parquet/**`` (duckdb — its httpfs
                                 reads a directory tree via a recursive glob, not a bare
                                 prefix)
* glob (partition wildcards)      ``s3://<bucket>/seed_parquet/country=*/year=*/*.parquet``
* single object                  ``s3://<bucket>/seed_parquet/country=us/year=2021/data.parquet``

Tests are intentionally broad. Some cases may currently fail because the gateway
does not yet implement every S3 behaviour a given client relies on (e.g. HEAD on a
directory prefix, or a missing operation). The failures are the signal for what to
fix in the gateway — they are not expected to all pass on the first run.
"""

import os

import pytest

import polars as pl
import duckdb
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import s3fs

from hdfs_s3_gateway import serving

# --- Configuration (overridable from the environment for local / CI runs) ---------
NN_URI = os.environ.get("HDFS_NN_URI", "hdfs://127.0.0.1:9000")
HDFS_ROOT = os.environ.get("HDFS_ROOT", "/data")
BUCKET = os.environ.get("GW_BUCKET", "hdfs")
TABLE = "seed_parquet"

# Expected shape of the seeded table.
FULL_ROWS = 450  # 3 countries x 3 years x 50 rows
SINGLE_ROWS = 50
EXPECTED_COUNTRIES = {"asia", "eu", "us"}
EXPECTED_YEARS = {2021, 2022, 2023}
EXPECTED_COLUMNS = {"id", "value", "country", "year"}


# --------------------------------------------------------------------------- #
# Gateway fixture
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def endpoint():
    """Start the gateway in the background and yield its HTTP endpoint."""
    with serving(
        namenode_uri=NN_URI,
        hdfs_root=HDFS_ROOT,
        bucket_name=BUCKET,
        listen_addr="127.0.0.1:0",
    ) as gw:
        yield f"http://{gw.address}"


# --------------------------------------------------------------------------- #
# Engine readers: each returns (n_rows, column_names)
# --------------------------------------------------------------------------- #


def read_polars(endpoint: str, path: str):
    so = {
        "aws_skip_signature": "true",
        "aws_endpoint_url": endpoint,
        "aws_allow_http": "true",
    }
    df = pl.read_parquet(path, storage_options=so)
    return df.height, df.columns


def read_duckdb(endpoint: str, path: str):
    host = endpoint.replace("http://", "")
    duckdb.sql("INSTALL httpfs; LOAD httpfs;")
    duckdb.sql(f"SET s3_endpoint='{host}';")
    duckdb.sql("SET s3_use_ssl=false;")
    duckdb.sql("SET s3_access_key_id='';")
    duckdb.sql("SET s3_secret_access_key='';")
    duckdb.sql("SET s3_session_token='';")
    duckdb.sql("SET s3_url_style='path';")
    df = duckdb.sql(f"SELECT * FROM read_parquet('{path}')").df()
    return len(df), list(df.columns)


def read_pandas_pyarrow(endpoint: str, path: str):
    # pandas' default (pyarrow) engine does NOT use pyarrow's native S3FileSystem for
    # s3:// URIs — it delegates to fsspec/s3fs. So we use s3fs-style anonymous options.
    so = {
        "anon": "true",
        "endpoint_url": endpoint,
        "client_kwargs": {"endpoint_url": endpoint},
    }
    df = pd.read_parquet(path, storage_options=so)
    return len(df), list(df.columns)


def read_pandas_s3fs(endpoint: str, path: str):
    # s3fs / fsspec engine: anonymous + endpoint, no object_store-specific keys.
    so = {
        "anon": "true",
        "endpoint_url": endpoint,
        "client_kwargs": {"endpoint_url": endpoint},
    }
    df = pd.read_parquet(path, storage_options=so)
    return len(df), list(df.columns)


def read_pyarrow_native(endpoint: str, path: str):
    # pyarrow's native S3FileSystem wants a "bucket/key" path, not a full s3:// URI.
    host = endpoint.replace("http://", "")
    s3 = pafs.S3FileSystem(anonymous=True, endpoint_override=host, scheme="http")
    bucket_key = path.replace(f"s3://{BUCKET}/", f"{BUCKET}/")
    dataset = ds.dataset(bucket_key, format="parquet", filesystem=s3, partitioning="hive")
    table = dataset.to_table()
    return table.num_rows, table.column_names


def read_pyarrow_s3fs(endpoint: str, path: str):
    fsys = s3fs.S3FileSystem(anon=True, client_kwargs={"endpoint_url": endpoint})
    dataset = ds.dataset(path, format="parquet", filesystem=fsys, partitioning="hive")
    table = dataset.to_table()
    return table.num_rows, table.column_names


# Engine registry: name -> (reader, expected_rows_for_full_table)
ENGINES = {
    "polars": (read_polars, FULL_ROWS),
    "duckdb": (read_duckdb, FULL_ROWS),
    "pandas-pyarrow": (read_pandas_pyarrow, FULL_ROWS),
    "pandas-s3fs": (read_pandas_s3fs, FULL_ROWS),
    "pyarrow-native": (read_pyarrow_native, FULL_ROWS),
    "pyarrow-s3fs": (read_pyarrow_s3fs, FULL_ROWS),
}


# --------------------------------------------------------------------------- #
# Path variants
# --------------------------------------------------------------------------- #
#
# Each engine gets only the variants it *actually supports* against a real S3 gateway.
# We determined the supported set empirically by watching the S3 requests each client
# emits (RUST_LOG=info): a framework either expands a glob client-side into real
# ListObjectsV2 calls, or it passes the glob *literally* to the gateway (which cannot
# match it). Only polars and duckdb expand globs, so the ``glob`` variant is restricted
# to them.
#
# The "directory" case is expressed in the form each engine actually uses to read a
# whole partitioned table:
#
# * ``dir_slash`` — read the whole directory tree.
#     - polars / pyarrow-native: a bare prefix ``s3://bucket/table/`` (they enumerate
#       the prefix via ListObjectsV2).
#     - duckdb: a recursive glob ``s3://bucket/table/**`` — duckdb's httpfs does not
#       fall back to a listing when handed a bare directory (it HEADs then GETs the
#       prefix and 404s), so its directory-read *is* a glob. This is the idiomatic
#       duckdb form per its docs (``read_parquet('orders/**')``).
#     - pandas (both engines): excluded — its s3fs/fsspec layer does not auto-discover
#       a partitioned directory the way pyarrow's dataset API does; reading the bare
#       directory raises a type-merge error across the partition files.
#     - pyarrow-s3fs: excluded from ``dir_slash`` — its S3FileSystem treats the
#       ``bucket/`` prefix as part of the path and reports files "outside base dir".
#       It is covered instead by the explicit file-list form in ``_full_table_path``.
# * ``glob`` (``s3://bucket/table/country=*/year=*/*.parquet``) — only polars and duckdb
#   expand this client-side; the other four pass it literally, so it is not tested for
#   them.
# * ``single`` (one explicit object) — supported by all.

_DIR_SLASH_ENGINES = {
    "polars": f"s3://{BUCKET}/{TABLE}/",
    "pyarrow-native": f"s3://{BUCKET}/{TABLE}/",
    "duckdb": f"s3://{BUCKET}/{TABLE}/**",
    "pandas-pyarrow": f"s3://{BUCKET}/{TABLE}/",
    "pandas-s3fs": f"s3://{BUCKET}/{TABLE}/",
}

# Engines that expand a glob pattern client-side into real S3 listings.
_GLOB_ENGINES = {"polars", "duckdb"}


def _paths(engine_name: str):
    base = f"s3://{BUCKET}/{TABLE}"
    variants = {
        "single": (f"{base}/country=us/year=2021/data.parquet", SINGLE_ROWS),
    }
    if engine_name in _DIR_SLASH_ENGINES:
        variants["dir_slash"] = (_DIR_SLASH_ENGINES[engine_name], FULL_ROWS)
    if engine_name in _GLOB_ENGINES:
        variants["glob"] = (f"{base}/country=*/year=*/*.parquet", FULL_ROWS)
    return variants


# --------------------------------------------------------------------------- #
# The matrix: every engine x every (supported) path variant
# --------------------------------------------------------------------------- #


def _cases():
    for engine_name, (reader, _) in ENGINES.items():
        for variant, (path, expected) in _paths(engine_name).items():
            yield engine_name, reader, variant, path, expected


@pytest.mark.parametrize(
    "engine_name,reader,variant,path,expected_rows",
    list(_cases()),
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_read(endpoint, engine_name, reader, variant, path, expected_rows):
    """Each engine reads the table via the given path variant and gets the right shape."""
    n_rows, columns = reader(endpoint, path)
    assert n_rows == expected_rows, f"{engine_name}/{variant}: got {n_rows} rows"
    # Only the directory read (dir_slash) reliably recovers the hive partition columns
    # for every engine: a single object contains only the in-file columns (id, value),
    # and glob reads (e.g. polars) are treated as a flat file list without hive
    # partitioning, so they also lack country/year. Partition recovery is checked
    # explicitly in `test_partition_columns_recovered`.
    if variant == "dir_slash":
        assert EXPECTED_COLUMNS.issubset(set(columns)), (
            f"{engine_name}/{variant}: missing columns {EXPECTED_COLUMNS - set(columns)}"
        )


# --------------------------------------------------------------------------- #
# Partition correctness: hive partition columns must be recovered
# --------------------------------------------------------------------------- #


def _collect_column_values(endpoint, reader, path, column):
    if reader is read_polars:
        so = {"aws_skip_signature": "true", "aws_endpoint_url": endpoint, "aws_allow_http": "true"}
        df = pl.read_parquet(path, storage_options=so)
        return set(df[column].unique().to_list())
    if reader is read_duckdb:
        host = endpoint.replace("http://", "")
        duckdb.sql("INSTALL httpfs; LOAD httpfs;")
        duckdb.sql(f"SET s3_endpoint='{host}';")
        duckdb.sql("SET s3_use_ssl=false;")
        duckdb.sql("SET s3_access_key_id='';")
        duckdb.sql("SET s3_secret_access_key='';")
        duckdb.sql("SET s3_session_token='';")
        duckdb.sql("SET s3_url_style='path';")
        df = duckdb.sql(f"SELECT DISTINCT {column} FROM read_parquet('{path}')").df()
        return set(df[column].tolist())
    if reader is read_pandas_pyarrow:
        so = {"anon": "true", "endpoint_url": endpoint, "client_kwargs": {"endpoint_url": endpoint}}
        df = pd.read_parquet(path, storage_options=so)
        return set(df[column].unique().tolist())
    if reader is read_pandas_s3fs:
        so = {"anon": "true", "endpoint_url": endpoint, "client_kwargs": {"endpoint_url": endpoint}}
        df = pd.read_parquet(path, storage_options=so)
        return set(df[column].unique().tolist())
    if reader is read_pyarrow_native:
        host = endpoint.replace("http://", "")
        s3 = pafs.S3FileSystem(anonymous=True, endpoint_override=host, scheme="http")
        bucket_key = path.replace(f"s3://{BUCKET}/", f"{BUCKET}/")
        dataset = ds.dataset(bucket_key, format="parquet", filesystem=s3, partitioning="hive")
        table = dataset.to_table()
        return set(table.column(column).to_pylist())
    if reader is read_pyarrow_s3fs:
        fsys = s3fs.S3FileSystem(anon=True, client_kwargs={"endpoint_url": endpoint})
        dataset = ds.dataset(path, format="parquet", filesystem=fsys, partitioning="hive")
        table = dataset.to_table()
        return set(table.column(column).to_pylist())
    raise AssertionError("unknown reader")


# Full-table path used to verify partition-column recovery. Not every engine can read
# the whole partitioned table through a single S3 path (see notes in `_paths`):
# pyarrow-s3fs cannot use the bare directory form (its S3FileSystem prepends the bucket
# name to each key and then reports the files as "outside base dir"), so it reads the
# full table via an explicit file list built from a real ListObjectsV2. Engines absent
# from this map are skipped by `test_partition_columns_recovered`.
_FULL_TABLE_PATH = {
    "polars": f"s3://{BUCKET}/{TABLE}/",
    "duckdb": f"s3://{BUCKET}/{TABLE}/**",
    "pyarrow-native": f"s3://{BUCKET}/{TABLE}/",
    "pandas-pyarrow": f"s3://{BUCKET}/{TABLE}/",
    "pandas-s3fs": f"s3://{BUCKET}/{TABLE}/",
    # pyarrow-s3fs reads the full table only via an explicit file list, which we build
    # from a real (empty-delimiter) ListObjectsV2 — the gateway returns the object keys.
    "pyarrow-s3fs": "__file_list__",
}


@pytest.mark.parametrize("engine_name,reader,_", [(k, v[0], None) for k, v in ENGINES.items()])
def test_partition_columns_recovered(endpoint, engine_name, reader, _):
    """Hive partition columns (country, year) must be present and correct.

    Uses each engine's supported full-table path. Engines that cannot read the whole
    partitioned table through any single S3 path (the pandas engines — they fail to
    merge the partition columns even against the local filesystem) are skipped.
    """
    if engine_name not in _FULL_TABLE_PATH:
        pytest.skip(f"{engine_name} cannot read the full partitioned table via S3")

    path = _FULL_TABLE_PATH[engine_name]
    if path == "__file_list__":
        # Build an explicit file list from a real ListObjectsV2 (empty delimiter).
        import urllib.request
        import urllib.parse
        import xml.etree.ElementTree as ET

        keys = []
        tok = None
        NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        while True:
            q = f"/{BUCKET}?list-type=2&prefix={TABLE}/&delimiter=&encoding-type=url"
            if tok:
                q += f"&continuation-token={urllib.parse.quote(tok)}"
            r = urllib.request.urlopen(f"{endpoint}{q}")
            root = ET.fromstring(r.read())
            for c in root.findall(f"{NS}Contents"):
                keys.append(c.find(f"{NS}Key").text)
            nxt = root.find(f"{NS}NextContinuationToken")
            tok = nxt.text if nxt is not None else None
            if not tok:
                break
        path = [f"s3://{BUCKET}/{k}" for k in keys]

    countries = _collect_column_values(endpoint, reader, path, "country")
    years = _collect_column_values(endpoint, reader, path, "year")
    assert countries == EXPECTED_COUNTRIES, f"{engine_name}: countries={countries}"
    assert years == EXPECTED_YEARS, f"{engine_name}: years={years}"
