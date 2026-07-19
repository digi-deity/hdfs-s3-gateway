# HDFS → S3 Gateway (read-only)

A lightweight Rust service that exposes an HDFS cluster **as if it were an S3-compatible
endpoint**, for the read operations it supports. Any tool with a good S3 client — query
engines (Spark, Trino, DuckDB), `rclone`, `s5cmd`, `aws` CLI, data-lake frameworks
(Iceberg/Delta) — can read HDFS-resident data with zero HDFS-specific code on its side.

It is a **translation layer, not a reimplementation of S3**: it speaks the S3 HTTP protocol
(via the [`s3s`](https://github.com/Nugine/s3s) crate) in front of an HDFS client
([`hdfs-native`](https://github.com/datafusion-contrib/hdfs-native), a from-scratch async
Rust HDFS client — no JVM at runtime).

> **Scope:** this is the **read-only** first version. All write-shaped operations
> (`PUT`, `DELETE`, `COPY`, multipart upload, bucket create/delete, …) return a uniform
> `AccessDenied`. See [Operations](#operations-supported).

---

## Operations supported

| Operation | Status |
|---|---|
| `HeadBucket`, `ListBuckets` | Supported |
| `HeadObject` | Supported |
| `GetObject` (full body, byte ranges, conditionals `If-Match`/`If-None-Match`/`If-Modified-Since`/`If-Unmodified-Since`) | Supported (streamed, never buffered in full) |
| `ListObjectsV2` (prefix, delimiter, max-keys, continuation token) | Supported |
| `GetBucketVersioning` / `GetBucketTagging` / `GetBucketAcl` / `GetBucketCors` | Return the "not configured" shapes a fresh S3 bucket returns (no error) |
| All write-shaped ops (`PutObject`, `DeleteObject`, `CopyObject`, multipart, `CreateBucket`, `DeleteBucket`, …) | Uniform `AccessDenied` (read-only by design) |

**Directories are not objects**: a `GetObject`/`HeadObject` whose key resolves to an
HDFS directory returns `404 NoSuchKey`. Subdirectories appear only as `CommonPrefixes` in
listing.

---

## Bucket / key mapping

A single configured bucket (e.g. `hdfs`) is exposed, backed by a single HDFS root
path configured for this gateway (e.g. `hdfs_root = "/data"`). This configured root is
**not** HDFS's own filesystem root (`/`) — it is the directory you chose to expose, and
every S3 key is resolved *relative to it*. An S3 URI `s3://hdfs/foo/bar.parquet` in this example 
therefore maps to the absolute HDFS path `/data/foo/bar.parquet`. Path traversal attempts (`../`)
are rejected.

| S3 key | HDFS path (configured `hdfs_root` = `/data`) |
|---|---|
| `file.txt` | `/data/file.txt` |
| `foo/bar.parquet` | `/data/foo/bar.parquet` |
| `` (empty / bucket root) | — (not an object; `404 NoSuchKey`) |

---

## Configuration

Configuration is loaded from an optional TOML file (`--config <path>`), overridden by CLI
flags, or built from environment variables when no file is given. Invalid/missing values fail
fast at startup.

| Setting | Env var | CLI flag | Default | Notes |
|---|---|---|---|---|
| `namenode_uri` | `HDFS_NN_URI` | `--namenode-uri` | — (required) | e.g. `hdfs://namenode:8020` |
| `hdfs_root` | `HDFS_ROOT` | `--hdfs-root` | — (required) | S3 keys are relative to this |
| `bucket_name` | `BUCKET_NAME` | `--bucket-name` | — (required) | single exposed bucket; must be a legal S3 bucket name |
| `listen_addr` | — | `--listen-addr` | `0.0.0.0:8080` | **Bind to a private interface in production** |
| `max_concurrent_requests` | — | — | `2048` | Backpressure cap |
| `expose_upstream_errors` | — | — | `true` | See [security](#upstream-error-exposure-expose_upstream_errors) |
| `hdfs_options` | — | — | `{}` | Free-form `HashMap<String,String>` → `hdfs-native` `ClientBuilder::with_config` (raw Hadoop keys; **override** XML) |
| `hdfs_config_dir` | — | — | `None` | Optional → `with_config_dir`; **replaces** `HADOOP_CONF_DIR`/`HADOOP_HOME` fallback when set |
| `hdfs_user` | — | — | `None` | Optional → `with_user`; else `HADOOP_USER_NAME`/`HADOOP_PROXY_USER` |

Example `config.toml`:

```toml
namenode_uri = "hdfs://namenode:8020"
hdfs_root    = "/data"
bucket_name  = "hdfs"
listen_addr  = "127.0.0.1:8080"   # private interface — not public
max_concurrent_requests = 2048

# Optional: raw Hadoop config overrides
hdfs_options = { "dfs.client.use.datanode.hostname" = "true" }
```

---

## Running

```bash
# Build
cargo build --release

# Run (config file)
./target/release/hdfs-s3-gateway --config config.toml

# Or via environment variables
HDFS_NN_URI=hdfs://namenode:8020 HDFS_ROOT=/data BUCKET_NAME=hdfs \
  ./target/release/hdfs-s3-gateway

# Log level via RUST_LOG / the tracing env filter (default: info)
RUST_LOG=debug ./target/release/hdfs-s3-gateway
```

The gateway listens on `listen_addr`, serves the single bucket, and gracefully drains
in-flight requests on `SIGTERM`/`SIGINT` (no abrupt connection drops).

### Talking to it

```bash
# List the bucket
aws --endpoint-url=http://127.0.0.1:8080 s3 ls s3://hdfs/

# Read an object (range supported)
aws --endpoint-url=http://127.0.0.1:8080 s3 cp s3://hdfs/foo/bar.parquet ./bar.parquet
curl -H "Range: bytes=0-1023" http://127.0.0.1:8080/hdfs/foo/bar.parquet
```

No credentials are required (by design — see [Security](#️-security-posture-read-this-before-deploying)).

---

## Python bindings

The gateway is also exposed as a Python package (`hdfs_s3_gateway`), built with
[`pyo3`](https://pyo3.rs) + [`maturin`](https://www.maturin.rs). It wraps the same Rust
binary logic, so the Python API and the CLI behave identically.

### Build the wheel

```bash
# From the repo root, in a virtualenv:
python -m venv .venv && source .venv/bin/activate
pip install maturin
cd python
maturin build --release        # produces ../target/wheels/*.whl
pip install ../target/wheels/hdfs_s3_gateway-*.whl
```

The wheel is built as **abi3** (`abi3-py310`), so a single build works on **any CPython ≥ 3.10**
— no need to compile per Python version.

### Use it

Two modes, mirroring the CLI:

```python
from hdfs_s3_gateway import gateway

# Background server — runs on its own native thread + tokio runtime.
# No subprocess is spawned, and the GIL is released while it runs, so your
# Python process stays responsive. Call stop() to shut it down gracefully.
gw = gateway(
    namenode_uri="hdfs://namenode:8020",
    hdfs_root="/data",
    bucket_name="hdfs",
    listen_addr="127.0.0.1:0",   # 0 → ephemeral port
)
gw.start()
print(gw.address)     # e.g. "127.0.0.1:34711"
# ... point any S3 client at the endpoint ...
gw.stop()

# Foreground server — blocks the calling thread until stop() is called from
# another thread (e.g. a signal handler or a second thread).
gw = gateway(config_path="config.toml")
gw.serve_blocking()
```

The recommended pattern is the `serving()` context manager, which starts the server and
**always stops it gracefully** (even if the block raises):

```python
from hdfs_s3_gateway import serving

with serving(listen_addr="127.0.0.1:0") as gw:
    print(gw.address)   # e.g. "127.0.0.1:39111"
    # ... point any S3 client at the endpoint ...
# gw.stop() is called automatically here
```

`gateway(...)` / `serving(...)` accept the same options as the CLI: `config_path` (TOML file),
or the `namenode_uri` / `hdfs_root` / `bucket_name` / `listen_addr` overrides (applied on top of
the config file / environment). The `Gateway` object exposes `listen_addr`, `address` (the
actually-bound address, once running), `is_running`, `start()`, `stop()`, and
`serve_blocking()`.

> **Process-exit behavior:** the background server runs on a *native* OS thread, not a Python
> `threading.Thread`, so CPython's shutdown does **not** wait for it — a forgotten `start()`
> will never hang Python exit. As a safety net, an `atexit` hook stops any live gateway
> gracefully on interpreter exit (best-effort drain). For guaranteed clean shutdown, prefer
> `serving(...)` or an explicit `stop()`.

### Logging

By default the gateway is **silent** — it emits no logs and never writes to Python's
`stdout`. This is deliberate: a library should not decide logging for its host, and the
previous behavior (writing to the raw stdout file descriptor) bypassed `sys.stdout`
redirection and corrupted captured/notebook output.

Logging is configured **process-globally** with `set_logging()` and is frozen once the
first gateway is constructed. When you opt in via `level`, logs go to one of two
destinations chosen by `to`:

| `to` | Destination | Notes |
|---|---|---|
| `"stderr"` (default) | process **stderr** | Never stdout. Plain single-line format. |
| `"python"` | Python `logging` module | Bridged into the `hdfs_s3_gateway` logger. |

```python
from hdfs_s3_gateway import gateway, set_logging

# Silent (default) — no output at all:
gw = gateway(listen_addr="127.0.0.1:0")

# Logs to stderr at info level (call before constructing the gateway):
set_logging("info", to="stderr")
gw = gateway(listen_addr="127.0.0.1:0")

# Logs into Python's logging under the "hdfs_s3_gateway" logger:
import logging
logging.basicConfig(level=logging.INFO)
set_logging("info", to="python")
gw = gateway(listen_addr="127.0.0.1:0")
```

**Non-blocking by design.** The server thread only *enqueues* log lines onto an in-process
channel; a dedicated consumer thread drains the channel and performs the (GIL-contended)
Python `logging` call. The server thread therefore never waits on the GIL, so logging can
never hold back request handling, and no log line is dropped under normal operation. The
consumer uses a non-blocking GIL acquire (`try_attach`) so it also cannot deadlock the
interpreter at shutdown.

> **One subscriber per process.** `tracing` installs a single global subscriber, so
> logging can only be configured once. `set_logging()` stores the setting at the module
> level and freezes it the moment the first gateway is constructed; a later call raises
> `RuntimeError` instead of silently changing the destination. `RUST_LOG` is still honored
> as the default filter when `level` is omitted.

> The Python package deliberately does **not** re-expose `hdfs-native`'s Python client — if you
> want raw HDFS access from Python, install the separate `hdfs-native` package. This package is
> only the S3 gateway server.

---

## Testing

Integration tests spin up a real `MiniDFSCluster` via `hdfs-native`'s `minidfs` feature, which
requires a JVM 17+, Maven, and Hadoop binaries on `PATH`:

- Java 17 (matches the version validated by the upstream `hdfs-native` project)
- `JAVA_HOME` pointing to the JDK (set automatically by `actions/setup-java` in CI)
- `HADOOP_HOME=/opt/hadoop-3.4.2` (or your Hadoop 3.4.x install)
- `PATH=$PATH:$HADOOP_HOME/bin`
- `mvn` available

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64   # or wherever JDK 17 lives
export HADOOP_HOME=/opt/hadoop-3.4.2
export PATH=$PATH:$HADOOP_HOME/bin

# Unit tests (no cluster needed)
cargo test --lib

# Integration tests — MUST run single-threaded; MiniDFSCluster hardcodes its work dir
rm -rf target/test
cargo test --test integration -- --test-threads=1
```

> **Why `--test-threads=1`:** the MiniDFSCluster harness hardcodes `target/test` as its work
> directory, so parallel test binaries clobber each other's `dfs/name-*` dirs. Run serially and
> clean the work dir between runs.

Test layout:

- `src/core` / `src/config` — pure unit tests (path mapping, range math, config).
- `tests/integration.rs` — end-to-end against MiniDFS (head/get/list/write-denied).
- `tests/load.rs` — concurrent-read throughput scaling.
- `tests/backpressure_test.rs` — `503 SlowDown` under load.
- `tests/conformance.rs` — real-HTTP conformance (headers, ranges, error shapes).
- `tests/graceful_shutdown.rs` — graceful drain of in-flight GETs + request-id logging.
- `tests/error_propagation.rs` — `hdfs-native` error → S3 code + server-side log correlation.

---

## ⚠️ Security posture

**This service performs NO authentication or authorization.** It does not validate the
`Authorization` header, SigV4 query parameters, or anything else — requests are accepted
whether or not they carry credentials, and access is never denied for auth reasons.

**Consequence: anyone with network access to this service can read all exposed HDFS data.**

This is **not optional hardening** — running this gateway exposed to an untrusted network is
a data breach. You MUST place it behind network-level access control:

- Bind `listen_addr` to a **private interface / private subnet** (not `0.0.0.0` on a public host).
- Put it behind a firewall / security-group / VPC routing rules that only allow trusted clients.
- Or front it with a reverse proxy that enforces its own auth (and terminates TLS — see below).

`s3s` itself documents that it has **no built-in security protection**; body-size limits,
rate limiting, and backpressure are the integrator's responsibility (implemented here in
`backpressure.rs`). None of that substitutes for network-level access control.

### TLS

There is **no TLS termination in this service**. Terminate TLS at a reverse proxy / load
balancer in front of it; do not add TLS handling inside the gateway.

### Upstream error exposure (`expose_upstream_errors`)

By default (`expose_upstream_errors = true`), the text of an upstream `hdfs-native` error is
returned to the client in the S3 error `Message`. HDFS error strings can leak NameNode
internals (hostnames, paths, exception class names), so in untrusted deployments set
`expose_upstream_errors = false` to suppress them.

The upstream error is **always logged server-side** regardless of this setting, correlated
with the per-request `x-amz-request-id` returned to the client — operators match a client's
request id to the server logs to diagnose failures. Set `expose_upstream_errors = false` in
untrusted deployments; leave it `true` (the default) in trusted, internal deployments where
surfacing the raw HDFS error in the S3 `Message` field is useful for debugging.

---

## ETag

> **Current limitation — checksum not available upstream.** The `hdfs-native` client (the
> version this gateway depends on) does **not** expose the NameNode `getFileChecksum` RPC, and
> its `FileStatus` type carries no checksum field. So the HDFS-native checksum cannot be
> fetched today. Until that support lands upstream, the ETag falls back to a deterministic
> value derived from the object's length and modification time (e.g. `hdfs-<len>-<mtime>`),
> which is stable across requests for an unchanged object but is **not** the real HDFS
> checksum. The ETag plumbing is already in place and will switch to the true checksum
> automatically once `hdfs-native` exposes it.

---

## Repository layout

```
src/
  lib.rs            # declares the config / core / s3 submodules
  config/           # config loading/validation (TOML + env + CLI overrides)
  core/             # PathMapper, metadata translation, range math (no s3s/HTTP types)
  s3/               # impl S3 for HdfsGateway + binary hdfs-s3-gateway
  main.rs           # binary entry point
tests/              # integration / load / conformance / shutdown / error tests
python/             # Python bindings (maturin extension module)
```

The S3 protocol layer ([`s3s`](https://github.com/Nugine/s3s)) and the HDFS client
([`hdfs-native`](https://github.com/datafusion-contrib/hdfs-native)) are used as published
crate dependencies (see `Cargo.toml`), not vendored into the tree.
