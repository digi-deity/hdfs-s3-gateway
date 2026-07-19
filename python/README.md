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

The gateway performs **no authentication** — it must run behind network-level access
control. See the top-level project README for the full security posture.
