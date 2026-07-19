"""Python bindings for the HDFS → S3 read-only gateway.

This package wraps the Rust gateway so it can be driven from Python either as a
foreground blocking call or as an in-process background server.

Examples
--------
Run in the foreground (blocks until stopped from another thread)::

    from hdfs_s3_gateway import Gateway

    gw = Gateway(namenode_uri="hdfs://localhost:8020", hdfs_root="/data", bucket_name="hdfs")
    gw.serve_blocking()  # blocks

Run in the background (returns immediately; the server runs on its own thread)::

    gw = Gateway(config_path="gateway.toml")
    gw.start()          # non-blocking
    print(gw.address)   # e.g. "127.0.0.1:8080"
    # ... use the S3 endpoint from any S3 client ...
    gw.stop()           # graceful shutdown

No subprocess is spawned for the background server: it runs on a dedicated native
thread with its own tokio runtime, so the Python interpreter stays responsive.
"""

from __future__ import annotations

import atexit
import weakref
from contextlib import contextmanager
from typing import Iterator, Optional

from ._internal import Gateway as _Gateway
from ._internal import GatewayError

# Track every Gateway started in the background so we can shut them down gracefully
# when the interpreter exits. The server runs on a *native* OS thread (not a Python
# threading.Thread), so CPython's shutdown does NOT wait for it — without this hook a
# forgotten `start()` would just be killed abruptly at process exit. Registering here
# gives a best-effort graceful drain instead.
_live_gateways: "weakref.WeakSet[Gateway]" = weakref.WeakSet()


def _shutdown_live_gateways() -> None:
    for gw in list(_live_gateways):
        try:
            if gw.is_running:
                gw.stop()
        except Exception:
            pass


atexit.register(_shutdown_live_gateways)

# Process-global logging configuration. `tracing` installs a single global subscriber
# per process, so this can only be set once — before the first gateway is constructed.
# `gateway()` reads these when it builds a Gateway and then freezes them.
_log_level: "Optional[str]" = None
_log_target: str = "stderr"
_log_frozen: bool = False


def set_logging(level: "Optional[str]" = None, *, to: str = "stderr") -> None:
    """Configure gateway logging (process-global; set once before the first gateway).

    Parameters
    ----------
    level:
        Rust tracing level (e.g. ``"info"``, ``"debug"``). ``None`` (default) keeps the
        gateway silent unless ``RUST_LOG`` is set in the environment.
    to:
        Destination: ``"stderr"`` (default) or ``"python"`` (bridged into the
        :mod:`logging` module under the ``hdfs_s3_gateway`` logger).

    Raises
    ------
    RuntimeError:
        If a gateway has already been constructed (the setting is frozen once consumed).
    """
    global _log_level, _log_target, _log_frozen
    if _log_frozen:
        raise RuntimeError(
            "logging is already configured; it can only be set before the first gateway is created"
        )
    if to not in ("stderr", "python"):
        raise ValueError("'to' must be 'stderr' or 'python'")
    _log_level = level
    _log_target = to


__all__ = ["Gateway", "GatewayError", "gateway", "serving", "set_logging"]


def gateway(
    config_path: Optional[str] = None,
    *,
    namenode_uri: Optional[str] = None,
    hdfs_root: Optional[str] = None,
    bucket_name: Optional[str] = None,
    listen_addr: Optional[str] = None,
) -> "Gateway":
    """Construct a :class:`Gateway`.

    Parameters
    ----------
    config_path:
        Optional path to a TOML configuration file. If omitted, configuration is taken
        from environment variables and the keyword overrides below.
    namenode_uri, hdfs_root, bucket_name, listen_addr:
        Optional overrides applied on top of the config file / environment.
    Logging is configured process-globally via :func:`set_logging` and is frozen once the
    first gateway is created. By default the gateway is **silent** and emits no logs
    (unless ``RUST_LOG`` is set). Logs never go to Python's ``stdout``.
    """
    global _log_frozen
    log_level = _log_level
    log_target = _log_target
    _log_frozen = True
    if config_path is None:
        gw = _Gateway(
            namenode_uri=namenode_uri,
            hdfs_root=hdfs_root,
            bucket_name=bucket_name,
            listen_addr=listen_addr,
            log_level=log_level,
            log_to=log_target,
        )
    else:
        gw = _Gateway(
            config_path,
            namenode_uri=namenode_uri,
            hdfs_root=hdfs_root,
            bucket_name=bucket_name,
            listen_addr=listen_addr,
            log_level=log_level,
            log_to=log_target,
        )
    _live_gateways.add(gw)
    return gw


@contextmanager
def serving(
    config_path: Optional[str] = None,
    *,
    namenode_uri: Optional[str] = None,
    hdfs_root: Optional[str] = None,
    bucket_name: Optional[str] = None,
    listen_addr: Optional[str] = None,
) -> Iterator[Gateway]:
    """Context manager that starts the gateway in the background and stops it on exit.

    Example::

        with serving(listen_addr="127.0.0.1:0") as gw:
            print(gw.address)
            # ... use the S3 endpoint ...
        # gw.stop() is called automatically here (graceful shutdown)

    This is the recommended way to run the gateway from Python: it guarantees the
    background server is shut down even if the ``with`` block raises.

    Logging is configured process-globally via :func:`set_logging` (see :func:`gateway`).
    """
    gw = gateway(
        config_path,
        namenode_uri=namenode_uri,
        hdfs_root=hdfs_root,
        bucket_name=bucket_name,
        listen_addr=listen_addr,
    )
    gw.start()
    try:
        yield gw
    finally:
        gw.stop()


# Re-export the pyo3 class directly. Subclassing a pyo3 type from Python is fragile
# across versions, so we expose it as-is and provide the `gateway(...)` factory plus
# a context-manager helper below.
Gateway = _Gateway

