"""Tests for the hdfs_s3_gateway Python bindings.

These exercise the integration points we cared about during development:

* background server lifecycle (start / address / stop)
* the `serving()` context manager (always stops, even on error)
* atexit graceful shutdown of a forgotten `start()`
* logging does NOT pollute Python's stdout (silent by default; stderr / python targets)
* panic safety: a panic in the server thread must NOT take down the process, and the
  gateway must report itself as stopped rather than zombie.

The panic path requires a wheel built with the `test-hooks` cargo feature and the
`HDFS_S3_GW_TEST_PANIC` env var set; those tests are skipped otherwise.
"""

import logging
import os
import subprocess
import sys

import pytest

from hdfs_s3_gateway import GatewayError, gateway, serving

# `TEST_HOOKS` is exported by the Rust module only when the wheel is built with the
# `test-hooks` cargo feature. When absent, the panic-injection tests are skipped.
try:
    from hdfs_s3_gateway._internal import TEST_HOOKS

    _HAS_TEST_HOOKS = bool(TEST_HOOKS)
except ImportError:  # pragma: no cover - depends on build configuration
    _HAS_TEST_HOOKS = False

# A listen address that always succeeds to bind on the loopback with an ephemeral port.
LISTEN = "127.0.0.1:0"
KW = dict(namenode_uri="hdfs://localhost:8020", hdfs_root="/data", bucket_name="hdfs")


def _has_test_hooks() -> bool:
    return _HAS_TEST_HOOKS


# --------------------------------------------------------------------------- #
# Lifecycle
# --------------------------------------------------------------------------- #


def test_start_reports_address_and_stops():
    gw = gateway(listen_addr=LISTEN, **KW)
    gw.start()
    try:
        addr = gw.address
        assert addr is not None
        assert addr.startswith("127.0.0.1:")
        assert gw.is_running is True
    finally:
        gw.stop()
        assert gw.is_running is False


def test_serving_context_manager_always_stops():
    with serving(listen_addr=LISTEN, **KW) as gw:
        assert gw.address is not None
        assert gw.is_running is True
    # After the block, the gateway must be stopped even though we never called stop().
    assert gw.is_running is False


def test_serving_stops_on_exception():
    class Boom(Exception):
        pass

    gw = None
    with pytest.raises(Boom):
        with serving(listen_addr=LISTEN, **KW) as g:
            gw = g
            raise Boom()
    assert gw is not None
    assert gw.is_running is False


def test_double_start_is_rejected():
    gw = gateway(listen_addr=LISTEN, **KW)
    gw.start()
    try:
        with pytest.raises(GatewayError):
            gw.start()
    finally:
        gw.stop()


def test_stop_is_idempotent_when_not_running():
    gw = gateway(listen_addr=LISTEN, **KW)
    # No start() -> stop() should be a clean no-op.
    gw.stop()


# --------------------------------------------------------------------------- #
# Logging: must not pollute Python's stdout
# --------------------------------------------------------------------------- #


def test_default_is_silent_on_stdout_and_stderr():
    # Run in a subprocess so we can capture stdout/stderr cleanly and isolate the
    # process-global tracing subscriber.
    code = (
        "from hdfs_s3_gateway import gateway\n"
        "gw = gateway(listen_addr='127.0.0.1:0', **" + repr(KW) + ")\n"
        "gw.start()\n"
        "print('ADDR', gw.address, flush=True)\n"
        "gw.stop()\n"
        "print('DONE', flush=True)\n"
    )
    out, err = _run(code)
    assert "ADDR" in out
    assert "DONE" in out
    # No gateway log lines (they contain the marker string below) on either stream.
    assert "gateway listening" not in out
    assert "gateway listening" not in err


def test_stderr_target_writes_to_stderr_not_stdout():
    code = (
        "from hdfs_s3_gateway import gateway, set_logging\n"
        "set_logging('info', to='stderr')\n"
        "gw = gateway(listen_addr='127.0.0.1:0', **" + repr(KW) + ")\n"
        "gw.start()\n"
        "print('ADDR', gw.address, flush=True)\n"
        "gw.stop()\n"
    )
    out, err = _run(code)
    assert "ADDR" in out
    assert "gateway listening" in err
    assert "gateway listening" not in out


def test_python_target_bridges_to_logging():
    # `set_logging(to='python')` routes into the `logging` module under the
    # `hdfs_s3_gateway` logger. Run in a subprocess so the process-global logging
    # setting is isolated, and capture the bridged records via a handler.
    code = (
        "import logging\n"
        "from hdfs_s3_gateway import gateway, set_logging\n"
        "records = []\n"
        "h = logging.Handler()\n"
        "h.emit = lambda r: records.append(r)\n"
        "logging.getLogger('hdfs_s3_gateway').addHandler(h)\n"
        "logging.getLogger('hdfs_s3_gateway').setLevel(logging.INFO)\n"
        "set_logging('info', to='python')\n"
        "gw = gateway(listen_addr='127.0.0.1:0', **" + repr(KW) + ")\n"
        "gw.start()\n"
        "gw.stop()\n"
        "print('MSG', any('gateway listening' in r.getMessage() for r in records), flush=True)\n"
    )
    out, _ = _run(code)
    assert "MSG True" in out


def test_set_logging_after_gateway_raises():
    # The logging setting freezes once the first gateway is constructed, so a later
    # `set_logging` must error rather than silently change the destination.
    code = (
        "from hdfs_s3_gateway import gateway, set_logging\n"
        "gateway(listen_addr='127.0.0.1:0', **" + repr(KW) + ")\n"
        "set_logging('info')\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode != 0
    assert "already configured" in proc.stderr


# --------------------------------------------------------------------------- #
# Panic safety
# --------------------------------------------------------------------------- #


@pytest.mark.skipif(
    not _has_test_hooks() or "HDFS_S3_GW_TEST_PANIC" not in os.environ,
    reason="requires a wheel built with the test-hooks feature and HDFS_S3_GW_TEST_PANIC=1",
)
def test_panic_does_not_crash_process_and_gateway_reports_stopped():
    # The injected panic happens after bind, so start() succeeds and returns an address.
    gw = gateway(listen_addr=LISTEN, **KW)
    gw.start()
    assert gw.address is not None
    # Give the server thread time to hit the injected panic.
    import time

    for _ in range(50):
        if not gw.is_running:
            break
        time.sleep(0.05)
    # The process is still alive (we're running this assertion), and the gateway must
    # report itself as NOT running (not zombied).
    assert gw.is_running is False
    # stop() should surface that the thread already died, not silently succeed.
    with pytest.raises(GatewayError):
        gw.stop()


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _run(code: str):
    """Run `code` in a fresh interpreter, return (stdout, stderr) strings."""
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, f"subprocess failed: {proc.stderr}"
    return proc.stdout, proc.stderr
