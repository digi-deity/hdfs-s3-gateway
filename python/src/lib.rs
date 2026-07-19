//! Python bindings for the HDFS→S3 gateway.
//!
//! Exposes a `Gateway` class that can be run either in the foreground (blocking the
//! calling thread) or in the background (a dedicated OS thread running a tokio runtime,
//! so the Python caller keeps control of the GIL and can `stop()` it later). This mirrors
//! the pattern used by `hdfs-native`'s Python wrapper, but for a long-running server
//! rather than a client.
//!
//! Running in the background does NOT require a subprocess: we spawn a native thread and
//! release the GIL while it runs, so other Python threads (and the interpreter) stay
//! responsive. `stop()` signals the server to gracefully drain and joins the thread.

use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;

use pyo3::create_exception;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use tokio::sync::Notify;

use hdfs_s3_gateway::config::{CliArgs, Config};
use hdfs_s3_gateway::s3::{server, HdfsGateway};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::prelude::*;

create_exception!(_internal, GatewayError, pyo3::exceptions::PyException);

/// Build a tokio multi-threaded runtime for serving.
fn build_runtime() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| GatewayError::new_err(format!("failed to build async runtime: {e}")))
}

/// Thread-local flag: when set, the global panic hook stays silent for that thread.
/// Our background server thread sets this so an (injected or real) panic is reported
/// through the `tx` channel rather than dumping a backtrace to stderr.
thread_local! {
    static SUPPRESS_PANIC_PRINT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Install a panic hook that defers to the *default* hook for every thread EXCEPT our
/// server thread (flagged via `SUPPRESS_PANIC_PRINT`). Idempotent. This keeps a panic in
/// the server thread from spewing a backtrace to stderr while leaving normal Rust panic
/// behavior intact everywhere else.
fn install_panic_hook() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if SUPPRESS_PANIC_PRINT.with(|f| f.get()) {
                return;
            }
            prev(info);
        }));
    });
}

/// Extract a human-readable message from a panic payload.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// A `tracing_subscriber` writer that forwards log lines into a Python `logging`
/// logger **without ever blocking the server thread**. `write` only enqueues the line
/// onto an unbounded channel; a dedicated consumer thread (see `spawn_log_consumer`)
/// drains the channel and calls into Python's `logging`. This keeps request handling
/// off the critical path: the server never waits on the GIL, and no log line is dropped.
struct PyLogger {
    tx: std::sync::mpsc::Sender<String>,
}

impl PyLogger {
    fn new(tx: std::sync::mpsc::Sender<String>) -> Self {
        Self { tx }
    }
}

impl Write for PyLogger {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Ok(s) = std::str::from_utf8(buf) {
            // Non-blocking enqueue: no GIL, no wait. The consumer thread does the
            // actual Python call. Ordering is preserved because the channel is FIFO.
            let _ = self.tx.send(s.to_string());
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for PyLogger {
    type Writer = PyLogger;
    fn make_writer(&'a self) -> Self::Writer {
        PyLogger::new(self.tx.clone())
    }
}

/// Drain the log channel and forward each complete line into the Python `logging`
/// logger. Runs on its own OS thread so the server thread is never involved in the
/// (potentially GIL-contended) Python call. `tx` is dropped when the layer is torn
/// down, which closes `rx` and ends this loop. The logger is resolved lazily on this
/// thread (the only one that touches the GIL), so the server thread never blocks on it.
fn spawn_log_consumer(rx: std::sync::mpsc::Receiver<String>) {
    std::thread::spawn(move || {
        // Resolve the logger once, off the server's critical path.
        let logger: Option<Py<PyAny>> = Python::attach(|py| {
            py.import("logging")
                .and_then(|m| m.getattr("getLogger"))
                .and_then(|g| g.call1(("hdfs_s3_gateway",)))
                .ok()
                .map(|l| l.unbind())
        });
        let mut pending = String::new();
        while let Ok(chunk) = rx.recv() {
            pending.push_str(&chunk);
            while let Some(idx) = pending.find('\n') {
                let line: String = pending.drain(..=idx).collect();
                emit_line(&logger, &line);
            }
        }
        // Channel closed: flush any trailing partial line.
        if !pending.is_empty() {
            emit_line(&logger, &pending);
        }
    });
}

/// Call `logger.info(line)` from the consumer thread. Uses `try_attach` (non-blocking)
/// so the consumer never deadlocks the interpreter at shutdown — if the GIL is held by
/// the main thread or the interpreter is finalizing, `try_attach` returns `None`
/// immediately instead of blocking. We retry briefly so a line is only dropped under
/// sustained GIL contention or during finalization, never just because the main thread
/// was momentarily busy.
fn emit_line(logger: &Option<Py<PyAny>>, line: &str) {
    let line = line.trim_end_matches(['\r', '\n']);
    if line.is_empty() {
        return;
    }
    let Some(logger) = logger else { return };
    for _ in 0..50 {
        let attached = Python::try_attach(|py| {
            let _ = logger.bind(py).call_method1("info", (line,));
        });
        if attached.is_some() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// Ensures tracing is initialized at most once per process. A second call (e.g. calling
/// `serve_blocking` after `start`) is a no-op, avoiding a leaked consumer thread.
static INIT_TRACING: std::sync::Once = std::sync::Once::new();

/// Initialize tracing. By default (`level = None` and no `RUST_LOG`) nothing is
/// installed, so the gateway is **silent** and never touches Python's stdout. When
/// enabled, logs go to stderr (never stdout) or, with `target = "python"`, are bridged
/// into the `logging` module under the `hdfs_s3_gateway` logger. Idempotent: a second
/// call is a no-op (the global subscriber is already set).
fn init_tracing(level: Option<String>, target: String) {
    let has_env = std::env::var("RUST_LOG").is_ok();
    let level_str = match level {
        Some(l) => l,
        // Respect RUST_LOG for power users even when no explicit level was given.
        None if has_env => "info".to_string(),
        None => return,
    };

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level_str));

    INIT_TRACING.call_once(|| {
        if target == "python" {
            // Channel decouples the server thread (producer) from the Python logging
            // call (consumer). The server thread never touches the GIL — the consumer
            // thread resolves the logger and emits lines, so request handling is never
            // blocked on the GIL.
            let (tx, rx) = std::sync::mpsc::channel::<String>();
            spawn_log_consumer(rx);
            let layer = tracing_subscriber::fmt::layer()
                .with_writer(PyLogger::new(tx))
                .with_filter(filter);
            let _ = tracing_subscriber::registry().with(layer).try_init();
        } else {
            // Default target (anything other than "python") → stderr. Never stdout.
            let layer = tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(filter);
            let _ = tracing_subscriber::registry().with(layer).try_init();
        }
    });
}

/// The gateway server, controllable from Python.
///
/// Construct it with a config file path and/or keyword overrides, then either call
/// `serve_blocking()` (blocks the calling thread until `stop()` is invoked from another
/// thread) or `start()` (spawns a background thread and returns immediately so you can
/// keep using Python — call `stop()` to shut it down).
#[pyclass(subclass, weakref)]
struct Gateway {
    config: Arc<Config>,
    /// Shared shutdown signal. Cloned into the serving thread; `stop()` triggers it.
    shutdown: Arc<Notify>,
    /// Handle of the background thread, if `start()` was used.
    thread: Option<JoinHandle<()>>,
    /// The actually-bound address (ephemeral port resolved), set once serving begins.
    address: Option<String>,
    /// Optional explicit log level (e.g. "info", "debug"). `None` → silent unless
    /// `RUST_LOG` is set.
    log_level: Option<String>,
    /// Log destination: "stderr" (default) or "python" (bridge to `logging`).
    log_target: String,
}

#[pymethods]
impl Gateway {
    #[new]
    #[pyo3(signature = (config_path=None, *, namenode_uri=None, hdfs_root=None, bucket_name=None, listen_addr=None, auth_secret=None, log_level=None, log_to=None))]
    fn new(
        config_path: Option<PathBuf>,
        namenode_uri: Option<String>,
        hdfs_root: Option<String>,
        bucket_name: Option<String>,
        listen_addr: Option<String>,
        auth_secret: Option<String>,
        log_level: Option<String>,
        log_to: Option<String>,
    ) -> PyResult<Self> {
        let args = CliArgs {
            config: config_path,
            listen_addr,
            namenode_uri,
            hdfs_root,
            bucket_name,
            auth_secret,
        };
        let config = Config::load(&args)
            .map_err(|e| PyValueError::new_err(format!("invalid configuration: {e}")))?;
        Ok(Gateway {
            config: Arc::new(config),
            shutdown: Arc::new(Notify::new()),
            thread: None,
            address: None,
            log_level,
            log_target: log_to.unwrap_or_else(|| "stderr".to_string()),
        })
    }

    /// The configured listen address (what the server will try to bind to).
    #[getter]
    fn listen_addr(&self) -> String {
        self.config.listen_addr.clone()
    }

    /// The address the server actually bound to (only meaningful after `start()` /
    /// `serve_blocking()` has begun listening). For `0.0.0.0:0`-style configs this
    /// reflects the resolved ephemeral port.
    #[getter]
    fn address(&self) -> Option<String> {
        self.address.clone()
    }

    /// Run the gateway in the foreground, blocking the calling Python thread until
    /// `stop()` is called from another thread. The GIL is released while serving so the
    /// interpreter remains responsive to other threads.
    fn serve_blocking(&self, py: Python) -> PyResult<()> {
        let config = self.config.clone();
        let shutdown = self.shutdown.clone();
        let log_level = self.log_level.clone();
        let log_target = self.log_target.clone();
        let result = py.detach(move || {
            let rt = build_runtime()?;
            init_tracing(log_level, log_target);
            rt.block_on(async move {
                let gateway =
                    HdfsGateway::from_config(&config).map_err(|e| GatewayError::new_err(e))?;
                let service = server::build_service(gateway, &config);
                let addr: SocketAddr = config
                    .listen_addr
                    .parse()
                    .map_err(|e| GatewayError::new_err(format!("invalid listen_addr: {e}")))?;
                let listener = tokio::net::TcpListener::bind(addr)
                    .await
                    .map_err(|e| GatewayError::new_err(format!("failed to bind {addr}: {e}")))?;
                tracing::info!(
                    addr = %listener.local_addr().unwrap_or(addr),
                    "gateway listening (NO AUTH — must be behind network access control)"
                );
                let _ = server::serve(listener, service, async move { shutdown.notified().await })
                    .await;
                Ok::<(), PyErr>(())
            })
        });
        result
    }

    /// Start the gateway in a background OS thread and return immediately. The server runs
    /// on its own tokio runtime; the Python caller keeps the GIL and can call `stop()`
    /// later. No subprocess is spawned.
    fn start(&mut self, _py: Python) -> PyResult<()> {
        if self.thread.is_some() {
            return Err(GatewayError::new_err("gateway is already running"));
        }
        let config = self.config.clone();
        let shutdown = self.shutdown.clone();
        let log_level = self.log_level.clone();
        let log_target = self.log_target.clone();
        let (tx, rx) = mpsc::channel::<Result<String, String>>();

        let handle = std::thread::spawn(move || {
            // Suppress the default panic backtrace for this thread; panics are reported
            // through `tx` instead (see catch_unwind below).
            SUPPRESS_PANIC_PRINT.with(|f| f.set(true));
            install_panic_hook();

            let rt = match build_runtime() {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = tx.send(Err(e.to_string()));
                    return;
                }
            };
            init_tracing(log_level, log_target);

            let tx_inner = tx.clone();
            let serve = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                rt.block_on(async move {
                    let gateway = match HdfsGateway::from_config(&config) {
                        Ok(g) => g,
                        Err(e) => {
                            let _ = tx_inner.send(Err(e));
                            return;
                        }
                    };
                    let service = server::build_service(gateway, &config);
                    let addr: SocketAddr = match config.listen_addr.parse() {
                        Ok(a) => a,
                        Err(e) => {
                            let _ = tx_inner.send(Err(format!("invalid listen_addr: {e}")));
                            return;
                        }
                    };
                    let listener = match tokio::net::TcpListener::bind(addr).await {
                        Ok(l) => l,
                        Err(e) => {
                            let _ = tx_inner.send(Err(format!("failed to bind {addr}: {e}")));
                            return;
                        }
                    };
                    let bound = listener.local_addr().unwrap_or(addr).to_string();
                    let _ = tx_inner.send(Ok(bound.clone()));
                    tracing::info!(
                        addr = %bound,
                        "gateway listening (NO AUTH — must be behind network access control)"
                    );

                    // Test-only hook: force a panic AFTER bind so `start()` succeeds and
                    // the panic is contained by the surrounding `catch_unwind`. Gated
                    // behind the `test-hooks` feature and the `HDFS_S3_GW_TEST_PANIC` env
                    // var, so it never ships in the release wheel.
                    #[cfg(feature = "test-hooks")]
                    if std::env::var("HDFS_S3_GW_TEST_PANIC").is_ok() {
                        panic!("injected test panic in server thread");
                    }

                    let _ =
                        server::serve(listener, service, async move { shutdown.notified().await })
                            .await;
                })
            }));

            if let Err(payload) = serve {
                let _ = tx.send(Err(format!(
                    "gateway panicked: {}",
                    panic_message(&payload)
                )));
            }
        });

        // Wait for the server to bind and report its address, or for an error. This lets
        // `start()` surface bind/config errors synchronously. The wait is brief (until
        // bind completes), so we don't need to release the GIL here.
        let bound = rx.recv();
        match bound {
            Ok(Ok(addr)) => {
                self.address = Some(addr);
                self.thread = Some(handle);
                Ok(())
            }
            Ok(Err(e)) => Err(GatewayError::new_err(e)),
            Err(_) => Err(GatewayError::new_err(
                "gateway thread terminated before binding (likely a panic)",
            )),
        }
    }

    /// Stop a background gateway started with `start()`. Signals graceful shutdown and
    /// joins the background thread. No-op if the gateway is not running in the background.
    ///
    /// If the background thread had already terminated (e.g. it panicked), `join()`
    /// returns `Err` and we surface that as a `GatewayError` rather than silently
    /// leaving a zombied `Gateway` that still reports `is_running == True`.
    fn stop(&mut self, py: Python) -> PyResult<()> {
        // If the thread already finished on its own (e.g. it panicked and was caught),
        // `join()` would return `Ok` and we'd silently leave a dead gateway. Surface it.
        if let Some(handle) = &self.thread {
            if handle.is_finished() {
                self.thread = None;
                return Err(GatewayError::new_err(
                    "gateway background thread had already terminated (likely a panic)",
                ));
            }
        }
        self.shutdown.notify_one();
        if let Some(handle) = self.thread.take() {
            let _ = py.detach(|| handle.join());
        }
        Ok(())
    }

    /// Whether the gateway was started in the background via `start()` and its thread is
    /// still alive. Returns `False` if the thread panicked or already exited.
    #[getter]
    fn is_running(&self) -> bool {
        match &self.thread {
            Some(h) => !h.is_finished(),
            None => false,
        }
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Gateway>()?;
    m.add("GatewayError", m.py().get_type::<GatewayError>())?;
    // Exposed only when the `test-hooks` feature is compiled in, so the test-suite can
    // reliably detect whether the panic-injection path is available.
    #[cfg(feature = "test-hooks")]
    m.add("TEST_HOOKS", true)?;
    Ok(())
}
