//! Reusable server wiring: builds the `s3s` service (with backpressure) and serves it over
//! hyper. Extracted from `main.rs` so integration/load tests can start the gateway in-process
//! against a real TCP port (load harness, backpressure test).

use std::sync::Arc;

use crate::config::Config;
use crate::s3::HdfsGateway;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use hyper_util::server::graceful::GracefulShutdown;
use s3s::config::{S3Config, StaticConfigProvider};
use s3s::service::S3ServiceBuilder;

use crate::s3::backpressure::BackpressureService;

/// Build the `s3s` service (no auth) wrapped in the concurrency backpressure layer.
///
/// Body-size limits are set explicitly because `s3s` does not enforce them for us.
pub fn build_service(gateway: HdfsGateway, config: &Config) -> BackpressureService {
    let mut s3_config = S3Config::default();
    s3_config.xml_max_body_size = 20 * 1024 * 1024; // 20 MiB
    s3_config.post_object_max_file_size = 5 * 1024 * 1024 * 1024; // 5 GiB (unused read-only, but set)
    s3_config.form_max_field_size = 1024 * 1024; // 1 MiB
    s3_config.form_max_fields_size = 20 * 1024 * 1024; // 20 MiB
    s3_config.form_max_parts = 1000;

    // No `set_auth` → no SigV4 verification.
    let mut builder = S3ServiceBuilder::new(gateway);
    builder.set_config(Arc::new(StaticConfigProvider::new(Arc::new(s3_config))));
    let service = builder.build();

    BackpressureService::new(service, config.max_concurrent_requests)
}

/// Serve the gateway on an already-bound `listener`, running the accept loop until the
/// `shutdown` future resolves, then performing a graceful drain of in-flight connections.
///
/// Used by `main` (passing `tokio::signal::ctrl_c()`) and by integration/load tests (passing
/// `std::future::pending()` and aborting the spawned task when done). Taking a bound listener
/// (rather than an `addr`) lets tests bind to port 0 and recover the ephemeral address.
pub async fn serve<F>(
    listener: tokio::net::TcpListener,
    service: BackpressureService,
    shutdown: F,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let http_server = ConnBuilder::new(TokioExecutor::new());
    let graceful = GracefulShutdown::new();

    tokio::pin!(shutdown);

    loop {
        let (socket, _) = tokio::select! {
            res = listener.accept() => match res {
                Ok(conn) => conn,
                Err(err) => {
                    tracing::error!("error accepting connection: {err}");
                    continue;
                }
            },
            _ = &mut shutdown => {
                tracing::info!("shutdown signal received; draining in-flight connections");
                break;
            }
        };

        let conn = http_server.serve_connection(TokioIo::new(socket), service.clone());
        let conn = graceful.watch(conn.into_owned());
        tokio::spawn(async move {
            let _ = conn.await;
        });
    }

    tokio::select! {
        () = graceful.shutdown() => tracing::info!("gracefully shut down"),
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            tracing::info!("waited 10s for graceful shutdown, aborting");
        }
    }

    Ok(())
}
