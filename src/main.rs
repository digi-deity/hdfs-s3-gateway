//! Binary entry point: wires config → HDFS client → `HdfsGateway` → `s3s` service →
//! hyper server. Auth is OPTIONAL: when `auth_secret` is set, signed requests are
//! verified (bad signatures rejected) while unsigned requests are still accepted; when
//! unset, `s3s` defaults to no auth and all requests are accepted. Either way the service
//! runs over plaintext HTTP and MUST sit behind network-level access control and (for any
//! real confidentiality) a TLS-terminating reverse proxy (see README / ops docs).

use std::net::SocketAddr;

use clap::Parser;
use hdfs_s3_gateway::config::{CliArgs, Config};
use hdfs_s3_gateway::s3::{server, HdfsGateway};

use tracing::info;

fn setup_tracing() {
    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing();

    let args = CliArgs::parse();
    let config = Config::load(&args)?;
    info!(?config, "loaded configuration");

    // Build the shared HDFS client + gateway (shared with the Python bindings via
    // `HdfsGateway::from_config`).
    let gateway = HdfsGateway::from_config(&config)?;
    info!(namenode = %config.namenode_uri, "connected to HDFS NameNode");

    // Build the S3 service (optional auth) wrapped in the concurrency backpressure layer.
    let service = server::build_service(gateway, &config);

    let addr: SocketAddr = config.listen_addr.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    match &config.auth_secret {
        Some(_) => {
            info!(%addr, "gateway listening (optional SigV4 auth — signed verified, unsigned accepted; must be behind TLS + network access control)")
        }
        None => info!(%addr, "gateway listening (NO AUTH — must be behind network access control)"),
    }

    // Serve until SIGTERM/SIGINT, then gracefully drain in-flight connections.
    server::serve(listener, service, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;

    Ok(())
}
