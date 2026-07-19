//! Configuration types and parsing for the HDFS→S3 gateway.
//!
//! This crate owns the configuration *surface*: loading from a
//! TOML file and/or environment variables, and validating it at startup so that
//! misconfiguration fails fast rather than at first request.

use std::collections::HashMap;
use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;

/// Command-line arguments for the gateway binary.
#[derive(Debug, Parser)]
#[command(version, about = "HDFS → S3 read-only gateway")]
pub struct CliArgs {
    /// Path to a TOML configuration file.
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Override the listen address (e.g. `0.0.0.0:8080`).
    #[arg(long)]
    pub listen_addr: Option<String>,

    /// Override the NameNode URI (e.g. `hdfs://namenode:8020`).
    #[arg(long)]
    pub namenode_uri: Option<String>,

    /// Override the HDFS root path (e.g. `/data`).
    #[arg(long)]
    pub hdfs_root: Option<String>,

    /// Override the single exposed bucket name (e.g. `hdfs`).
    #[arg(long)]
    pub bucket_name: Option<String>,

    /// Optional shared secret enabling optional S3 SigV4 auth. When set, signed requests
    /// are verified (bad signatures rejected) while unsigned requests are still accepted.
    #[arg(long)]
    pub auth_secret: Option<String>,
}

/// Resolved, validated gateway configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// NameNode URI passed to `hdfs-native`'s client builder (e.g. `hdfs://namenode:8020`).
    pub namenode_uri: String,

    /// S3 keys are relative to this HDFS path (e.g. `/data`).
    pub hdfs_root: String,

    /// The single exposed bucket name (e.g. `hdfs`).
    pub bucket_name: String,

    /// Address the HTTP server binds to. No TLS is terminated here.
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,

    /// Backpressure cap on concurrent in-flight requests.
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,

    /// When true, the text of an upstream `hdfs-native` error is surfaced in the S3 error
    /// `Message` returned to the client. **Default true** — the error is always logged
    /// server-side with the request id regardless; operators correlate client
    /// `x-amz-request-id` to those logs. HDFS error strings can leak NameNode internals
    /// (hosts, paths, exception class names), so set this to false in untrusted deployments.
    #[serde(default)]
    pub expose_upstream_errors: bool,

    /// Free-form HDFS configuration options passed straight through to `hdfs-native`'s
    /// `ClientBuilder::with_config`. Keys are raw Hadoop config keys (e.g.
    /// `dfs.client.use.datanode.hostname`, `dfs.replication`, HA failover settings). These
    /// **override** any value loaded from `core-site.xml`/`hdfs-site.xml`, so they win over
    /// `HADOOP_CONF_DIR`/`HADOOP_HOME`. This is a generic string→string map on purpose: it
    /// requires no code change when `hdfs-native` adds support for new options, so it stays
    /// future-proof. Empty by default (no overrides).
    #[serde(default)]
    pub hdfs_options: HashMap<String, String>,

    /// Optional explicit HDFS config directory passed to `hdfs-native`'s
    /// `ClientBuilder::with_config_dir`. **CAUTION:** when set, this *replaces* the normal
    /// `HADOOP_CONF_DIR` → `HADOOP_HOME/etc/hadoop` env-var fallback entirely — hdfs-native
    /// will only read XML from this directory. Leave `None` (default) to preserve the
    /// standard env-var-driven XML lookup.
    #[serde(default)]
    pub hdfs_config_dir: Option<String>,

    /// Optional effective user for the HDFS client, passed to `hdfs-native`'s
    /// `ClientBuilder::with_user`. When `None` (default), hdfs-native detects the user from
    /// `HADOOP_USER_NAME` / `HADOOP_PROXY_USER` environment variables.
    #[serde(default)]
    pub hdfs_user: Option<String>,

    /// Optional shared secret enabling **optional** S3 SigV4 authentication. When set,
    /// the gateway verifies the signature on *signed* requests (rejecting bad ones) while
    /// still accepting *unsigned* requests — so both client modes work. The gateway never
    /// maps the access key to a user; it only checks that a signed request knew this
    /// secret. When `None` (default), no auth provider is configured and all requests are
    /// accepted unsigned (the original no-auth behavior). Clients then use the normal
    /// default signing flow (any access-key id + this secret) instead of `anonymous` /
    /// `aws_skip_signature` flags. This is NOT user identity — it is a shared password.
    #[serde(default)]
    pub auth_secret: Option<String>,
}

fn default_listen_addr() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_max_concurrent_requests() -> usize {
    2048
}

impl Config {
    /// Load configuration: a TOML file (if provided) merged with CLI overrides, then
    /// validated. Fails fast with a clear error on any missing/malformed value.
    pub fn load(args: &CliArgs) -> Result<Self, ConfigError> {
        let mut config = match &args.config {
            Some(path) => {
                let text = std::fs::read_to_string(path)
                    .map_err(|e| ConfigError::ReadFile(path.clone(), e))?;
                toml::from_str(&text).map_err(ConfigError::ParseToml)?
            }
            None => Config::from_env().unwrap_or_default(),
        };

        if let Some(v) = &args.listen_addr {
            config.listen_addr = v.clone();
        }
        if let Some(v) = &args.namenode_uri {
            config.namenode_uri = v.clone();
        }
        if let Some(v) = &args.hdfs_root {
            config.hdfs_root = v.clone();
        }
        if let Some(v) = &args.bucket_name {
            config.bucket_name = v.clone();
        }
        if let Some(v) = &args.auth_secret {
            config.auth_secret = Some(v.clone());
        }

        config.validate()?;
        Ok(config)
    }

    /// Build a default config from environment variables (`HDFS_NN_URI`, `HDFS_ROOT`,
    /// `BUCKET_NAME`). Used when no config file is supplied.
    fn from_env() -> Option<Self> {
        let namenode_uri = std::env::var("HDFS_NN_URI").ok()?;
        let hdfs_root = std::env::var("HDFS_ROOT").ok()?;
        let bucket_name = std::env::var("BUCKET_NAME").ok()?;
        Some(Config {
            namenode_uri,
            hdfs_root,
            bucket_name,
            listen_addr: default_listen_addr(),
            max_concurrent_requests: default_max_concurrent_requests(),
            expose_upstream_errors: true,
            hdfs_options: HashMap::new(),
            hdfs_config_dir: None,
            hdfs_user: None,
            auth_secret: None,
        })
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.namenode_uri.is_empty() {
            return Err(ConfigError::Missing("namenode_uri"));
        }
        if self.hdfs_root.is_empty() {
            return Err(ConfigError::Missing("hdfs_root"));
        }
        if self.bucket_name.is_empty() {
            return Err(ConfigError::Missing("bucket_name"));
        }
        if self.max_concurrent_requests == 0 {
            return Err(ConfigError::Invalid(
                "max_concurrent_requests must be greater than 0".into(),
            ));
        }
        // Bucket name must be a legal S3 bucket name.
        if !s3s::path::check_bucket_name(&self.bucket_name) {
            return Err(ConfigError::Invalid(format!(
                "bucket_name {:?} is not a valid S3 bucket name",
                self.bucket_name
            )));
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            namenode_uri: "hdfs://localhost:8020".into(),
            hdfs_root: "/".into(),
            bucket_name: "hdfs".into(),
            listen_addr: default_listen_addr(),
            max_concurrent_requests: default_max_concurrent_requests(),
            expose_upstream_errors: true,
            hdfs_options: HashMap::new(),
            hdfs_config_dir: None,
            hdfs_user: None,
            auth_secret: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {0}: {1}")]
    ReadFile(PathBuf, std::io::Error),

    #[error("failed to parse TOML config: {0}")]
    ParseToml(#[from] toml::de::Error),

    #[error("missing required config field: {0}")]
    Missing(&'static str),

    #[error("invalid config value: {0}")]
    Invalid(String),
}
