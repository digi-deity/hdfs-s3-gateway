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

    /// Force-disable the metadata, block-map, directory-listing and negative caches for
    /// this process. Incident kill switch: takes precedence over `[cache] enabled` and
    /// `HDFS_S3_CACHE_ENABLED`, so a suspected cache bug can be mitigated by changing
    /// only the launch arguments (no new release, no config edit).
    #[arg(long, conflicts_with = "cache")]
    pub no_cache: bool,

    /// Force-enable the caches for this process, overriding `[cache] enabled` and
    /// `HDFS_S3_CACHE_ENABLED`.
    #[arg(long, conflicts_with = "no_cache")]
    pub cache: bool,
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

    /// Bounded TTL caches for metadata, block maps, directory listings and negative
    /// lookups. Defaults to safe, short TTLs (see [`CacheConfig`]); setting `[cache]
    /// enabled` to false disables them.
    #[serde(default)]
    pub cache: CacheConfig,

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

/// Bounded TTL caches for NameNode metadata, block maps, directory listings and negative
/// (known-missing) lookups.
///
/// The gateway is read-only: only writers OUTSIDE the gateway can change HDFS, and there
/// is no invalidation channel from the NameNode. Freshness is therefore bounded by these
/// TTLs (after which the next request re-reads), and memory by the weight budgets.
///
/// **Disabled by default**: enabling them trades read-after-write visibility of an
/// external writer for fewer NameNode round trips (within the TTL, a just-written object
/// or listing may not be visible). That is the right trade for read-heavy analytics
/// workloads and the wrong one when clients expect immediate visibility, so it is an
/// explicit deployment decision.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    /// Master switch (default: disabled — see the type docs). When false every lookup
    /// misses and nothing is cached.
    pub enabled: bool,
    /// `getFileInfo` results (length/mtime/isdir) reused for GET and HEAD.
    pub metadata_ttl_ms: u64,
    /// Cached `FileReader`s (the object's block map) reused for ranged GETs.
    pub blocks_ttl_ms: u64,
    /// Non-recursive directory listings reused by both listing strategies.
    pub listing_ttl_ms: u64,
    /// Missing paths (S3 probes `_SUCCESS`/`_common_metadata` constantly).
    pub negative_ttl_ms: u64,
    /// Maximum number of cached file metadata entries.
    pub metadata_capacity: usize,
    /// Maximum number of cached block maps (`FileReader`s).
    pub blocks_capacity: usize,
    /// Total weight budget for cached directory listings, in entries.
    pub listing_capacity_entries: usize,
    /// Directories larger than this are listed but never cached (scan resistance).
    pub listing_max_dir_entries: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig {
            enabled: false,
            metadata_ttl_ms: 1_000,
            blocks_ttl_ms: 10_000,
            listing_ttl_ms: 5_000,
            negative_ttl_ms: 1_000,
            metadata_capacity: 100_000,
            blocks_capacity: 512,
            listing_capacity_entries: 200_000,
            listing_max_dir_entries: 50_000,
        }
    }
}

/// Parse a boolean environment variable, failing fast on values that are neither clearly
/// true nor clearly false (a mistyped kill switch must not silently keep caches on).
fn env_bool(name: &str) -> Result<Option<bool>, ConfigError> {
    let Some(raw) = std::env::var_os(name) else {
        return Ok(None);
    };
    let value = raw.to_string_lossy().trim().to_ascii_lowercase();
    match value.as_str() {
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        _ => Err(ConfigError::Invalid(format!(
            "{name} must be one of 1/0, true/false, yes/no, on/off (got {value:?})"
        ))),
    }
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

        // Cache kill switch, lowest to highest precedence:
        //   config file `[cache] enabled`  <  HDFS_S3_CACHE_ENABLED  <  --cache/--no-cache
        // The env var exists so an operator can flip it without editing the config file
        // (e.g. via a container env change), and the flags so it can be done without even
        // that — no new release required if a cache bug is suspected.
        if let Some(enabled) = env_bool("HDFS_S3_CACHE_ENABLED")? {
            config.cache.enabled = enabled;
        }
        if args.no_cache {
            config.cache.enabled = false;
        }
        if args.cache {
            config.cache.enabled = true;
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
            cache: CacheConfig::default(),
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
        // The listing cache weight budget must be able to hold at least one entry,
        // otherwise enabling the cache would silently do nothing. (A zero capacity is
        // harmless while caching is disabled, e.g. left over in a shared config.)
        if self.cache.enabled
            && self.cache.listing_ttl_ms > 0
            && self.cache.listing_capacity_entries == 0
        {
            return Err(ConfigError::Invalid(
                "cache.listing_capacity_entries must be greater than 0".into(),
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
            cache: CacheConfig::default(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn args(config: Option<PathBuf>) -> CliArgs {
        CliArgs {
            config,
            listen_addr: None,
            namenode_uri: None,
            hdfs_root: None,
            bucket_name: None,
            auth_secret: None,
            no_cache: false,
            cache: false,
        }
    }

    fn write_config(cache_enabled: bool) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "gateway-cache-toggle-{}-{}.toml",
            std::process::id(),
            cache_enabled
        ));
        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(
            file,
            "namenode_uri = \"hdfs://localhost:8020\"\n\
             hdfs_root = \"/data\"\n\
             bucket_name = \"hdfs\"\n\
             [cache]\n\
             enabled = {cache_enabled}"
        )
        .unwrap();
        path
    }

    #[test]
    fn cache_kill_switch_precedence() {
        // file says ON; env says OFF; CLI --cache says ON again.
        let path = write_config(true);

        std::env::remove_var("HDFS_S3_CACHE_ENABLED");
        assert!(
            Config::load(&args(Some(path.clone())))
                .unwrap()
                .cache
                .enabled
        );

        std::env::set_var("HDFS_S3_CACHE_ENABLED", "0");
        assert!(
            !Config::load(&args(Some(path.clone())))
                .unwrap()
                .cache
                .enabled,
            "env must override the config file"
        );

        let mut with_cache = args(Some(path.clone()));
        with_cache.cache = true;
        assert!(
            Config::load(&with_cache).unwrap().cache.enabled,
            "--cache must override the env"
        );

        std::env::remove_var("HDFS_S3_CACHE_ENABLED");
        let mut no_cache = args(Some(path.clone()));
        no_cache.no_cache = true;
        assert!(
            !Config::load(&no_cache).unwrap().cache.enabled,
            "--no-cache must override an enabled config file"
        );

        std::env::set_var("HDFS_S3_CACHE_ENABLED", "definitely-not-a-bool");
        assert!(
            Config::load(&args(Some(path.clone()))).is_err(),
            "a mistyped kill switch must fail fast, not keep caches enabled"
        );
        std::env::remove_var("HDFS_S3_CACHE_ENABLED");
        let _ = std::fs::remove_file(path);
    }
}
