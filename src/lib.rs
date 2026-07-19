//! HDFS → S3 read-only gateway.
//!
//! This crate is organized into three submodules that mirror the original separation of
//! concerns, kept as modules rather than separate crates because they are always built and
//! released together:
//!
//! - [`config`]: configuration loading/validation (TOML + env + CLI overrides).
//! - [`core`]: pure translation logic — S3 key ↔ HDFS path mapping, HDFS metadata → S3
//!   metadata, listing, and byte-range resolution. Deliberately free of `s3s`/HTTP types so
//!   every decision here is unit-testable in isolation.
//! - [`s3`]: the thin mechanical `impl S3 for HdfsGateway` layer that connects [`core`] to
//!   `s3s`, plus the server wiring and the binary entry point.

pub mod config;
pub mod core;
pub mod s3;
