//! Optional S3 authentication for the gateway.
//!
//! The gateway's default posture is **no authentication** (see the top-level README
//! security section): when `auth_secret` is unset, no auth provider is configured, so
//! `s3s` accepts BOTH anonymous and any signed request.
//!
//! When `auth_secret` IS configured, we enable `s3s` signature verification and rely on
//! `s3s`'s default access check, which **requires** a valid signature:
//!
//! - **Unsigned** requests are rejected ("Signature is required").
//! - **Signed** requests are verified against the shared secret. A valid signature is
//!   accepted; a bad signature is rejected with `403 SignatureDoesNotMatch`. We never map
//!   the access key to a user — the gateway only cares that the client knows the secret.
//! - This lets clients use the *normal* default SigV4 signing flow (any access-key id +
//!   this secret) instead of `anonymous` / `aws_skip_signature` flags.

use s3s::auth::{S3Auth, SecretKey};
use s3s::S3Result;

/// Auth provider that returns the **same** shared secret for *any* access key.
///
/// This verifies that the request's SigV4 signature is cryptographically valid (proving
/// the client knows the shared secret) without ever identifying or distinguishing users.
#[derive(Clone)]
pub struct SharedSecretAuth {
    secret: SecretKey,
}

impl SharedSecretAuth {
    pub fn new(secret: impl Into<SecretKey>) -> Self {
        Self {
            secret: secret.into(),
        }
    }
}

#[async_trait::async_trait]
impl S3Auth for SharedSecretAuth {
    async fn get_secret_key(&self, _access_key: &str) -> S3Result<SecretKey> {
        // Same secret for everyone — we accept any access key id, as long as the
        // signature was produced with our shared secret.
        Ok(self.secret.clone())
    }
}
