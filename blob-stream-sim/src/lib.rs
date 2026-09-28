//! Deterministic simulation harness for Blob Stream's storage dependencies.
//!
//! Tests run inside a [`turmoil`] simulation: every host is a single-threaded tokio runtime with a
//! simulated clock and a simulated network that tests can partition, hold, or crash. The harness
//! serves an in-memory S3 over the real S3 HTTP protocol with [`s3s`] and points an unmodified
//! `aws_sdk_s3::Client` at it through [`TurmoilHttpClient`], so production code such as
//! `S3BlobStore` runs its real request, retry, and timeout paths against simulated faults.

mod http;
mod s3;

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
pub use http::TurmoilHttpClient;
pub use s3::{PausedResponse, S3Call, S3Fault, S3Op, SIM_S3_ACCESS_KEY, SIM_S3_SECRET_KEY, SimS3};
pub use s3s::S3ErrorCode;

/// Build an S3 client for `http://{host}:{port}` that sends every request over turmoil.
///
/// Callers pass the retry and timeout policy under test, normally the production policy. The SDK's
/// default tokio sleep implementation drives retries and timeouts, so both follow simulated time.
#[must_use]
pub fn sim_s3_client(
  host: &str,
  port: u16,
  retry: RetryConfig,
  timeout: TimeoutConfig,
) -> aws_sdk_s3::Client {
  let config = aws_sdk_s3::Config::builder()
    .behavior_version(BehaviorVersion::latest())
    .region(Region::new("us-east-1"))
    .credentials_provider(Credentials::new(
      SIM_S3_ACCESS_KEY,
      SIM_S3_SECRET_KEY,
      None,
      None,
      "blob-stream-sim",
    ))
    .endpoint_url(format!("http://{host}:{port}"))
    .force_path_style(true)
    .http_client(TurmoilHttpClient)
    .retry_config(retry)
    .timeout_config(timeout)
    .build();
  aws_sdk_s3::Client::from_conf(config)
}
