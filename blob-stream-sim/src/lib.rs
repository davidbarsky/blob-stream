//! Deterministic simulation harness for Blob Stream's storage dependencies.
//!
//! Tests run on one current-thread tokio runtime started with `start_paused(true)`. Tokio then
//! advances its clock straight to the next timer whenever no task can run, so retries, timeouts,
//! and outages lasting simulated minutes finish in milliseconds of wall time. Hosts talk over a
//! [`SimNet`] that tests can partition or hold. The harness serves an in-memory S3 over the real S3
//! HTTP protocol with [`s3s`] and points an unmodified `aws_sdk_s3::Client` at it through
//! [`SimHttpClient`], so production code such as `S3BlobStore` runs its real request, retry, and
//! timeout paths against simulated faults.

#[cfg(test)]
#[path = "./s3_property_test.rs"]
mod s3_property_tests;
#[cfg(test)]
mod test_support;

mod http;
mod net;
mod s3;

use aws_sdk_s3::config::retry::{RetryConfig, RetryPartition};
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_smithy_async::time::TimeSource;
pub use http::SimHttpClient;
pub use net::{SimListener, SimNet, SimStream};
pub use s3::{
  PausedResponse,
  S3Call,
  S3Fault,
  S3FaultSource,
  S3Op,
  SIM_S3_ACCESS_KEY,
  SIM_S3_SECRET_KEY,
  SimS3,
};
pub use s3s::S3ErrorCode;
use std::time::SystemTime;

//
// TokioTimeSource
//

/// Wall-clock time that advances with tokio's clock.
///
/// The SDK reads a time source for request signing and for measuring download throughput in its
/// stalled-stream protection, while it sleeps on tokio timers. Its default source is the system
/// clock, which does not move when a paused runtime skips ahead, so the throughput check would
/// compare simulated sleeps against real elapsed time. This source starts at the real time when
/// it is created, which keeps signatures inside the server's clock-skew window, and then moves
/// only with tokio's clock.
#[derive(Clone, Debug)]
pub struct TokioTimeSource {
  base: SystemTime,
  start: tokio::time::Instant,
}

impl TokioTimeSource {
  /// Must be called inside a tokio runtime.
  #[must_use]
  pub fn new() -> Self {
    Self {
      base: SystemTime::now(),
      start: tokio::time::Instant::now(),
    }
  }
}

impl Default for TokioTimeSource {
  fn default() -> Self {
    Self::new()
  }
}

impl TimeSource for TokioTimeSource {
  fn now(&self) -> SystemTime {
    self.base + self.start.elapsed()
  }
}

/// Build an S3 client for `http://{host}:{port}` whose requests originate from `from` on `net`.
///
/// Callers pass the retry and timeout policy under test, normally the production policy. The SDK's
/// default tokio sleep implementation drives retries and timeouts, and [`TokioTimeSource`] drives
/// its clock reads, so both follow the paused tokio clock. Must be called inside a tokio runtime.
///
/// Each client gets its own retry token bucket. The SDK otherwise shares one bucket per partition
/// name across the whole process, which would carry retry budget from one simulation into the next.
#[must_use]
pub fn sim_s3_client(
  net: &SimNet,
  from: &str,
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
    .http_client(SimHttpClient::new(net.clone(), from))
    .time_source(TokioTimeSource::new())
    .retry_config(retry)
    .retry_partition(RetryPartition::custom("blob-stream-sim").build())
    .timeout_config(timeout)
    .build();
  aws_sdk_s3::Client::from_conf(config)
}
