//! Simulation setup shared by the scenario and property tests.

use crate::{SimNet, SimS3, sim_s3_client};
use aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::error::BoxError;
use blob_stream_blob_store::S3BlobStore;
use blob_stream_metadata_store::{aws_retry_config, aws_timeout_config};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

pub const SEED: u64 = 0x5eed_b10b;
pub const S3_HOST: &str = "s3";
pub const S3_PORT: u16 = 9000;
pub const CLIENT_HOST: &str = "broker";
pub const BUCKET: &str = "blobs";
/// One-way network latency between the client and S3.
pub const LATENCY: Duration = Duration::from_millis(1);

// Production policy from `blob_stream_metadata_store::aws`.
pub const OPERATION_TIMEOUT: Duration = Duration::from_secs(15);
pub const OPERATION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
pub const MAX_ATTEMPTS: usize = 4;

/// A network with a running S3 host that already has `BUCKET`.
pub struct Simulation {
  pub net: SimNet,
  pub s3: SimS3,
}

impl Simulation {
  /// Start the S3 host. Must be called inside a runtime started with `start_paused(true)`.
  pub fn start(s3: &SimS3) -> Self {
    // The SDK draws retry jitter from fastrand's thread-local generator, and the whole simulation
    // runs on this thread, so seeding here makes the retry schedule part of the seeded run.
    fastrand::seed(SEED);
    let net = SimNet::new();
    net.set_latency(LATENCY);
    s3.create_bucket(BUCKET);
    tokio::spawn(s3.clone().serve(net.bind(S3_HOST, S3_PORT)));
    Self {
      net,
      s3: s3.clone(),
    }
  }

  /// The production blob store over the simulated network with the production AWS policy.
  pub fn blob_store(&self) -> S3BlobStore {
    self.counted_blob_store().0
  }

  pub fn counted_blob_store(&self) -> (S3BlobStore, AttemptCounter) {
    let attempts = AttemptCounter::default();
    let client = sim_s3_client(
      &self.net,
      CLIENT_HOST,
      S3_HOST,
      S3_PORT,
      aws_retry_config(),
      aws_timeout_config(),
    );
    let config = client
      .config()
      .to_builder()
      .interceptor(attempts.clone())
      .build();
    (
      S3BlobStore::new(aws_sdk_s3::Client::from_conf(config), BUCKET),
      attempts,
    )
  }
}

/// Counts SDK transmit attempts, including attempts that never reach the server.
#[derive(Clone, Debug, Default)]
pub struct AttemptCounter(Arc<AtomicUsize>);

impl AttemptCounter {
  pub fn take(&self) -> usize {
    self.0.swap(0, Ordering::SeqCst)
  }
}

impl Intercept for AttemptCounter {
  fn name(&self) -> &'static str {
    "AttemptCounter"
  }

  fn read_before_attempt(
    &self,
    _context: &BeforeTransmitInterceptorContextRef<'_>,
    _runtime_components: &RuntimeComponents,
    _cfg: &mut ConfigBag,
  ) -> Result<(), BoxError> {
    self.0.fetch_add(1, Ordering::SeqCst);
    Ok(())
  }
}
