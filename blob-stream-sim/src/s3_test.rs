use crate::{S3ErrorCode, S3Fault, S3Op, SimS3, sim_s3_client};
use aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_s3::error::{BoxError, SdkError};
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use blob_stream_blob_store::{BlobKey, BlobStore, BlobStoreError, ByteRange, S3BlobStore};
use blob_stream_metadata_store::{aws_retry_config, aws_timeout_config};
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const SEED: u64 = 0x5eed_b10b;
const S3_HOST: &str = "s3";
const S3_PORT: u16 = 9000;
const CLIENT_HOST: &str = "broker";
const BUCKET: &str = "blobs";
const PAYLOAD: &[u8] = b"0123456789abcdef";

// Production policy from `blob_stream_metadata_store::aws`.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(15);
const OPERATION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_ATTEMPTS: usize = 4;

/// Build a seeded simulation with a running S3 host that already has `BUCKET`.
fn simulation<'a>(s3: &SimS3) -> turmoil::Sim<'a> {
  // The SDK draws retry jitter from fastrand's thread-local generator, and turmoil drives every
  // host on this thread, so seeding here makes the retry schedule part of the seeded run.
  fastrand::seed(SEED);
  let mut sim = turmoil::Builder::new()
    .rng_seed(SEED)
    .simulation_duration(Duration::from_secs(120))
    .build();
  s3.create_bucket(BUCKET);
  let server = s3.clone();
  sim.host(S3_HOST, move || server.clone().serve(S3_PORT));
  sim
}

/// Counts SDK transmit attempts, including attempts that never reach the server.
#[derive(Clone, Debug, Default)]
struct AttemptCounter(Arc<AtomicUsize>);

impl AttemptCounter {
  fn take(&self) -> usize {
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

/// The production blob store over the simulated network with the production AWS policy.
fn blob_store() -> S3BlobStore {
  counted_blob_store().0
}

fn counted_blob_store() -> (S3BlobStore, AttemptCounter) {
  let attempts = AttemptCounter::default();
  let client = sim_s3_client(S3_HOST, S3_PORT, aws_retry_config(), aws_timeout_config());
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

fn key() -> BlobKey {
  BlobKey::new("topic/partition-0/blob-0")
}

#[test]
fn blob_store_round_trips_over_the_s3_protocol() -> turmoil::Result {
  let s3 = SimS3::new();
  let mut sim = simulation(&s3);
  let observed = s3.clone();
  sim.client(CLIENT_HOST, async move {
    let store = blob_store();
    store.put(&key(), Bytes::from_static(PAYLOAD)).await?;

    let range = store
      .get_range(&key(), ByteRange { start: 2, end: 6 })
      .await?;
    assert_eq!(range, Bytes::from_static(b"2345"));
    let empty = store
      .get_range(&key(), ByteRange { start: 6, end: 6 })
      .await?;
    assert!(empty.is_empty());
    let whole = store
      .get_with_cache_admission(&key(), &|length| length == PAYLOAD.len() as u64)
      .await?;
    assert_eq!(whole, Bytes::from_static(PAYLOAD));

    assert_eq!(
      observed.object(BUCKET, key().as_str()),
      Some(Bytes::from_static(PAYLOAD))
    );
    let ranges: Vec<_> = observed
      .calls()
      .into_iter()
      .filter(|call| call.op == S3Op::GetObject)
      .map(|call| call.range)
      .collect();
    // The empty range is answered locally; the full read sends no Range header.
    assert_eq!(ranges, [Some("bytes=2-5".to_string()), None]);
    assert_eq!(observed.call_count(S3Op::PutObject), 1);
    Ok(())
  });
  sim.run()
}

#[test]
fn missing_key_is_not_found_for_both_read_paths() -> turmoil::Result {
  let s3 = SimS3::new();
  let mut sim = simulation(&s3);
  let observed = s3.clone();
  sim.client(CLIENT_HOST, async move {
    let store = blob_store();

    let range = store
      .get_range(&key(), ByteRange { start: 0, end: 1 })
      .await;
    assert!(
      matches!(range, Err(BlobStoreError::NotFound { .. })),
      "{range:?}"
    );
    let whole = store.get_with_cache_admission(&key(), &|_| true).await;
    assert!(
      matches!(whole, Err(BlobStoreError::NotFound { .. })),
      "{whole:?}"
    );
    // NoSuchKey is a modeled, non-retryable error, so each read makes exactly one request.
    assert_eq!(observed.call_count(S3Op::GetObject), 2);
    Ok(())
  });
  sim.run()
}

#[test]
fn cache_admission_sees_the_advertised_length_and_can_reject() -> turmoil::Result {
  let s3 = SimS3::new();
  let mut sim = simulation(&s3);
  sim.client(CLIENT_HOST, async move {
    let store = blob_store();
    store.put(&key(), Bytes::from_static(PAYLOAD)).await?;

    let offered = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
    let recorded = offered.clone();
    let result = store
      .get_with_cache_admission(&key(), &move |length| {
        recorded.lock().push(length);
        false
      })
      .await;
    assert!(
      matches!(result, Err(BlobStoreError::AdmissionRejected { .. })),
      "{result:?}"
    );
    assert_eq!(*offered.lock(), [PAYLOAD.len() as u64]);
    Ok(())
  });
  sim.run()
}

#[test]
fn truncated_response_body_is_a_read_error_without_retry() -> turmoil::Result {
  let s3 = SimS3::new();
  let mut sim = simulation(&s3);
  let observed = s3.clone();
  sim.client(CLIENT_HOST, async move {
    let store = blob_store();
    store.put(&key(), Bytes::from_static(PAYLOAD)).await?;

    observed.inject(S3Op::GetObject, S3Fault::TruncateBody { after: 3 });
    let whole = store.get_with_cache_admission(&key(), &|_| true).await;
    assert!(
      matches!(whole, Err(BlobStoreError::Read { .. })),
      "{whole:?}"
    );

    observed.inject(S3Op::GetObject, S3Fault::TruncateBody { after: 3 });
    let range = store
      .get_range(&key(), ByteRange { start: 0, end: 8 })
      .await;
    assert!(
      matches!(range, Err(BlobStoreError::Read { .. })),
      "{range:?}"
    );

    // Body errors surface after the SDK has returned the response, so they are never retried.
    assert_eq!(observed.call_count(S3Op::GetObject), 2);
    Ok(())
  });
  sim.run()
}

#[test]
fn transient_server_errors_are_retried_within_the_attempt_budget() -> turmoil::Result {
  let s3 = SimS3::new();
  let mut sim = simulation(&s3);
  let observed = s3.clone();
  sim.client(CLIENT_HOST, async move {
    let store = blob_store();

    observed.inject(S3Op::PutObject, S3Fault::Error(S3ErrorCode::SlowDown));
    observed.inject(S3Op::PutObject, S3Fault::Error(S3ErrorCode::InternalError));
    store.put(&key(), Bytes::from_static(PAYLOAD)).await?;
    assert_eq!(observed.call_count(S3Op::PutObject), 3);

    let other = BlobKey::new("topic/partition-0/blob-1");
    for _ in 0 .. MAX_ATTEMPTS {
      observed.inject(S3Op::PutObject, S3Fault::Error(S3ErrorCode::InternalError));
    }
    let exhausted = store.put(&other, Bytes::from_static(PAYLOAD)).await;
    assert!(exhausted.is_err());
    assert_eq!(observed.call_count(S3Op::PutObject), 3 + MAX_ATTEMPTS);
    assert_eq!(observed.object(BUCKET, other.as_str()), None);
    Ok(())
  });
  sim.run()
}

#[test]
fn refused_connections_exhaust_the_retry_attempts() -> turmoil::Result {
  let s3 = SimS3::new();
  let mut sim = simulation(&s3);
  let observed = s3.clone();
  sim.client(CLIENT_HOST, async move {
    let (store, attempts) = counted_blob_store();

    // A turmoil partition drops the SYN, which the client observes as a refused connection.
    turmoil::partition(CLIENT_HOST, S3_HOST);
    let started = tokio::time::Instant::now();
    let put = store.put(&key(), Bytes::from_static(PAYLOAD)).await;
    let elapsed = started.elapsed();
    turmoil::repair(CLIENT_HOST, S3_HOST);

    // Each refusal is a retryable I/O error, so the attempt limit, not a timeout, ends the call.
    assert!(put.is_err());
    assert_eq!(attempts.take(), MAX_ATTEMPTS);
    assert!(elapsed < OPERATION_TIMEOUT, "{elapsed:?}");
    assert!(observed.calls().is_empty());
    Ok(())
  });
  sim.run()
}

#[test]
fn black_hole_shorter_than_the_operation_timeout_recovers() -> turmoil::Result {
  let s3 = SimS3::new();
  let mut sim = simulation(&s3);
  let observed = s3.clone();
  sim.client(CLIENT_HOST, async move {
    let (store, attempts) = counted_blob_store();
    let heal_after = OPERATION_ATTEMPT_TIMEOUT + Duration::from_secs(2);

    // A hold buffers every message on the link until release, like a black-holed route.
    turmoil::hold(CLIENT_HOST, S3_HOST);
    let started = tokio::time::Instant::now();
    let healer = tokio::spawn(async move {
      tokio::time::sleep(heal_after).await;
      turmoil::release(CLIENT_HOST, S3_HOST);
    });
    let put = store.put(&key(), Bytes::from_static(PAYLOAD)).await;
    let elapsed = started.elapsed();
    healer.await?;

    put?;
    // Connect attempts into the black hole time out and are retried; only the attempt in flight
    // at release reaches the server.
    assert!(attempts.take() > 1);
    assert!(elapsed >= heal_after, "{elapsed:?}");
    assert!(elapsed < OPERATION_TIMEOUT, "{elapsed:?}");
    assert_eq!(observed.call_count(S3Op::PutObject), 1);
    assert_eq!(
      observed.object(BUCKET, key().as_str()),
      Some(Bytes::from_static(PAYLOAD))
    );
    Ok(())
  });
  sim.run()
}

#[test]
fn black_hole_longer_than_the_operation_timeout_fails_at_the_budget() -> turmoil::Result {
  let s3 = SimS3::new();
  let mut sim = simulation(&s3);
  let observed = s3.clone();
  sim.client(CLIENT_HOST, async move {
    let (store, attempts) = counted_blob_store();

    turmoil::hold(CLIENT_HOST, S3_HOST);
    let started = tokio::time::Instant::now();
    let put = store.put(&key(), Bytes::from_static(PAYLOAD)).await;
    let elapsed = started.elapsed();

    let error = put.expect_err("put into a black hole must fail");
    assert!(
      matches!(
        error.downcast_ref::<SdkError<PutObjectError, HttpResponse>>(),
        Some(SdkError::TimeoutError(_))
      ),
      "{error:?}"
    );
    assert!(attempts.take() > 1);
    // Turmoil advances time in 1ms ticks, so allow one tick of overshoot.
    assert!(elapsed >= OPERATION_TIMEOUT, "{elapsed:?}");
    assert!(
      elapsed <= OPERATION_TIMEOUT + Duration::from_millis(1),
      "{elapsed:?}"
    );
    assert!(observed.calls().is_empty());
    turmoil::release(CLIENT_HOST, S3_HOST);
    Ok(())
  });
  sim.run()
}

#[test]
fn lost_put_response_is_retried_as_an_idempotent_overwrite() -> turmoil::Result {
  let s3 = SimS3::new();
  let mut sim = simulation(&s3);
  let observed = s3.clone();
  sim.client(CLIENT_HOST, async move {
    let store = blob_store();
    let mut paused = observed.pause_next_response(S3Op::PutObject);

    let put = tokio::spawn(async move { store.put(&key(), Bytes::from_static(PAYLOAD)).await });
    paused.reached().await;
    // The object is durable before the client has any answer.
    assert_eq!(
      observed.object(BUCKET, key().as_str()),
      Some(Bytes::from_static(PAYLOAD))
    );
    turmoil::partition(CLIENT_HOST, S3_HOST);
    paused.release();
    tokio::time::sleep(OPERATION_ATTEMPT_TIMEOUT).await;
    turmoil::repair(CLIENT_HOST, S3_HOST);

    put.await??;
    assert_eq!(observed.call_count(S3Op::PutObject), 2);
    assert_eq!(
      observed.object(BUCKET, key().as_str()),
      Some(Bytes::from_static(PAYLOAD))
    );
    Ok(())
  });
  sim.run()
}

#[test]
fn failed_put_can_still_have_written_the_object() -> turmoil::Result {
  let s3 = SimS3::new();
  let mut sim = simulation(&s3);
  let observed = s3.clone();
  sim.client(CLIENT_HOST, async move {
    let store = blob_store();
    let mut paused = observed.pause_next_response(S3Op::PutObject);

    let put = tokio::spawn(async move { store.put(&key(), Bytes::from_static(PAYLOAD)).await });
    paused.reached().await;
    turmoil::partition(CLIENT_HOST, S3_HOST);
    paused.release();

    // Callers must treat a failed put as ambiguous: the blob may exist.
    assert!(put.await?.is_err());
    assert_eq!(observed.call_count(S3Op::PutObject), 1);
    assert_eq!(
      observed.object(BUCKET, key().as_str()),
      Some(Bytes::from_static(PAYLOAD))
    );
    Ok(())
  });
  sim.run()
}
