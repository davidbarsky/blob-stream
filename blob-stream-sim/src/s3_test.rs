use crate::test_support::{
  BUCKET,
  CLIENT_HOST,
  MAX_ATTEMPTS,
  OPERATION_ATTEMPT_TIMEOUT,
  OPERATION_TIMEOUT,
  S3_HOST,
  Simulation,
};
use crate::{S3ErrorCode, S3Fault, S3Op, SimS3};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use blob_stream_blob_store::{BlobKey, BlobStore, BlobStoreError, ByteRange};
use bytes::Bytes;
use std::time::Duration;

const PAYLOAD: &[u8] = b"0123456789abcdef";

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn key() -> BlobKey {
  BlobKey::new("topic/partition-0/blob-0")
}

#[tokio::test(start_paused = true)]
async fn blob_store_round_trips_over_the_s3_protocol() -> TestResult {
  let sim = Simulation::start(&SimS3::new());
  let observed = sim.s3.clone();
  let store = sim.blob_store();
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
}

#[tokio::test(start_paused = true)]
async fn missing_key_is_not_found_for_both_read_paths() -> TestResult {
  let sim = Simulation::start(&SimS3::new());
  let observed = sim.s3.clone();
  let store = sim.blob_store();

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
}

#[tokio::test(start_paused = true)]
async fn cache_admission_sees_the_advertised_length_and_can_reject() -> TestResult {
  let sim = Simulation::start(&SimS3::new());
  let store = sim.blob_store();
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
}

#[tokio::test(start_paused = true)]
async fn truncated_response_body_is_a_read_error_without_retry() -> TestResult {
  let sim = Simulation::start(&SimS3::new());
  let observed = sim.s3.clone();
  let store = sim.blob_store();
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
}

#[tokio::test(start_paused = true)]
async fn transient_server_errors_are_retried_within_the_attempt_budget() -> TestResult {
  let sim = Simulation::start(&SimS3::new());
  let observed = sim.s3.clone();
  let store = sim.blob_store();

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
}

#[tokio::test(start_paused = true)]
async fn refused_connections_exhaust_the_retry_attempts() -> TestResult {
  let sim = Simulation::start(&SimS3::new());
  let observed = sim.s3.clone();
  let (store, attempts) = sim.counted_blob_store();

  // A partition refuses new connections.
  sim.net.partition(CLIENT_HOST, S3_HOST);
  let started = tokio::time::Instant::now();
  let put = store.put(&key(), Bytes::from_static(PAYLOAD)).await;
  let elapsed = started.elapsed();
  sim.net.repair(CLIENT_HOST, S3_HOST);

  // Each refusal is a retryable I/O error, so the attempt limit, not a timeout, ends the call.
  assert!(put.is_err());
  assert_eq!(attempts.take(), MAX_ATTEMPTS);
  assert!(elapsed < OPERATION_TIMEOUT, "{elapsed:?}");
  assert!(observed.calls().is_empty());
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn black_hole_shorter_than_the_operation_timeout_recovers() -> TestResult {
  let sim = Simulation::start(&SimS3::new());
  let observed = sim.s3.clone();
  let (store, attempts) = sim.counted_blob_store();
  let heal_after = OPERATION_ATTEMPT_TIMEOUT + Duration::from_secs(2);

  // A hold buffers every message on the link until release, like a black-holed route.
  sim.net.hold(CLIENT_HOST, S3_HOST);
  let started = tokio::time::Instant::now();
  let net = sim.net.clone();
  let healer = tokio::spawn(async move {
    tokio::time::sleep(heal_after).await;
    net.release(CLIENT_HOST, S3_HOST);
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
}

#[tokio::test(start_paused = true)]
async fn black_hole_longer_than_the_operation_timeout_fails_at_the_budget() -> TestResult {
  let sim = Simulation::start(&SimS3::new());
  let observed = sim.s3.clone();
  let (store, attempts) = sim.counted_blob_store();

  sim.net.hold(CLIENT_HOST, S3_HOST);
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
  // The paused clock fires the operation timeout exactly at its deadline.
  assert_eq!(elapsed, OPERATION_TIMEOUT);
  assert!(observed.calls().is_empty());
  sim.net.release(CLIENT_HOST, S3_HOST);
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn lost_put_response_is_retried_as_an_idempotent_overwrite() -> TestResult {
  let sim = Simulation::start(&SimS3::new());
  let observed = sim.s3.clone();
  let store = sim.blob_store();
  let mut paused = observed.pause_next_response(S3Op::PutObject);

  let put = tokio::spawn(async move { store.put(&key(), Bytes::from_static(PAYLOAD)).await });
  paused.reached().await;
  // The object is durable before the client has any answer.
  assert_eq!(
    observed.object(BUCKET, key().as_str()),
    Some(Bytes::from_static(PAYLOAD))
  );
  sim.net.partition(CLIENT_HOST, S3_HOST);
  paused.release();
  tokio::time::sleep(OPERATION_ATTEMPT_TIMEOUT).await;
  sim.net.repair(CLIENT_HOST, S3_HOST);

  put.await??;
  assert_eq!(observed.call_count(S3Op::PutObject), 2);
  assert_eq!(
    observed.object(BUCKET, key().as_str()),
    Some(Bytes::from_static(PAYLOAD))
  );
  Ok(())
}

#[tokio::test(start_paused = true)]
async fn failed_put_can_still_have_written_the_object() -> TestResult {
  let sim = Simulation::start(&SimS3::new());
  let observed = sim.s3.clone();
  let store = sim.blob_store();
  let mut paused = observed.pause_next_response(S3Op::PutObject);

  let put = tokio::spawn(async move { store.put(&key(), Bytes::from_static(PAYLOAD)).await });
  paused.reached().await;
  sim.net.partition(CLIENT_HOST, S3_HOST);
  paused.release();

  // Callers must treat a failed put as ambiguous: the blob may exist.
  assert!(put.await?.is_err());
  assert_eq!(observed.call_count(S3Op::PutObject), 1);
  assert_eq!(
    observed.object(BUCKET, key().as_str()),
    Some(Bytes::from_static(PAYLOAD))
  );
  Ok(())
}
