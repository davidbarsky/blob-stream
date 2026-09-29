use crate::test_support::{
  BUCKET,
  CLIENT_HOST,
  MAX_ATTEMPTS,
  OPERATION_ATTEMPT_TIMEOUT,
  OPERATION_TIMEOUT,
  S3_HOST,
  TestResult,
  simulate,
};
use crate::{S3ErrorCode, S3Fault, S3Op, SimS3};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use blob_stream_blob_store::{BlobKey, BlobStore, BlobStoreError, ByteRange};
use bytes::Bytes;
use std::time::Duration;

const PAYLOAD: &[u8] = b"0123456789abcdef";

fn key() -> BlobKey {
  BlobKey::new("topic/partition-0/blob-0")
}

#[test]
fn blob_store_round_trips_over_the_s3_protocol() -> TestResult {
  simulate(SimS3::new(), |sim| async move {
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
  })
}

#[test]
fn missing_key_is_not_found_for_both_read_paths() -> TestResult {
  simulate(SimS3::new(), |sim| async move {
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
  })
}

#[test]
fn cache_admission_sees_the_advertised_length_and_can_reject() -> TestResult {
  simulate(SimS3::new(), |sim| async move {
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
  })
}

#[test]
fn truncated_response_body_is_a_read_error_without_retry() -> TestResult {
  simulate(SimS3::new(), |sim| async move {
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
  })
}

#[test]
fn transient_server_errors_are_retried_within_the_attempt_budget() -> TestResult {
  simulate(SimS3::new(), |sim| async move {
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
  })
}

#[test]
fn crashed_s3_refuses_connections_until_the_retry_attempts_run_out() -> TestResult {
  simulate(SimS3::new(), |sim| async move {
    let observed = sim.s3.clone();
    let (store, attempts) = sim.counted_blob_store();

    // With the S3 process gone, its host answers every connection attempt with a reset.
    sim.control.crash(S3_HOST);
    let started = tokio::time::Instant::now();
    let put = store.put(&key(), Bytes::from_static(PAYLOAD)).await;
    let elapsed = started.elapsed();

    // Each refusal is a retryable I/O error, so the attempt limit, not a timeout, ends the call.
    assert!(put.is_err());
    assert_eq!(attempts.take(), MAX_ATTEMPTS);
    assert!(elapsed < OPERATION_TIMEOUT, "{elapsed:?}");
    assert!(observed.calls().is_empty());
    Ok(())
  })
}

#[test]
fn black_hole_shorter_than_the_operation_timeout_recovers() -> TestResult {
  simulate(SimS3::new(), |sim| async move {
    let observed = sim.s3.clone();
    let (store, attempts) = sim.counted_blob_store();
    let heal_after = OPERATION_ATTEMPT_TIMEOUT + Duration::from_secs(2);

    // A hold buffers every message on the link until release, like a black-holed route.
    sim.control.hold(CLIENT_HOST, S3_HOST);
    let started = tokio::time::Instant::now();
    let control = sim.control.clone();
    let healer = tokio::spawn(async move {
      tokio::time::sleep(heal_after).await;
      control.release(CLIENT_HOST, S3_HOST);
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
  })
}

#[test]
fn black_hole_longer_than_the_operation_timeout_fails_at_the_budget() -> TestResult {
  simulate(SimS3::new(), |sim| async move {
    let observed = sim.s3.clone();
    let (store, attempts) = sim.counted_blob_store();

    sim.control.hold(CLIENT_HOST, S3_HOST);
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
    sim.control.release(CLIENT_HOST, S3_HOST);
    Ok(())
  })
}

#[test]
fn lost_put_response_is_retried_as_an_idempotent_overwrite() -> TestResult {
  simulate(SimS3::new(), |sim| async move {
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
    sim.control.partition(CLIENT_HOST, S3_HOST);
    paused.release();
    tokio::time::sleep(OPERATION_ATTEMPT_TIMEOUT).await;
    sim.control.repair(CLIENT_HOST, S3_HOST);

    put.await??;
    assert_eq!(observed.call_count(S3Op::PutObject), 2);
    assert_eq!(
      observed.object(BUCKET, key().as_str()),
      Some(Bytes::from_static(PAYLOAD))
    );
    Ok(())
  })
}

#[test]
fn failed_put_can_still_have_written_the_object() -> TestResult {
  simulate(SimS3::new(), |sim| async move {
    let observed = sim.s3.clone();
    let store = sim.blob_store();
    let mut paused = observed.pause_next_response(S3Op::PutObject);

    let put = tokio::spawn(async move { store.put(&key(), Bytes::from_static(PAYLOAD)).await });
    paused.reached().await;
    sim.control.partition(CLIENT_HOST, S3_HOST);
    paused.release();

    // Callers must treat a failed put as ambiguous: the blob may exist.
    assert!(put.await?.is_err());
    assert_eq!(observed.call_count(S3Op::PutObject), 1);
    assert_eq!(
      observed.object(BUCKET, key().as_str()),
      Some(Bytes::from_static(PAYLOAD))
    );
    Ok(())
  })
}

#[test]
fn s3_crash_after_commit_is_retried_once_it_restarts() -> TestResult {
  simulate(SimS3::new(), |sim| async move {
    let observed = sim.s3.clone();
    let store = sim.blob_store();
    let mut paused = observed.pause_next_response(S3Op::PutObject);

    let put = tokio::spawn(async move { store.put(&key(), Bytes::from_static(PAYLOAD)).await });
    paused.reached().await;
    // The write is durable, but the process dies before it answers.
    sim.control.crash(S3_HOST);
    tokio::time::sleep(Duration::from_secs(1)).await;
    sim.control.restart(S3_HOST);

    put.await??;
    assert!(observed.call_count(S3Op::PutObject) >= 2);
    assert_eq!(
      observed.object(BUCKET, key().as_str()),
      Some(Bytes::from_static(PAYLOAD))
    );
    Ok(())
  })
}

#[test]
fn client_crash_mid_upload_leaves_no_partial_object() -> TestResult {
  // Large enough that the upload spans several round trips.
  let payload = Bytes::from(vec![7; 1 << 20]);
  simulate(SimS3::new(), move |sim| async move {
    let observed = sim.s3.clone();
    let store = sim.blob_store();

    let upload = payload.clone();
    let put = sim
      .client
      .spawn(async move { store.put(&key(), upload).await });
    // Let the request head and part of the body reach the server.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!put.is_finished(), "the upload must still be in flight");
    // The server's handler is already streaming the body.
    assert_eq!(observed.call_count(S3Op::PutObject), 1);
    sim.control.crash(CLIENT_HOST);
    assert!(put.await.unwrap_err().is_cancelled());

    // The server sees the connection close mid-body and stores nothing.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(observed.object(BUCKET, key().as_str()), None);

    // A new client process can still write the key.
    let store = sim.blob_store();
    store.put(&key(), payload.clone()).await?;
    assert_eq!(observed.object(BUCKET, key().as_str()), Some(payload));
    Ok(())
  })
}
