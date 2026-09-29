//! Property test: the production S3 blob store keeps its contract under any fault schedule.
//!
//! Every hegel test case is one turmoil-net simulation on a paused tokio runtime, which skips idle
//! time, so a case that waits out long timeouts replays in milliseconds. Hegel is the only source
//! of choices: it draws the workload before the simulation starts, and two independent `TestCase`
//! clones draw server faults and the chaos schedule (partitions, holds, and crashes of either host)
//! while it runs. Every fault draw shrinks toward "no fault", so a failure is reported with the
//! fewest faults that still break an invariant.
//!
//! A client crash is either timed by the schedule or triggered once the server has received part of
//! an upload, because a crash at a random time almost never lands inside one. A call cut short by a
//! client crash has no outcome; it is exempt from invariants that need one, but its requests still
//! count against invariants 4 and 7.
//!
//! The invariants are checked after the simulation from a recorded history:
//!
//! 1. Termination: a put finishes within the AWS operation timeout. A read may take longer: the
//!    operation timeout covers the request and response head, but a response body that stops
//!    arriving is ended by the SDK's stalled-stream protection, up to six seconds later.
//! 2. No corruption: a successful read returns exactly the requested slice of the key's payload.
//! 3. Acknowledged puts are durable: after a successful put the server holds the full payload.
//! 4. No partial or phantom objects: every object version the server stored, including overwritten
//!    ones, is the full payload of a key that was put.
//! 5. No false `NotFound`: a read that starts after a successful put of its key is never
//!    `NotFound`. The consumer treats `NotFound` as authoritative loss and skips the data.
//! 6. Cache admission: the admission callback runs at most once, with the object's true length, and
//!    its answer decides between `AdmissionRejected` and success.
//! 7. Bounded attempts: the server sees at most `MAX_ATTEMPTS` requests per blob store call.
//! 8. Fault-free runs succeed: without any injected fault, puts succeed and reads return data,
//!    `NotFound`, or the admission decision, never an error.

use crate::test_support::{
  BUCKET,
  CLIENT_HOST,
  MAX_ATTEMPTS,
  OPERATION_TIMEOUT,
  S3_HOST,
  simulate,
};
use crate::{S3Call, S3ErrorCode, S3Fault, S3Op, SimControl, SimHost, SimS3};
use blob_stream_blob_store::{BlobKey, BlobStore, BlobStoreError, ByteRange, S3BlobStore};
use bytes::Bytes;
use hegel::{PrintableGenerator, TestCase, generators as gs};
use parking_lot::Mutex;
use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

const MAX_KEYS: usize = 3;
/// Large enough that an upload spans many TCP segments, so a crash can cut one short.
const MAX_PAYLOAD: usize = 128 * 1024;
const MAX_OPS: usize = 8;
/// Puts start earlier than reads so that most reads can observe an acknowledged write.
const MAX_PUT_START_DELAY_MS: u64 = 5_000;
const MAX_READ_START_DELAY_MS: u64 = 20_000;
const MAX_BODY_CUT: usize = 4;
/// Per-chunk server read delay; with many chunks an upload then lasts seconds.
const MAX_SLOW_BODY_MS: u64 = 100;
const MAX_CHAOS_EVENTS: usize = 6;
const MAX_FAULT_MS: u64 = 20_000;
/// Time the SDK's stalled-stream protection may take to end a body that stops arriving: the S3
/// client's 5-second grace period plus the SDK's 1-second throughput check window. This is the
/// current SDK behavior, not a budget Blob Stream chose.
const STALLED_BODY_ALLOWANCE: Duration = Duration::from_secs(6);

/// The longest a call may take before it counts as hung.
fn termination_bound(op: &Op) -> Duration {
  match op {
    Op::Put { .. } => OPERATION_TIMEOUT,
    Op::GetRange { .. } | Op::GetWhole { .. } => OPERATION_TIMEOUT + STALLED_BODY_ALLOWANCE,
  }
}

//
// Workload
//

#[derive(Clone, Debug)]
enum Op {
  Put { key: usize },
  GetRange { key: usize, start: u64, end: u64 },
  GetWhole { key: usize, admit: bool },
}

impl Op {
  fn key(&self) -> usize {
    match self {
      Self::Put { key } | Self::GetRange { key, .. } | Self::GetWhole { key, .. } => *key,
    }
  }
}

#[derive(Clone, Debug)]
struct ScheduledOp {
  op: Op,
  start_after: Duration,
}

struct Workload {
  payloads: Vec<Bytes>,
  ops: Vec<ScheduledOp>,
}

fn blob_key(key: usize) -> BlobKey {
  BlobKey::new(format!("topic/partition-0/blob-{key}"))
}

/// A payload that differs from every other key's payload at every offset.
fn payload(key: usize, len: usize) -> Bytes {
  (0 .. len)
    .map(|offset| u8::try_from((key * 97 + offset * 13 + 1) % 251).unwrap_or_default())
    .collect::<Vec<_>>()
    .into()
}

fn draw_workload(tc: &TestCase) -> Workload {
  let lengths: Vec<usize> = tc.draw(
    gs::vecs(gs::integers::<usize>().min_value(1).max_value(MAX_PAYLOAD))
      .min_size(1)
      .max_size(MAX_KEYS),
  );
  let payloads = lengths
    .iter()
    .enumerate()
    .map(|(key, length)| payload(key, *length))
    .collect();
  let count = tc.draw(gs::integers::<usize>().min_value(1).max_value(MAX_OPS));
  let ops = (0 .. count)
    .map(|_| {
      let key = tc.draw(gs::integers::<usize>().max_value(lengths.len() - 1));
      let length = lengths[key] as u64;
      let op = match tc.draw(gs::integers::<u8>().max_value(2)) {
        0 => Op::Put { key },
        1 => {
          let start = tc.draw(gs::integers::<u64>().max_value(length - 1));
          let end = tc.draw(gs::integers::<u64>().min_value(start + 1).max_value(length));
          Op::GetRange { key, start, end }
        },
        _ => Op::GetWhole {
          key,
          admit: tc.draw(gs::booleans()),
        },
      };
      let max_delay = match op {
        Op::Put { .. } => MAX_PUT_START_DELAY_MS,
        Op::GetRange { .. } | Op::GetWhole { .. } => MAX_READ_START_DELAY_MS,
      };
      let start_after = Duration::from_millis(tc.draw(gs::integers::<u64>().max_value(max_delay)));
      ScheduledOp { op, start_after }
    })
    .collect();
  Workload { payloads, ops }
}

//
// SimDraws
//

/// A `TestCase` stream that can be drawn from inside the simulation.
///
/// Hegel ends a test case by panicking out of `draw`. Inside a spawned task that panic would
/// become a `JoinError` and read as a test failure, so the draw catches it, remembers it, and
/// reports "no more choices". The test re-raises it on the test thread after the simulation.
#[derive(Clone)]
struct SimDraws {
  tc: Arc<Mutex<TestCase>>,
  stop: Arc<Mutex<Option<Box<dyn Any + Send>>>>,
}

impl SimDraws {
  fn draw<T>(&self, generator: impl PrintableGenerator<T>) -> Option<T> {
    let mut stop = self.stop.lock();
    if stop.is_some() {
      return None;
    }
    let tc = self.tc.lock();
    match catch_unwind(AssertUnwindSafe(|| tc.draw(generator))) {
      Ok(value) => Some(value),
      Err(panic) => {
        *stop = Some(panic);
        None
      },
    }
  }
}

fn draw_server_fault(draws: &SimDraws, call: &S3Call) -> Option<S3Fault> {
  if !draws.draw(gs::weighted_booleans(0.25))? {
    return None;
  }
  let kinds: u8 = match call.op {
    S3Op::GetObject => 6,
    S3Op::PutObject => 5,
    S3Op::CreateBucket | S3Op::HeadBucket => 3,
  };
  let fault = match draws.draw(gs::integers::<u8>().max_value(kinds - 1))? {
    0 => S3Fault::Error(S3ErrorCode::InternalError),
    1 => S3Fault::Error(S3ErrorCode::SlowDown),
    2 => S3Fault::Delay(Duration::from_millis(
      draws.draw(gs::integers::<u64>().min_value(1).max_value(MAX_FAULT_MS))?,
    )),
    3 if call.op == S3Op::PutObject => S3Fault::ErrorAfterCommit(S3ErrorCode::InternalError),
    _ if call.op == S3Op::PutObject => S3Fault::SlowBody(Duration::from_millis(
      draws.draw(
        gs::integers::<u64>()
          .min_value(1)
          .max_value(MAX_SLOW_BODY_MS),
      )?,
    )),
    kind => {
      // Payloads can be one byte long, so keep the cut point small enough to land inside most
      // bodies.
      let after = draws.draw(gs::integers::<usize>().max_value(MAX_BODY_CUT))?;
      if kind == 3 {
        S3Fault::TruncateBody { after }
      } else {
        S3Fault::StallBody { after }
      }
    },
  };
  Some(fault)
}

/// Partition or hold the client-to-S3 link, or crash a host, until the draws say stop.
async fn chaos(
  control: SimControl,
  s3: SimS3,
  draws: SimDraws,
  faults: Arc<AtomicUsize>,
  log: Arc<Mutex<Vec<String>>>,
) {
  for _ in 0 .. MAX_CHAOS_EVENTS {
    let Some(gap) = draws.draw(gs::integers::<u64>().max_value(MAX_FAULT_MS)) else {
      return;
    };
    tokio::time::sleep(Duration::from_millis(gap)).await;
    // Kind 0 ends the chaos, so shrinking toward 0 removes these faults.
    let Some(kind @ 1 ..= 5) = draws.draw(gs::integers::<u8>().max_value(5)) else {
      return;
    };
    let Some(duration) = draws.draw(gs::integers::<u64>().min_value(1).max_value(MAX_FAULT_MS))
    else {
      return;
    };
    faults.fetch_add(1, Ordering::SeqCst);
    let event = match kind {
      1 => "network partition",
      2 => "network hold",
      3 => "s3 crash",
      4 => "client crash",
      _ => "client crash mid-upload",
    };
    log.lock().push(event.to_string());
    match kind {
      1 => control.partition(CLIENT_HOST, S3_HOST),
      2 => control.hold(CLIENT_HOST, S3_HOST),
      3 => control.crash(S3_HOST),
      // A client crash kills its in-flight calls; later calls run in the restarted process.
      4 => {
        control.crash(CLIENT_HOST);
        continue;
      },
      // Crashing at a random time rarely lands inside an upload, so wait for one.
      _ => {
        s3.upload_in_progress().await;
        control.crash(CLIENT_HOST);
        continue;
      },
    }
    tokio::time::sleep(Duration::from_millis(duration)).await;
    match kind {
      1 => control.repair(CLIENT_HOST, S3_HOST),
      2 => control.release(CLIENT_HOST, S3_HOST),
      _ => control.restart(S3_HOST),
    }
  }
}

//
// History
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadFailure {
  NotFound,
  InvalidRange,
  AdmissionRejected,
  Read,
}

/// Read bytes whose debug output is their length, so failure reports stay short.
#[derive(Clone, PartialEq, Eq)]
struct Blob(Bytes);

impl std::fmt::Debug for Blob {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{} bytes", self.0.len())
  }
}

#[derive(Clone, Debug)]
enum Outcome {
  Put(Result<(), String>),
  Read(Result<Blob, ReadFailure>),
  /// The client host crashed while the call was in flight.
  Crashed,
  Hung,
}

#[derive(Clone, Debug)]
struct Record {
  op: Op,
  /// Position of the call's start and end in one sequence shared by all calls.
  started: u64,
  ended: u64,
  elapsed: Duration,
  outcome: Outcome,
  admissions: Vec<u64>,
}

fn read_failure(error: &BlobStoreError) -> ReadFailure {
  match error {
    BlobStoreError::NotFound { .. } => ReadFailure::NotFound,
    BlobStoreError::InvalidRange { .. } => ReadFailure::InvalidRange,
    BlobStoreError::AdmissionRejected { .. } => ReadFailure::AdmissionRejected,
    BlobStoreError::Read { .. } => ReadFailure::Read,
  }
}

async fn run_op(
  scheduled: ScheduledOp,
  client: SimHost,
  store: S3BlobStore,
  payload: Bytes,
  sequence: Arc<AtomicU64>,
) -> Record {
  tokio::time::sleep(scheduled.start_after).await;
  let op = scheduled.op;
  let key = blob_key(op.key());
  let admissions = Arc::new(Mutex::new(Vec::new()));
  let started = sequence.fetch_add(1, Ordering::SeqCst);
  let started_at = tokio::time::Instant::now();
  let call_op = op.clone();
  let call_admissions = Arc::clone(&admissions);
  // The call runs on the client host so that a client crash kills it; this task only observes.
  let call = client.spawn(async move {
    let admissions = call_admissions;
    match &call_op {
      Op::Put { .. } => Outcome::Put(
        store
          .put(&key, payload)
          .await
          .map_err(|error| format!("{error:#}")),
      ),
      Op::GetRange { start, end, .. } => Outcome::Read(
        store
          .get_range(
            &key,
            ByteRange {
              start: *start,
              end: *end,
            },
          )
          .await
          .map(Blob)
          .map_err(|error| read_failure(&error)),
      ),
      Op::GetWhole { admit, .. } => {
        let admit = *admit;
        let recorded = Arc::clone(&admissions);
        let admission = move |length| {
          recorded.lock().push(length);
          admit
        };
        Outcome::Read(
          store
            .get_with_cache_admission(&key, &admission)
            .await
            .map(Blob)
            .map_err(|error| read_failure(&error)),
        )
      },
    }
  });
  let abort = call.abort_handle();
  // Wait past the bound so an overrun is measured rather than cut off at the bound itself.
  let outcome =
    match tokio::time::timeout(termination_bound(&op) + Duration::from_secs(1), call).await {
      Ok(Ok(outcome)) => outcome,
      Ok(Err(error)) if error.is_cancelled() => Outcome::Crashed,
      Ok(Err(error)) => std::panic::resume_unwind(error.into_panic()),
      Err(_) => {
        abort.abort();
        Outcome::Hung
      },
    };
  let ended = sequence.fetch_add(1, Ordering::SeqCst);
  let admissions = admissions.lock().clone();
  Record {
    op,
    started,
    ended,
    elapsed: started_at.elapsed(),
    outcome,
    admissions,
  }
}

//
// Invariants
//

fn check_invariants(workload: &Workload, history: &[Record], server: &SimS3, faults: usize) {
  for record in history {
    let key = record.op.key();
    let payload = &workload.payloads[key];

    match (&record.op, &record.outcome) {
      // 2. No corruption.
      (Op::GetRange { start, end, .. }, Outcome::Read(Ok(bytes))) => {
        let expected =
          payload.slice(usize::try_from(*start).unwrap() .. usize::try_from(*end).unwrap());
        assert!(
          bytes.0 == expected,
          "ranged read returned wrong bytes: {record:?}"
        );
      },
      (Op::GetWhole { .. }, Outcome::Read(Ok(bytes))) => {
        assert!(
          bytes.0 == *payload,
          "whole read returned wrong bytes: {record:?}"
        );
      },
      // 3. Acknowledged puts are durable.
      (Op::Put { .. }, Outcome::Put(Ok(()))) => {
        let stored = server.object(BUCKET, blob_key(key).as_str());
        assert!(
          stored.as_ref() == Some(payload),
          "acknowledged put is not durable: {record:?}, stored={:?}",
          stored.as_ref().map(Bytes::len)
        );
      },
      _ => {},
    }

    // 5. No false NotFound.
    if matches!(record.outcome, Outcome::Read(Err(ReadFailure::NotFound))) {
      let acknowledged_before = history.iter().find(|put| {
        matches!(put.op, Op::Put { key: put_key } if put_key == key)
          && matches!(put.outcome, Outcome::Put(Ok(())))
          && put.ended < record.started
      });
      assert!(
        acknowledged_before.is_none(),
        "read after an acknowledged put returned NotFound: read={record:?}, \
         put={acknowledged_before:?}"
      );
    }

    // 6. Cache admission.
    if let Op::GetWhole { admit, .. } = record.op {
      assert!(
        record.admissions.len() <= 1,
        "admission called more than once: {record:?}"
      );
      if let Some(length) = record.admissions.first() {
        assert_eq!(
          *length,
          payload.len() as u64,
          "admission offered the wrong length: {record:?}"
        );
      }
      match &record.outcome {
        Outcome::Read(Ok(_)) => assert!(
          admit && record.admissions.len() == 1,
          "read succeeded without admission: {record:?}"
        ),
        Outcome::Read(Err(ReadFailure::AdmissionRejected)) => assert!(
          !admit && record.admissions.len() == 1,
          "read rejected without a rejecting admission: {record:?}"
        ),
        _ => {},
      }
    } else {
      assert!(record.admissions.is_empty());
    }

    // 8. Fault-free runs succeed.
    if faults == 0 {
      let expected_outcome = match &record.outcome {
        Outcome::Put(result) => result.is_ok(),
        Outcome::Read(Ok(_) | Err(ReadFailure::NotFound)) => true,
        Outcome::Read(Err(ReadFailure::AdmissionRejected)) => {
          matches!(record.op, Op::GetWhole { admit: false, .. })
        },
        _ => false,
      };
      assert!(expected_outcome, "fault-free call failed: {record:?}");
    }

    // 1. Termination. Checked after the data invariants, which matter more when both fail.
    let bound = termination_bound(&record.op);
    assert!(
      !matches!(record.outcome, Outcome::Hung) && record.elapsed <= bound,
      "call did not finish within {bound:?}: {record:?}"
    );
  }

  let calls = server.calls();
  let writes = server.writes();
  for (key, payload) in workload.payloads.iter().enumerate() {
    let key_name = blob_key(key);
    let ops_on_key = |matches: fn(&Op) -> bool| {
      history
        .iter()
        .filter(|record| record.op.key() == key && matches(&record.op))
        .count()
    };
    let calls_on_key = |op: S3Op| {
      calls
        .iter()
        .filter(|call| call.op == op && call.key.as_deref() == Some(key_name.as_str()))
        .count()
    };
    let puts = ops_on_key(|op| matches!(op, Op::Put { .. }));
    let reads = ops_on_key(|op| !matches!(op, Op::Put { .. }));

    // 4. No partial or phantom objects, in any version the server ever stored.
    for (_, stored) in writes.iter().filter(|(name, _)| name == key_name.as_str()) {
      assert!(puts > 0, "object exists without a put: key={key}");
      assert!(
        stored == payload,
        "stored object is not the full payload: key={key}, stored={} bytes, payload={} bytes",
        stored.len(),
        payload.len()
      );
    }

    // 7. Bounded attempts.
    assert!(
      calls_on_key(S3Op::PutObject) <= MAX_ATTEMPTS * puts,
      "more than {MAX_ATTEMPTS} PutObject requests per put: key={key}, puts={puts}, \
       calls={calls:?}"
    );
    assert!(
      calls_on_key(S3Op::GetObject) <= MAX_ATTEMPTS * reads,
      "more than {MAX_ATTEMPTS} GetObject requests per read: key={key}, reads={reads}, \
       calls={calls:?}"
    );
  }
}

//
// Property
//

#[hegel::test]
fn blob_store_upholds_its_contract_under_injected_faults(tc: TestCase) {
  let workload = draw_workload(&tc);
  let stop = Arc::new(Mutex::new(None));
  let server_draws = SimDraws {
    tc: Arc::new(Mutex::new(tc.clone())),
    stop: Arc::clone(&stop),
  };
  let network_draws = SimDraws {
    tc: Arc::new(Mutex::new(tc.clone())),
    stop: Arc::clone(&stop),
  };
  let faults = Arc::new(AtomicUsize::new(0));
  let fault_log = Arc::new(Mutex::new(Vec::new()));

  let s3 = SimS3::new();
  let server_faults = Arc::clone(&faults);
  let server_fault_log = Arc::clone(&fault_log);
  s3.set_fault_source(move |call| {
    let fault = draw_server_fault(&server_draws, call);
    if let Some(fault) = &fault {
      server_faults.fetch_add(1, Ordering::SeqCst);
      let name = format!("{fault:?}");
      let name = name.split(['(', ' ']).next().unwrap_or_default();
      server_fault_log.lock().push(format!("server fault {name}"));
    }
    fault
  });
  let ops = workload.ops.clone();
  let payloads = workload.payloads.clone();
  let chaos_faults = Arc::clone(&faults);
  let chaos_log = Arc::clone(&fault_log);
  let history = simulate(s3.clone(), move |sim| async move {
    let chaos = tokio::spawn(chaos(
      sim.control.clone(),
      sim.s3.clone(),
      network_draws,
      chaos_faults,
      chaos_log,
    ));
    let store = sim.blob_store();
    let sequence = Arc::new(AtomicU64::new(0));
    let calls: Vec<_> = ops
      .into_iter()
      .map(|scheduled| {
        let payload = payloads[scheduled.op.key()].clone();
        tokio::spawn(run_op(
          scheduled,
          sim.client.clone(),
          store.clone(),
          payload,
          Arc::clone(&sequence),
        ))
      })
      .collect();
    let mut history = Vec::with_capacity(calls.len());
    for call in calls {
      history.push(call.await);
    }
    chaos.abort();
    history
  });

  if let Some(panic) = stop.lock().take() {
    resume_unwind(panic);
  }
  let history: Vec<Record> = history
    .into_iter()
    .collect::<Result<_, _>>()
    .expect("a blob store call panicked");
  let mut ordered: Vec<_> = history.iter().collect();
  ordered.sort_by_key(|record| record.started);
  tc.note(&format!("history: {ordered:#?}"));
  for fault in fault_log.lock().iter() {
    tc.event(fault);
  }
  tc.event(format!(
    "faults injected {}",
    faults.load(Ordering::SeqCst).min(3)
  ));
  for record in &ordered {
    let outcome = match &record.outcome {
      Outcome::Put(Ok(())) => "put ok".to_string(),
      Outcome::Put(Err(_)) => "put err".to_string(),
      Outcome::Read(Ok(_)) => "read ok".to_string(),
      Outcome::Read(Err(failure)) => format!("read {failure:?}"),
      Outcome::Crashed => "crashed".to_string(),
      Outcome::Hung => "hung".to_string(),
    };
    tc.event(&outcome);
  }
  check_invariants(&workload, &history, &s3, faults.load(Ordering::SeqCst));
}
