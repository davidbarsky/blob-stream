//! Property test: the production S3 blob store keeps its contract under any fault schedule.
//!
//! Every hegel test case is one simulation on its own paused tokio runtime, which skips idle time,
//! so a case that waits out long timeouts replays in milliseconds. Hegel is the only source of
//! choices: it draws the workload before the simulation starts, and two independent `TestCase`
//! clones draw server faults and network faults while it runs. Every fault draw shrinks toward "no
//! fault", so a failure is reported with the fewest faults that still break an invariant.
//!
//! The invariants are checked after the simulation from a recorded history:
//!
//! 1. Termination: a put finishes within the AWS operation timeout. A read may take longer: the
//!    operation timeout covers the request and response head, but a response body that stops
//!    arriving is ended by the SDK's stalled-stream protection, up to six seconds later.
//! 2. No corruption: a successful read returns exactly the requested slice of the key's payload.
//! 3. Acknowledged puts are durable: after a successful put the server holds the full payload.
//! 4. No partial or phantom objects: every stored object is the full payload of a key that was put.
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
  Simulation,
};
use crate::{S3Call, S3ErrorCode, S3Fault, S3Op, SimNet, SimS3};
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
const MAX_PAYLOAD: usize = 64;
const MAX_OPS: usize = 8;
/// Puts start earlier than reads so that most reads can observe an acknowledged write.
const MAX_PUT_START_DELAY_MS: u64 = 5_000;
const MAX_READ_START_DELAY_MS: u64 = 20_000;
const MAX_BODY_CUT: usize = 4;
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
    S3Op::PutObject => 4,
    S3Op::CreateBucket | S3Op::HeadBucket => 3,
  };
  let fault = match draws.draw(gs::integers::<u8>().max_value(kinds - 1))? {
    0 => S3Fault::Error(S3ErrorCode::InternalError),
    1 => S3Fault::Error(S3ErrorCode::SlowDown),
    2 => S3Fault::Delay(Duration::from_millis(
      draws.draw(gs::integers::<u64>().min_value(1).max_value(MAX_FAULT_MS))?,
    )),
    3 if call.op == S3Op::PutObject => S3Fault::ErrorAfterCommit(S3ErrorCode::InternalError),
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

/// Partition or hold the client-to-S3 link for drawn intervals until the draws say stop.
async fn network_chaos(
  net: SimNet,
  draws: SimDraws,
  faults: Arc<AtomicUsize>,
  log: Arc<Mutex<Vec<String>>>,
) {
  for _ in 0 .. MAX_CHAOS_EVENTS {
    let Some(gap) = draws.draw(gs::integers::<u64>().max_value(MAX_FAULT_MS)) else {
      return;
    };
    tokio::time::sleep(Duration::from_millis(gap)).await;
    // Kind 0 ends the chaos, so shrinking toward 0 removes network faults.
    let Some(kind @ 1 ..= 2) = draws.draw(gs::integers::<u8>().max_value(2)) else {
      return;
    };
    let Some(duration) = draws.draw(gs::integers::<u64>().min_value(1).max_value(MAX_FAULT_MS))
    else {
      return;
    };
    faults.fetch_add(1, Ordering::SeqCst);
    log.lock().push(
      if kind == 1 {
        "network partition"
      } else {
        "network hold"
      }
      .to_string(),
    );
    if kind == 1 {
      net.partition(CLIENT_HOST, S3_HOST);
    } else {
      net.hold(CLIENT_HOST, S3_HOST);
    }
    tokio::time::sleep(Duration::from_millis(duration)).await;
    if kind == 1 {
      net.repair(CLIENT_HOST, S3_HOST);
    } else {
      net.release(CLIENT_HOST, S3_HOST);
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

#[derive(Clone, Debug)]
enum Outcome {
  Put(Result<(), String>),
  Read(Result<Bytes, ReadFailure>),
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
  let call = async {
    match &op {
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
            .map_err(|error| read_failure(&error)),
        )
      },
    }
  };
  // Wait past the bound so an overrun is measured rather than cut off at the bound itself.
  let outcome = tokio::time::timeout(termination_bound(&op) + Duration::from_secs(1), call)
    .await
    .unwrap_or(Outcome::Hung);
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
        assert_eq!(
          bytes, &expected,
          "ranged read returned wrong bytes: {record:?}"
        );
      },
      (Op::GetWhole { .. }, Outcome::Read(Ok(bytes))) => {
        assert_eq!(
          bytes, payload,
          "whole read returned wrong bytes: {record:?}"
        );
      },
      // 3. Acknowledged puts are durable.
      (Op::Put { .. }, Outcome::Put(Ok(()))) => {
        assert_eq!(
          server.object(BUCKET, blob_key(key).as_str()).as_ref(),
          Some(payload),
          "acknowledged put is not durable: {record:?}"
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

    // 4. No partial or phantom objects.
    if let Some(stored) = server.object(BUCKET, key_name.as_str()) {
      assert!(puts > 0, "object exists without a put: key={key}");
      assert_eq!(
        &stored, payload,
        "stored object is not the full payload: key={key}"
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
  let runtime = tokio::runtime::Builder::new_current_thread()
    .enable_time()
    .start_paused(true)
    .build()
    .expect("build paused runtime");
  let history = runtime.block_on(async {
    let sim = Simulation::start(&s3);
    let chaos = tokio::spawn(network_chaos(
      sim.net.clone(),
      network_draws,
      Arc::clone(&faults),
      Arc::clone(&fault_log),
    ));
    let store = sim.blob_store();
    let sequence = Arc::new(AtomicU64::new(0));
    let calls: Vec<_> = workload
      .ops
      .iter()
      .map(|scheduled| {
        tokio::spawn(run_op(
          scheduled.clone(),
          store.clone(),
          workload.payloads[scheduled.op.key()].clone(),
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
  // Dropping the runtime cancels the server and any stalled connections.
  drop(runtime);

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
      Outcome::Hung => "hung".to_string(),
    };
    tc.event(&outcome);
  }
  check_invariants(&workload, &history, &s3, faults.load(Ordering::SeqCst));
}
