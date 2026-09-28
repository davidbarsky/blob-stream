#[cfg(test)]
#[path = "./s3_test.rs"]
mod tests;

use bytes::Bytes;
use futures::{StreamExt, stream};
use hyper_util::rt::TokioIo;
use log::{debug, trace};
use parking_lot::Mutex;
use s3s::auth::SimpleAuth;
use s3s::dto::{
  CreateBucketInput,
  CreateBucketOutput,
  GetObjectInput,
  GetObjectOutput,
  HeadBucketInput,
  HeadBucketOutput,
  PutObjectInput,
  PutObjectOutput,
  StreamingBlob,
};
use s3s::service::S3ServiceBuilder;
use s3s::{S3, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

/// Access key accepted by [`SimS3::serve`].
pub const SIM_S3_ACCESS_KEY: &str = "blob-stream-sim";
/// Secret key accepted by [`SimS3::serve`].
pub const SIM_S3_SECRET_KEY: &str = "blob-stream-sim-secret";

//
// S3Op
//

/// S3 operation implemented by [`SimS3`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum S3Op {
  CreateBucket,
  HeadBucket,
  PutObject,
  GetObject,
}

//
// S3Fault
//

/// Server-side behavior injected into the next matching operation.
#[derive(Clone, Debug)]
pub enum S3Fault {
  /// Reply with an S3 error code before touching state.
  Error(S3ErrorCode),
  /// Serve a `GetObject` whose advertised length is complete but whose body fails after this many
  /// bytes, which closes the HTTP connection mid-body.
  TruncateBody { after: usize },
}

//
// S3Call
//

/// One operation observed by the server, in arrival order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Call {
  pub op: S3Op,
  pub key: Option<String>,
  /// Raw `Range` header value for `GetObject`.
  pub range: Option<String>,
}

//
// PausedResponse
//

/// A response that the server has computed, and whose state change has been applied, but which
/// has not yet been written back to the client.
pub struct PausedResponse {
  reached: oneshot::Receiver<()>,
  release: oneshot::Sender<()>,
}

impl PausedResponse {
  /// Wait until the paused operation has applied its state change.
  pub async fn reached(&mut self) {
    // A dropped sender means the server task was torn down; there is nothing left to wait for.
    let _ = (&mut self.reached).await;
  }

  /// Let the paused response continue to the client.
  pub fn release(self) {
    let _ = self.release.send(());
  }
}

struct PauseSlot {
  reached: oneshot::Sender<()>,
  release: oneshot::Receiver<()>,
}

//
// SimS3
//

#[derive(Default)]
struct State {
  buckets: BTreeSet<String>,
  objects: BTreeMap<(String, String), Bytes>,
  calls: Vec<S3Call>,
  faults: VecDeque<(S3Op, S3Fault)>,
  pauses: Vec<(S3Op, PauseSlot)>,
}

/// In-memory S3 implementation served over the real S3 HTTP protocol by `s3s`.
///
/// Every clone shares state, so a simulation test can hand one clone to a turmoil host and keep
/// another to seed objects, script faults, and inspect the request log. All collections are
/// ordered so a seeded simulation observes the same state in the same order on every run.
#[derive(Clone, Default)]
pub struct SimS3 {
  state: Arc<Mutex<State>>,
}

impl SimS3 {
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }

  /// Create a bucket without going through the network.
  pub fn create_bucket(&self, bucket: impl Into<String>) {
    self.state.lock().buckets.insert(bucket.into());
  }

  /// Read an object directly from server state.
  #[must_use]
  pub fn object(&self, bucket: &str, key: &str) -> Option<Bytes> {
    self
      .state
      .lock()
      .objects
      .get(&(bucket.to_string(), key.to_string()))
      .cloned()
  }

  /// Every operation the server has received, in arrival order.
  #[must_use]
  pub fn calls(&self) -> Vec<S3Call> {
    self.state.lock().calls.clone()
  }

  /// Number of received calls for one operation.
  #[must_use]
  pub fn call_count(&self, op: S3Op) -> usize {
    self
      .state
      .lock()
      .calls
      .iter()
      .filter(|call| call.op == op)
      .count()
  }

  /// Apply `fault` to the next received `op` that has no earlier queued fault.
  pub fn inject(&self, op: S3Op, fault: S3Fault) {
    self.state.lock().faults.push_back((op, fault));
  }

  /// Hold the response of the next successful `op` after its state change is applied.
  #[must_use]
  pub fn pause_next_response(&self, op: S3Op) -> PausedResponse {
    let (reached_tx, reached_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    self.state.lock().pauses.push((
      op,
      PauseSlot {
        reached: reached_tx,
        release: release_rx,
      },
    ));
    PausedResponse {
      reached: reached_rx,
      release: release_tx,
    }
  }

  /// Serve this store on `port` of the current turmoil host until the host is crashed.
  ///
  /// Requests must be SigV4-signed with [`SIM_S3_ACCESS_KEY`] and [`SIM_S3_SECRET_KEY`].
  pub async fn serve(self, port: u16) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let listener = turmoil::net::TcpListener::bind(("0.0.0.0", port)).await?;
    let mut builder = S3ServiceBuilder::new(self);
    builder.set_auth(SimpleAuth::from_single(
      SIM_S3_ACCESS_KEY,
      SIM_S3_SECRET_KEY,
    ));
    let service = builder.build();
    loop {
      let (stream, peer) = listener.accept().await?;
      trace!("sim s3 accepted connection: peer={peer}");
      let service = service.clone();
      tokio::spawn(async move {
        if let Err(error) = hyper::server::conn::http1::Builder::new()
          .serve_connection(TokioIo::new(stream), service)
          .await
        {
          debug!("sim s3 connection ended with error: peer={peer}, error={error}");
        }
      });
    }
  }

  fn record(&self, call: S3Call) -> Option<S3Fault> {
    let mut state = self.state.lock();
    let op = call.op;
    state.calls.push(call);
    let index = state
      .faults
      .iter()
      .position(|(fault_op, _)| *fault_op == op)?;
    state.faults.remove(index).map(|(_, fault)| fault)
  }

  async fn pause_if_requested(&self, op: S3Op) {
    let slot = {
      let mut state = self.state.lock();
      let Some(index) = state
        .pauses
        .iter()
        .position(|(pause_op, _)| *pause_op == op)
      else {
        return;
      };
      state.pauses.remove(index).1
    };
    let _ = slot.reached.send(());
    let _ = slot.release.await;
  }

  fn require_bucket(&self, bucket: &str) -> S3Result<()> {
    if self.state.lock().buckets.contains(bucket) {
      Ok(())
    } else {
      Err(s3_error!(NoSuchBucket))
    }
  }
}

#[async_trait::async_trait]
impl S3 for SimS3 {
  async fn create_bucket(
    &self,
    req: S3Request<CreateBucketInput>,
  ) -> S3Result<S3Response<CreateBucketOutput>> {
    if let Some(S3Fault::Error(code)) = self.record(S3Call {
      op: S3Op::CreateBucket,
      key: None,
      range: None,
    }) {
      return Err(code.into());
    }
    if !self.state.lock().buckets.insert(req.input.bucket) {
      return Err(s3_error!(BucketAlreadyOwnedByYou));
    }
    self.pause_if_requested(S3Op::CreateBucket).await;
    Ok(S3Response::new(CreateBucketOutput::default()))
  }

  async fn head_bucket(
    &self,
    req: S3Request<HeadBucketInput>,
  ) -> S3Result<S3Response<HeadBucketOutput>> {
    if let Some(S3Fault::Error(code)) = self.record(S3Call {
      op: S3Op::HeadBucket,
      key: None,
      range: None,
    }) {
      return Err(code.into());
    }
    self.require_bucket(&req.input.bucket)?;
    self.pause_if_requested(S3Op::HeadBucket).await;
    Ok(S3Response::new(HeadBucketOutput::default()))
  }

  async fn put_object(
    &self,
    req: S3Request<PutObjectInput>,
  ) -> S3Result<S3Response<PutObjectOutput>> {
    let input = req.input;
    if let Some(S3Fault::Error(code)) = self.record(S3Call {
      op: S3Op::PutObject,
      key: Some(input.key.clone()),
      range: None,
    }) {
      return Err(code.into());
    }
    self.require_bucket(&input.bucket)?;
    let mut payload = Vec::new();
    if let Some(mut body) = input.body {
      while let Some(chunk) = body.next().await {
        payload.extend_from_slice(&chunk.map_err(|error| s3_error!(IncompleteBody, "{error}"))?);
      }
    }
    self
      .state
      .lock()
      .objects
      .insert((input.bucket, input.key), Bytes::from(payload));
    self.pause_if_requested(S3Op::PutObject).await;
    Ok(S3Response::new(PutObjectOutput::default()))
  }

  async fn get_object(
    &self,
    req: S3Request<GetObjectInput>,
  ) -> S3Result<S3Response<GetObjectOutput>> {
    let input = req.input;
    let fault = self.record(S3Call {
      op: S3Op::GetObject,
      key: Some(input.key.clone()),
      range: input.range.as_ref().map(s3s::dto::Range::to_header_string),
    });
    if let Some(S3Fault::Error(code)) = fault {
      return Err(code.into());
    }
    self.require_bucket(&input.bucket)?;
    let object = self
      .state
      .lock()
      .objects
      .get(&(input.bucket.clone(), input.key.clone()))
      .cloned()
      .ok_or_else(|| s3_error!(NoSuchKey))?;
    let full_length = object.len() as u64;
    let (body, content_range) = match input.range {
      None => (object, None),
      Some(range) => {
        let range = range.check(full_length)?;
        let start = usize::try_from(range.start).map_err(|_| s3_error!(InvalidRange))?;
        let end = usize::try_from(range.end).map_err(|_| s3_error!(InvalidRange))?;
        let body = object.slice(start .. end);
        let content_range = format!("bytes {}-{}/{full_length}", range.start, range.end - 1);
        (body, Some(content_range))
      },
    };
    let content_length = i64::try_from(body.len()).map_err(|_| s3_error!(InternalError))?;
    let body = match fault {
      Some(S3Fault::TruncateBody { after }) => {
        let prefix = body.slice(.. after.min(body.len()));
        // hyper polls the first body chunk before writing the response head. Failing only after a
        // simulated millisecond lets the head and prefix reach the client first, so the client
        // observes a short body rather than a connection closed before any response.
        let failure = async {
          tokio::time::sleep(Duration::from_millis(1)).await;
          Err(std::io::Error::other("sim s3 truncated body"))
        };
        StreamingBlob::wrap(stream::once(async { Ok(prefix) }).chain(stream::once(failure)))
      },
      _ => StreamingBlob::from_bytes(body),
    };
    self.pause_if_requested(S3Op::GetObject).await;
    Ok(S3Response::new(GetObjectOutput {
      body: Some(body),
      content_length: Some(content_length),
      content_range,
      ..Default::default()
    }))
  }
}
