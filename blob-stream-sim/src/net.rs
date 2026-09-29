#[cfg(test)]
#[path = "./net_test.rs"]
mod tests;

use log::trace;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf};
use tokio::sync::{Notify, mpsc};

/// Bytes buffered in each direction of a connection before the writer waits.
const CONNECTION_BUFFER: usize = 64 * 1024;

/// A connection endpoint. It is an ordinary in-memory byte stream; faults are applied by the link
/// between the two endpoints.
pub type SimStream = DuplexStream;

//
// SimNet
//

/// An in-process network between named hosts, for use on one paused tokio runtime.
///
/// Every connection runs through two link tasks, one per direction, that forward bytes after the
/// link latency and apply the fault state between the two hosts at the moment each chunk is sent:
///
/// - A partition refuses new connections and drops every chunk sent while it is in place, so an
///   established connection stalls without an error, like a black-holed route.
/// - A hold delays new connections and buffers every chunk until release, then delivers them in
///   order.
///
/// All timing uses tokio's clock. On a runtime started with `start_paused(true)`, tokio advances
/// the clock straight to the next timer whenever no task can run, so simulated minutes cost
/// microseconds of wall time and each run has the same schedule.
#[derive(Clone, Default)]
pub struct SimNet {
  state: Arc<Mutex<NetState>>,
}

#[derive(Default)]
struct NetState {
  listeners: BTreeMap<(String, u16), mpsc::UnboundedSender<SimStream>>,
  links: BTreeMap<(String, String), Arc<Link>>,
  latency: Duration,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum LinkState {
  #[default]
  Up,
  Partitioned,
  Held,
}

/// The fault state between two hosts.
///
/// Waiters use one `Notify`, which wakes them in the order they started waiting. A
/// `tokio::sync::watch` channel would not: it spreads receivers over several notifiers chosen with
/// an unseeded random number, so connections released together would proceed in a different order
/// on every run.
#[derive(Default)]
struct Link {
  state: Mutex<LinkState>,
  changed: Notify,
}

impl Link {
  fn set(&self, state: LinkState) {
    *self.state.lock() = state;
    self.changed.notify_waiters();
  }

  /// Wait until the link is not held, then return its state.
  async fn wait_unheld(&self) -> LinkState {
    loop {
      // Register before reading the state so a change between the two still wakes this waiter.
      let changed = self.changed.notified();
      let state = *self.state.lock();
      if state != LinkState::Held {
        return state;
      }
      changed.await;
    }
  }
}

/// Normalize a host pair so both directions share one link state.
fn pair(a: &str, b: &str) -> (String, String) {
  if a <= b {
    (a.to_string(), b.to_string())
  } else {
    (b.to_string(), a.to_string())
  }
}

impl SimNet {
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }

  /// Delay every chunk by `latency` in each direction.
  pub fn set_latency(&self, latency: Duration) {
    self.state.lock().latency = latency;
  }

  /// Drop all traffic between `a` and `b` and refuse new connections until [`repair`].
  ///
  /// [`repair`]: Self::repair
  pub fn partition(&self, a: &str, b: &str) {
    self.set_link(a, b, LinkState::Partitioned);
  }

  /// Undo [`partition`](Self::partition). Chunks dropped while partitioned stay lost.
  pub fn repair(&self, a: &str, b: &str) {
    self.set_link(a, b, LinkState::Up);
  }

  /// Buffer all traffic between `a` and `b`, including connection attempts, until [`release`].
  ///
  /// [`release`]: Self::release
  pub fn hold(&self, a: &str, b: &str) {
    self.set_link(a, b, LinkState::Held);
  }

  /// Undo [`hold`](Self::hold) and deliver the buffered traffic in order.
  pub fn release(&self, a: &str, b: &str) {
    self.set_link(a, b, LinkState::Up);
  }

  /// Accept connections to `host:port`.
  #[must_use]
  pub fn bind(&self, host: &str, port: u16) -> SimListener {
    let (sender, receiver) = mpsc::unbounded_channel();
    self
      .state
      .lock()
      .listeners
      .insert((host.to_string(), port), sender);
    SimListener { receiver }
  }

  /// Open a connection from host `from` to `host:port`.
  pub async fn connect(&self, from: &str, host: &str, port: u16) -> io::Result<SimStream> {
    let link = self.link(from, host);
    // A connection attempt waits out a hold, then fails if the hosts are partitioned.
    let state = link.wait_unheld().await;
    if state == LinkState::Partitioned {
      trace!("sim net connect refused by partition: from={from}, to={host}:{port}");
      return Err(io::Error::new(
        io::ErrorKind::ConnectionRefused,
        format!("{host}:{port} is partitioned from {from}"),
      ));
    }
    let listener = self
      .state
      .lock()
      .listeners
      .get(&(host.to_string(), port))
      .cloned()
      .ok_or_else(|| {
        io::Error::new(
          io::ErrorKind::ConnectionRefused,
          format!("nothing listens on {host}:{port}"),
        )
      })?;

    let (client, client_link) = tokio::io::duplex(CONNECTION_BUFFER);
    let (server_link, server) = tokio::io::duplex(CONNECTION_BUFFER);
    let (client_read, client_write) = tokio::io::split(client_link);
    let (server_read, server_write) = tokio::io::split(server_link);
    tokio::spawn(forward(
      self.clone(),
      Arc::clone(&link),
      client_read,
      server_write,
    ));
    tokio::spawn(forward(self.clone(), link, server_read, client_write));
    listener
      .send(server)
      .map_err(|_| io::Error::from(io::ErrorKind::ConnectionRefused))?;
    trace!("sim net connected: from={from}, to={host}:{port}");
    Ok(client)
  }

  fn set_link(&self, a: &str, b: &str, state: LinkState) {
    trace!("sim net link change: a={a}, b={b}, state={state:?}");
    self.link(a, b).set(state);
  }

  fn link(&self, a: &str, b: &str) -> Arc<Link> {
    Arc::clone(self.state.lock().links.entry(pair(a, b)).or_default())
  }

  fn latency(&self) -> Duration {
    self.state.lock().latency
  }
}

/// Forward one direction of a connection, applying the link state to each chunk.
async fn forward(
  net: SimNet,
  link: Arc<Link>,
  mut from: ReadHalf<DuplexStream>,
  mut to: WriteHalf<DuplexStream>,
) {
  let mut buffer = vec![0; CONNECTION_BUFFER];
  loop {
    let read = match from.read(&mut buffer).await {
      Ok(0) | Err(_) => break,
      Ok(read) => read,
    };
    if link.wait_unheld().await == LinkState::Partitioned {
      trace!("sim net dropped chunk: bytes={read}");
      continue;
    }
    let latency = net.latency();
    if !latency.is_zero() {
      tokio::time::sleep(latency).await;
    }
    if to.write_all(&buffer[.. read]).await.is_err() {
      break;
    }
  }
  let _ = to.shutdown().await;
}

//
// SimListener
//

/// Accepts connections opened with [`SimNet::connect`].
pub struct SimListener {
  receiver: mpsc::UnboundedReceiver<SimStream>,
}

impl SimListener {
  /// Wait for the next connection, or `None` once the network is dropped.
  pub async fn accept(&mut self) -> Option<SimStream> {
    self.receiver.recv().await
  }
}
