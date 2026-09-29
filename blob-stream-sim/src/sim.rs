#[cfg(test)]
#[path = "./sim_test.rs"]
mod tests;

use futures::future::BoxFuture;
use log::trace;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::Instant;
use turmoil_net::shim::tokio::net::{TcpListener, TcpStream};
use turmoil_net::{HostId, KernelConfig, Net, NetstatState, Packet, Proto, Verdict};

/// Simulated time between egress passes while a connection has unacknowledged data. Together with
/// [`RETRANSMIT_PASSES`] this sets the retransmission timeout to about 200ms, Linux's minimum.
const RETRANSMIT_TICK: Duration = Duration::from_millis(10);
/// Egress passes without acknowledgement before turmoil-net retransmits.
const RETRANSMIT_PASSES: u32 = 20;
/// Retransmissions before turmoil-net resets a connection. Linux keeps retrying an unanswered
/// connection for about 15 minutes, far longer than any simulation here, so this is effectively
/// unlimited: a black-holed connection stalls until something above TCP gives up.
const RETRANSMIT_LIMIT: u32 = 100_000;

/// Software run by a host each time it starts or restarts.
pub type HostSoftware = Arc<dyn Fn(SimHost) -> BoxFuture<'static, ()> + Send + Sync>;

//
// Sim
//

/// A deterministic simulation of hosts talking TCP over [`turmoil_net`].
///
/// All hosts share one current-thread tokio runtime started with `start_paused(true)`. An
/// event-driven router moves packets between hosts: it runs an egress pass whenever a socket
/// operation may have produced packets, when a delayed packet comes due, and every
/// [`RETRANSMIT_TICK`] while a connection has unacknowledged data. Between those events every task
/// is parked, so tokio advances its clock straight to the next timer and simulated idle time costs
/// no wall time.
///
/// Faults are applied per host pair when a packet leaves its host:
///
/// - A partition drops the packet. TCP retransmits it, so a connection survives a partition that
///   ends before something above TCP gives up, and connection attempts time out rather than being
///   refused.
/// - A hold queues the packet until release, then delivers the queue in order.
/// - A crash aborts every task the host spawned. Its sockets close as they would when a process
///   exits, and the host refuses new connections until it restarts. The host's kernel survives, so
///   this models a process crash, not a power loss.
pub struct Sim {
  net: Net,
  hosts: BTreeMap<String, (HostId, IpAddr)>,
  latency: Duration,
  seed: u64,
}

impl Sim {
  /// A simulation whose packets take `latency` to cross any link.
  #[must_use]
  pub fn new(latency: Duration) -> Self {
    Self {
      net: Net::with_config(
        KernelConfig::default()
          .retx_threshold(RETRANSMIT_PASSES)
          .retx_max(RETRANSMIT_LIMIT),
      ),
      hosts: BTreeMap::new(),
      latency,
      seed: 0,
    }
  }

  /// Seed fastrand's thread-local generator when the simulation starts. The AWS SDK draws retry
  /// jitter from it, and every task runs on this thread, so the seed fixes the retry schedule.
  #[must_use]
  pub fn seed(mut self, seed: u64) -> Self {
    self.seed = seed;
    self
  }

  /// Register a host. Every host must be registered before [`run`](Self::run).
  #[must_use]
  pub fn host(mut self, name: &str) -> Self {
    let id = self.net.add_host(name);
    let ip = self.net.lookup(name);
    self.hosts.insert(name.to_string(), (id, ip));
    self
  }

  /// Run `main` to completion and return its output.
  ///
  /// `main` runs outside every host, so a crash never aborts it. It drives the simulation through
  /// the [`SimControl`] it receives.
  pub fn run<F, Fut>(self, main: F) -> Fut::Output
  where
    F: FnOnce(SimControl) -> Fut,
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
  {
    let control = SimControl {
      shared: Arc::new(Shared {
        wake: Notify::new(),
        state: Mutex::new(State {
          hosts: self
            .hosts
            .iter()
            .map(|(name, (id, ip))| {
              (
                name.clone(),
                HostState {
                  id: *id,
                  ip: *ip,
                  tasks: Vec::new(),
                  software: None,
                },
              )
            })
            .collect(),
          faults: BTreeMap::new(),
          held: Vec::new(),
        }),
      }),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
      .enable_time()
      .start_paused(true)
      .build()
      .expect("build paused runtime");
    let guard = self.net.enter();
    fastrand::seed(self.seed);
    let mut router = Router {
      latency: self.latency,
      pending: BTreeMap::new(),
      next_seq: 0,
      egress: Vec::new(),
    };
    // The future borrows the guard so the network outlives the runtime; see the drop below.
    let guard = &guard;
    let output = runtime.block_on(async move {
      let mut main = tokio::spawn(main(control.clone()));
      loop {
        router.route(guard, &control);
        let deadline = router.next_deadline(&control);
        tokio::select! {
          biased;
          output = &mut main => return output.expect("simulation main task panicked"),
          () = control.shared.wake.notified() => {},
          () = sleep_until(deadline) => {},
        }
      }
    });
    // Tasks still parked on sockets drop them while the runtime shuts down, which needs the
    // network installed, so the runtime goes first.
    drop(runtime);
    output
  }
}

async fn sleep_until(deadline: Option<Instant>) {
  match deadline {
    Some(deadline) => tokio::time::sleep_until(deadline).await,
    None => std::future::pending().await,
  }
}

//
// Router
//

struct Router {
  latency: Duration,
  /// Packets waiting for their delivery time, ordered by time and then emission order.
  pending: BTreeMap<(Instant, u64), Packet>,
  next_seq: u64,
  egress: Vec<Packet>,
}

impl Router {
  /// Deliver due packets and route new egress until neither produces more work.
  fn route(&mut self, guard: &turmoil_net::EnterGuard, control: &SimControl) {
    loop {
      let now = Instant::now();
      // Packets released from a hold rejoin the network in the order they were sent.
      for packet in control.take_released() {
        self.schedule(packet, now + self.latency);
      }
      let due: Vec<_> = self
        .pending
        .keys()
        .take_while(|(at, _)| *at <= now)
        .copied()
        .collect();
      for key in &due {
        if let Some(packet) = self.pending.remove(key) {
          guard.deliver(packet);
        }
      }

      guard.egress_all(&mut self.egress);
      if self.egress.is_empty() && due.is_empty() {
        return;
      }
      for packet in std::mem::take(&mut self.egress) {
        match control.link(packet.src, packet.dst) {
          Link::Partitioned => trace!("sim dropped packet: src={}, dst={}", packet.src, packet.dst),
          Link::Held => control.shared.state.lock().held.push(packet),
          Link::Up => match guard.evaluate(&packet) {
            Verdict::Drop => {},
            Verdict::Deliver(delay) => self.schedule(packet, now + self.latency + delay),
            Verdict::Pass => self.schedule(packet, now + self.latency),
          },
        }
      }
    }
  }

  fn schedule(&mut self, packet: Packet, at: Instant) {
    self.pending.insert((at, self.next_seq), packet);
    self.next_seq += 1;
  }

  /// The next time the network needs an egress pass without being woken by a socket operation.
  fn next_deadline(&self, control: &SimControl) -> Option<Instant> {
    let delivery = self.pending.keys().next().map(|(at, _)| *at);
    let retransmit = control
      .has_unacknowledged_data()
      .then(|| Instant::now() + RETRANSMIT_TICK);
    delivery.into_iter().chain(retransmit).min()
  }
}

//
// SimControl
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Link {
  Up,
  Partitioned,
  Held,
}

struct HostState {
  id: HostId,
  ip: IpAddr,
  tasks: Vec<AbortHandle>,
  software: Option<HostSoftware>,
}

struct State {
  hosts: BTreeMap<String, HostState>,
  faults: BTreeMap<(IpAddr, IpAddr), Link>,
  /// Held packets; each is released when its link stops being held.
  held: Vec<Packet>,
}

struct Shared {
  /// Woken by every socket operation so the router can move the packets it may have produced.
  wake: Notify,
  state: Mutex<State>,
}

/// Controls a running [`Sim`]: hosts, network faults, and crashes.
#[derive(Clone)]
pub struct SimControl {
  shared: Arc<Shared>,
}

fn pair(a: IpAddr, b: IpAddr) -> (IpAddr, IpAddr) {
  if a <= b { (a, b) } else { (b, a) }
}

impl SimControl {
  /// A handle to a registered host.
  ///
  /// # Panics
  ///
  /// Panics if `name` was not registered with [`Sim::host`].
  #[must_use]
  pub fn host(&self, name: &str) -> SimHost {
    let state = self.shared.state.lock();
    let host = state
      .hosts
      .get(name)
      .unwrap_or_else(|| panic!("host {name} is not registered"));
    SimHost {
      name: Arc::from(name),
      id: host.id,
      control: self.clone(),
    }
  }

  /// Run `software` on `name` now and again after every [`restart`](Self::restart).
  pub fn start(&self, name: &str, software: HostSoftware) {
    let host = self.host(name);
    let running = software(host.clone());
    self
      .shared
      .state
      .lock()
      .hosts
      .get_mut(name)
      .unwrap_or_else(|| panic!("host {name} is not registered"))
      .software = Some(software);
    host.spawn(running);
  }

  /// Abort every task running on `name`, as if its process exited.
  pub fn crash(&self, name: &str) {
    trace!("sim crash: host={name}");
    let tasks = std::mem::take(
      &mut self
        .shared
        .state
        .lock()
        .hosts
        .get_mut(name)
        .unwrap_or_else(|| panic!("host {name} is not registered"))
        .tasks,
    );
    for task in tasks {
      task.abort();
    }
    self.shared.wake.notify_one();
  }

  /// Start the software of a crashed host again.
  pub fn restart(&self, name: &str) {
    trace!("sim restart: host={name}");
    let software = self
      .shared
      .state
      .lock()
      .hosts
      .get(name)
      .and_then(|host| host.software.clone())
      .unwrap_or_else(|| panic!("host {name} has no software to restart"));
    let host = self.host(name);
    host.spawn(software(host.clone()));
  }

  /// Drop every packet between `a` and `b` until [`repair`](Self::repair).
  pub fn partition(&self, a: &str, b: &str) {
    self.set_link(a, b, Link::Partitioned);
  }

  /// Undo [`partition`](Self::partition). Dropped packets stay lost until TCP retransmits them.
  pub fn repair(&self, a: &str, b: &str) {
    self.set_link(a, b, Link::Up);
  }

  /// Queue every packet between `a` and `b` until [`release`](Self::release).
  pub fn hold(&self, a: &str, b: &str) {
    self.set_link(a, b, Link::Held);
  }

  /// Undo [`hold`](Self::hold) and send the queued packets in order.
  pub fn release(&self, a: &str, b: &str) {
    self.set_link(a, b, Link::Up);
  }

  fn set_link(&self, a: &str, b: &str, link: Link) {
    trace!("sim link change: a={a}, b={b}, link={link:?}");
    {
      let mut state = self.shared.state.lock();
      let key = pair(state.ip(a), state.ip(b));
      if link == Link::Up {
        state.faults.remove(&key);
      } else {
        state.faults.insert(key, link);
      }
    }
    self.shared.wake.notify_one();
  }

  fn link(&self, a: IpAddr, b: IpAddr) -> Link {
    self
      .shared
      .state
      .lock()
      .faults
      .get(&pair(a, b))
      .copied()
      .unwrap_or(Link::Up)
  }

  /// Remove and return held packets whose link is no longer held, in the order they were sent.
  fn take_released(&self) -> Vec<Packet> {
    let mut state = self.shared.state.lock();
    let State { faults, held, .. } = &mut *state;
    let (released, still_held): (Vec<_>, Vec<_>) = std::mem::take(held)
      .into_iter()
      .partition(|packet| faults.get(&pair(packet.src, packet.dst)) != Some(&Link::Held));
    *held = still_held;
    released
  }

  /// Whether any connection has data or a handshake that TCP may need to retransmit.
  fn has_unacknowledged_data(&self) -> bool {
    self.shared.state.lock().hosts.values().any(|host| {
      let ip = host.ip;
      turmoil_net::netstat(ip).entries.iter().any(|entry| {
        entry.proto == Proto::Tcp
          && match entry.state {
            Some(
              NetstatState::SynSent
              | NetstatState::SynReceived
              | NetstatState::FinWait1
              | NetstatState::Closing
              | NetstatState::LastAck,
            ) => true,
            Some(NetstatState::Established | NetstatState::CloseWait) => entry.send_q > 0,
            _ => false,
          }
      })
    })
  }
}

impl State {
  fn ip(&self, name: &str) -> IpAddr {
    self
      .hosts
      .get(name)
      .unwrap_or_else(|| panic!("host {name} is not registered"))
      .ip
  }
}

//
// SimHost
//

/// A handle to one host: its sockets and the tasks a crash aborts.
#[derive(Clone)]
pub struct SimHost {
  name: Arc<str>,
  id: HostId,
  control: SimControl,
}

impl std::fmt::Debug for SimHost {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("SimHost")
      .field("name", &self.name)
      .finish_non_exhaustive()
  }
}

impl SimHost {
  #[must_use]
  pub fn name(&self) -> &str {
    &self.name
  }

  /// Spawn a task that belongs to this host, so a crash of the host aborts it.
  pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
  where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
  {
    let task = tokio::spawn(future);
    let mut state = self.control.shared.state.lock();
    let host = state.hosts.get_mut(&*self.name).expect("registered host");
    host.tasks.retain(|task| !task.is_finished());
    host.tasks.push(task.abort_handle());
    task
  }

  /// Open a TCP connection from this host to `host:port`.
  pub async fn connect(&self, host: &str, port: u16) -> io::Result<SimStream> {
    let ip = turmoil_net::lookup_host(host)
      .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("unknown host {host}")))?;
    let connect = Scoped {
      host: self.clone(),
      inner: Box::pin(TcpStream::connect((ip, port))),
    };
    let stream = connect.await?;
    Ok(SimStream {
      host: self.clone(),
      inner: Some(stream),
    })
  }

  /// Listen for TCP connections on `port` of this host.
  pub async fn bind(&self, port: u16) -> io::Result<SimListener> {
    let bind = Scoped {
      host: self.clone(),
      inner: Box::pin(TcpListener::bind((IpAddr::from([0, 0, 0, 0]), port))),
    };
    let listener = bind.await?;
    Ok(SimListener {
      host: self.clone(),
      inner: Some(listener),
    })
  }

  /// Point turmoil-net's socket calls at this host and wake the router afterwards.
  fn enter<R>(&self, f: impl FnOnce() -> R) -> R {
    turmoil_net::set_current(self.id);
    let result = f();
    self.control.shared.wake.notify_one();
    result
  }
}

/// Polls a socket future with its host current.
struct Scoped<F> {
  host: SimHost,
  inner: Pin<Box<F>>,
}

impl<F: Future> Future for Scoped<F> {
  type Output = F::Output;

  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
    let this = &mut *self;
    this.host.enter(|| this.inner.as_mut().poll(cx))
  }
}

//
// SimStream
//

/// A TCP connection that belongs to one host.
pub struct SimStream {
  host: SimHost,
  inner: Option<TcpStream>,
}

impl SimStream {
  fn inner(&mut self) -> Pin<&mut TcpStream> {
    Pin::new(self.inner.as_mut().expect("stream is open until drop"))
  }
}

impl AsyncRead for SimStream {
  fn poll_read(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>,
  ) -> Poll<io::Result<()>> {
    let host = self.host.clone();
    host.enter(|| self.inner().poll_read(cx, buf))
  }
}

impl AsyncWrite for SimStream {
  fn poll_write(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<io::Result<usize>> {
    let host = self.host.clone();
    host.enter(|| self.inner().poll_write(cx, buf))
  }

  fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    let host = self.host.clone();
    host.enter(|| self.inner().poll_flush(cx))
  }

  fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    let host = self.host.clone();
    host.enter(|| self.inner().poll_shutdown(cx))
  }
}

impl Drop for SimStream {
  fn drop(&mut self) {
    let inner = self.inner.take();
    self.host.enter(|| drop(inner));
  }
}

//
// SimListener
//

/// A TCP listener that belongs to one host.
pub struct SimListener {
  host: SimHost,
  inner: Option<TcpListener>,
}

impl SimListener {
  /// Wait for the next connection.
  pub async fn accept(&mut self) -> io::Result<SimStream> {
    let host = self.host.clone();
    let listener = self.inner.as_ref().expect("listener is open until drop");
    let stream = std::future::poll_fn(|cx| {
      host.enter(|| listener.poll_accept(cx).map_ok(|(stream, _peer)| stream))
    })
    .await?;
    Ok(SimStream {
      host,
      inner: Some(stream),
    })
  }
}

impl Drop for SimListener {
  fn drop(&mut self) {
    let inner = self.inner.take();
    self.host.enter(|| drop(inner));
  }
}
