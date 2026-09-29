use super::{HostSoftware, Sim, SimControl};
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PORT: u16 = 7;
const LATENCY: Duration = Duration::from_millis(5);

fn sim() -> Sim {
  Sim::new(LATENCY).host("client").host("server")
}

/// Echo every byte back on each accepted connection.
fn echo_server() -> HostSoftware {
  Arc::new(|host| {
    Box::pin(async move {
      let mut listener = host.bind(PORT).await.expect("bind");
      while let Ok(mut stream) = listener.accept().await {
        host.spawn(async move {
          let mut buffer = [0; 64];
          while let Ok(read) = stream.read(&mut buffer).await {
            if read == 0 || stream.write_all(&buffer[.. read]).await.is_err() {
              break;
            }
          }
        });
      }
    })
  })
}

async fn round_trip(control: &SimControl, bytes: &[u8]) -> std::io::Result<Vec<u8>> {
  let mut stream = control.host("client").connect("server", PORT).await?;
  stream.write_all(bytes).await?;
  let mut echoed = vec![0; bytes.len()];
  stream.read_exact(&mut echoed).await?;
  Ok(echoed)
}

#[test]
fn delivers_bytes_after_the_link_latency() {
  sim().run(|control| async move {
    control.start("server", echo_server());
    let started = tokio::time::Instant::now();
    assert_eq!(round_trip(&control, b"ping").await.unwrap(), b"ping");
    // The handshake and the echo each cross the link twice.
    assert_eq!(started.elapsed(), 4 * LATENCY);
  });
}

#[test]
fn partition_stalls_connections_until_repair_and_tcp_retransmits() {
  sim().run(|control| async move {
    control.start("server", echo_server());
    let client = control.host("client");
    let mut stream = client.connect("server", PORT).await.unwrap();

    control.partition("client", "server");
    let connect = tokio::time::timeout(Duration::from_secs(10), client.connect("server", PORT));
    assert!(
      connect.await.is_err(),
      "a partitioned connect must time out"
    );
    stream.write_all(b"late").await.unwrap();
    let mut echoed = [0; 4];
    let read = tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut echoed));
    assert!(read.await.is_err(), "nothing crosses a partition");

    control.repair("server", "client");
    stream.read_exact(&mut echoed).await.unwrap();
    assert_eq!(&echoed, b"late");
  });
}

#[test]
fn hold_delays_traffic_until_release() {
  sim().run(|control| async move {
    control.start("server", echo_server());
    control.hold("client", "server");
    let pending = tokio::spawn({
      let control = control.clone();
      async move { round_trip(&control, b"held").await }
    });
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert!(!pending.is_finished());

    control.release("client", "server");
    let released = tokio::time::Instant::now();
    assert_eq!(pending.await.unwrap().unwrap(), b"held");
    // The held SYN leaves at release, so the exchange takes exactly as long as an unheld one.
    assert_eq!(released.elapsed(), 4 * LATENCY);
  });
}

#[test]
fn crashed_host_closes_its_connections_and_refuses_new_ones_until_restart() {
  sim().run(|control| async move {
    control.start("server", echo_server());
    let mut stream = control
      .host("client")
      .connect("server", PORT)
      .await
      .unwrap();

    control.crash("server");
    let mut buffer = [0; 1];
    let read = stream.read(&mut buffer).await;
    assert!(
      matches!(read, Ok(0)) || read.is_err(),
      "the crashed server's socket must close: {read:?}"
    );
    let refused = round_trip(&control, b"x").await.unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::ConnectionRefused);

    control.restart("server");
    assert_eq!(round_trip(&control, b"back").await.unwrap(), b"back");
  });
}

#[test]
fn idle_time_is_skipped() {
  let wall = std::time::Instant::now();
  sim().run(|control| async move {
    control.start("server", echo_server());
    tokio::time::sleep(Duration::from_secs(3600)).await;
    assert_eq!(round_trip(&control, b"awake").await.unwrap(), b"awake");
  });
  assert!(
    wall.elapsed() < Duration::from_secs(5),
    "{:?}",
    wall.elapsed()
  );
}
