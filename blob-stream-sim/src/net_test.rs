use super::SimNet;
use std::io::ErrorKind;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PORT: u16 = 1;

#[tokio::test(start_paused = true)]
async fn delivers_bytes_in_both_directions_after_the_latency() {
  let net = SimNet::new();
  net.set_latency(Duration::from_millis(7));
  let mut listener = net.bind("server", PORT);
  let started = tokio::time::Instant::now();

  let mut client = net.connect("client", "server", PORT).await.unwrap();
  let mut server = listener.accept().await.unwrap();
  client.write_all(b"ping").await.unwrap();
  let mut request = [0; 4];
  server.read_exact(&mut request).await.unwrap();
  server.write_all(b"pong").await.unwrap();
  let mut response = [0; 4];
  client.read_exact(&mut response).await.unwrap();

  assert_eq!(&request, b"ping");
  assert_eq!(&response, b"pong");
  assert_eq!(started.elapsed(), Duration::from_millis(14));
}

#[tokio::test(start_paused = true)]
async fn partition_refuses_connections_and_drops_traffic_until_repair() {
  let net = SimNet::new();
  let mut listener = net.bind("server", PORT);
  let mut client = net.connect("client", "server", PORT).await.unwrap();
  let mut server = listener.accept().await.unwrap();

  net.partition("server", "client");
  let refused = net.connect("client", "server", PORT).await.unwrap_err();
  assert_eq!(refused.kind(), ErrorKind::ConnectionRefused);
  client.write_all(b"lost").await.unwrap();
  let mut byte = [0; 1];
  // Nothing arrives while partitioned; the paused clock skips the whole second.
  assert!(
    tokio::time::timeout(Duration::from_secs(1), server.read(&mut byte))
      .await
      .is_err()
  );

  net.repair("client", "server");
  client.write_all(b"x").await.unwrap();
  server.read_exact(&mut byte).await.unwrap();
  assert_eq!(&byte, b"x");
}

#[tokio::test(start_paused = true)]
async fn hold_buffers_traffic_and_connections_until_release() {
  let net = SimNet::new();
  let mut listener = net.bind("server", PORT);
  let mut client = net.connect("client", "server", PORT).await.unwrap();
  let mut server = listener.accept().await.unwrap();

  net.hold("client", "server");
  client.write_all(b"held").await.unwrap();
  let held_net = net.clone();
  let pending_connect =
    tokio::spawn(async move { held_net.connect("client", "server", PORT).await.is_ok() });
  tokio::time::sleep(Duration::from_secs(30)).await;
  assert!(!pending_connect.is_finished());

  net.release("client", "server");
  let mut buffer = [0; 4];
  server.read_exact(&mut buffer).await.unwrap();
  assert_eq!(&buffer, b"held");
  assert!(pending_connect.await.unwrap());
}

#[tokio::test(start_paused = true)]
async fn connecting_to_an_unbound_port_is_refused() {
  let net = SimNet::new();
  let refused = net.connect("client", "server", PORT).await.unwrap_err();
  assert_eq!(refused.kind(), ErrorKind::ConnectionRefused);
}
