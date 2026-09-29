use crate::SimHost;
use aws_smithy_runtime_api::client::http::{
  HttpClient,
  HttpConnector,
  HttpConnectorFuture,
  HttpConnectorSettings,
  SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use hyper_util::rt::TokioIo;
use log::{debug, trace};
use std::time::Duration;

//
// SimHttpClient
//

/// AWS SDK HTTP client that opens one HTTP/1.1 connection per request from a simulated host.
///
/// The client applies the SDK's connect and read timeouts with tokio timers, so they follow the
/// paused tokio clock. It opens a fresh connection per attempt so a fault affects exactly the
/// attempts that start or are in flight while it is active. Connection tasks belong to the host,
/// so crashing it closes them.
#[derive(Clone, Debug)]
pub struct SimHttpClient {
  host: SimHost,
}

impl SimHttpClient {
  /// A client whose connections originate from `host`.
  #[must_use]
  pub fn new(host: SimHost) -> Self {
    Self { host }
  }
}

impl HttpClient for SimHttpClient {
  fn http_connector(
    &self,
    settings: &HttpConnectorSettings,
    _components: &RuntimeComponents,
  ) -> SharedHttpConnector {
    SharedHttpConnector::new(SimConnector {
      client: self.clone(),
      connect_timeout: settings.connect_timeout(),
      read_timeout: settings.read_timeout(),
    })
  }
}

#[derive(Debug)]
struct SimConnector {
  client: SimHttpClient,
  connect_timeout: Option<Duration>,
  read_timeout: Option<Duration>,
}

impl HttpConnector for SimConnector {
  fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
    HttpConnectorFuture::new(send(
      self.client.clone(),
      request,
      self.connect_timeout,
      self.read_timeout,
    ))
  }
}

async fn send(
  client: SimHttpClient,
  request: HttpRequest,
  connect_timeout: Option<Duration>,
  read_timeout: Option<Duration>,
) -> Result<HttpResponse, ConnectorError> {
  let mut request = request
    .try_into_http1x()
    .map_err(|error| ConnectorError::other(error.into(), None))?;
  let uri = request.uri().clone();
  let host = uri
    .host()
    .ok_or_else(|| ConnectorError::other(format!("request URI has no host: {uri}").into(), None))?
    .to_string();
  let port = uri.port_u16().unwrap_or(80);

  // hyper's connection-level client sends the URI verbatim, so convert to origin-form and supply
  // the Host header that hyper's pooled client would otherwise add.
  if !request.headers().contains_key(http::header::HOST) {
    let authority = uri
      .authority()
      .map_or_else(|| host.clone(), ToString::to_string);
    let value = http::HeaderValue::from_str(&authority)
      .map_err(|error| ConnectorError::other(error.into(), None))?;
    request.headers_mut().insert(http::header::HOST, value);
  }
  let path_and_query = uri
    .path_and_query()
    .map_or_else(|| "/".to_string(), ToString::to_string);
  *request.uri_mut() = path_and_query
    .parse()
    .map_err(|error: http::uri::InvalidUri| ConnectorError::other(error.into(), None))?;

  trace!("sim http connect: host={host}, port={port}");
  let connect = client.host.connect(&host, port);
  let stream = match connect_timeout {
    Some(timeout) => tokio::time::timeout(timeout, connect)
      .await
      .map_err(|elapsed| ConnectorError::timeout(elapsed.into()))?,
    None => connect.await,
  }
  .map_err(|error| ConnectorError::io(error.into()))?;

  let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
    .await
    .map_err(|error| ConnectorError::io(error.into()))?;
  client.host.spawn(async move {
    if let Err(error) = connection.await {
      debug!("sim http connection ended with error: {error}");
    }
  });

  let response = sender.send_request(request);
  let response = match read_timeout {
    Some(timeout) => tokio::time::timeout(timeout, response)
      .await
      .map_err(|elapsed| ConnectorError::timeout(elapsed.into()))?,
    None => response.await,
  }
  .map_err(|error| ConnectorError::io(error.into()))?;

  HttpResponse::try_from(response.map(SdkBody::from_body_1_x))
    .map_err(|error| ConnectorError::other(error.into(), None))
}
