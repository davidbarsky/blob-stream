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
// TurmoilHttpClient
//

/// AWS SDK HTTP client that opens one HTTP/1.1 connection per request over turmoil's simulated
/// TCP.
///
/// The client applies the SDK's connect and read timeouts with tokio timers, which advance with
/// turmoil's simulated clock. It opens a fresh connection per attempt so a partition, hold, or
/// host crash affects exactly the attempts that start or are in flight while it is active.
#[derive(Clone, Debug, Default)]
pub struct TurmoilHttpClient;

impl HttpClient for TurmoilHttpClient {
  fn http_connector(
    &self,
    settings: &HttpConnectorSettings,
    _components: &RuntimeComponents,
  ) -> SharedHttpConnector {
    SharedHttpConnector::new(TurmoilConnector {
      connect_timeout: settings.connect_timeout(),
      read_timeout: settings.read_timeout(),
    })
  }
}

#[derive(Debug)]
struct TurmoilConnector {
  connect_timeout: Option<Duration>,
  read_timeout: Option<Duration>,
}

impl HttpConnector for TurmoilConnector {
  fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
    let connect_timeout = self.connect_timeout;
    let read_timeout = self.read_timeout;
    HttpConnectorFuture::new(send(request, connect_timeout, read_timeout))
  }
}

async fn send(
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
  let connect = turmoil::net::TcpStream::connect((host.as_str(), port));
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
  tokio::spawn(async move {
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
