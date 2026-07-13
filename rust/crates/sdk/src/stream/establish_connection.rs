use super::{Stats, StreamError, WebSocketConnection};

use crate::{
    auth::generate_auth_headers,
    config::{Config, WebSocketHighAvailability},
    endpoints::{get_cll_origin_header, API_V1_WS},
    stream::{DEFAULT_WS_CONNECT_TIMEOUT, MAX_WS_RECONNECT_INTERVAL, MIN_WS_RECONNECT_INTERVAL},
};

use chainlink_data_streams_report::feed_id::ID;

use std::{
    sync::{atomic::Ordering, Arc},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::TcpStream,
    time::{sleep, timeout},
};
use tokio_tungstenite::{
    connect_async, tungstenite::client::IntoClientRequest, MaybeTlsStream,
    WebSocketStream as TungsteniteWebSocketStream,
};
use tracing::{error, info};

async fn connect_to_origin(
    config: &Config,
    cll_origin: &str, // X-Cll-Origin header value; empty string = no header
    feed_ids: &[ID],
) -> Result<TungsteniteWebSocketStream<MaybeTlsStream<TcpStream>>, StreamError> {
    let feed_ids_str: Vec<String> = feed_ids.iter().map(|id| id.to_hex_string()).collect();
    let feed_ids_joined = feed_ids_str.join(",");

    let method = "GET";
    let path = format!("{}?feedIDs={}", API_V1_WS, feed_ids_joined.as_str());
    let body = b"";
    let client_id = &config.api_key;
    let user_secret = &config.api_secret;
    let request_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time error")
        .as_millis();

    let mut headers = generate_auth_headers(
        method,
        &path,
        body,
        client_id,
        user_secret,
        request_timestamp,
    )?;

    if !cll_origin.is_empty() {
        headers.insert(
            get_cll_origin_header(),
            reqwest::header::HeaderValue::from_str(cll_origin).map_err(|e| {
                StreamError::ConnectionError(format!("Invalid X-Cll-Origin header value: {}", e))
            })?,
        );
    }

    // Always connect to config.ws_url — cll_origin is a routing hint header, not a URL
    let url = format!("{}{}", config.ws_url, path);
    let mut request = url.into_client_request().map_err(|e| {
        StreamError::ConnectionError(format!("Failed to create client request: {}", e))
    })?;
    request.headers_mut().extend(headers);

    let connect_future = connect_async(request);

    let (ws_stream, ws_response) = timeout(DEFAULT_WS_CONNECT_TIMEOUT, connect_future)
        .await
        .map_err(|_| StreamError::ConnectionError("WebSocket connection timed out".to_string()))?
        .map_err(|e| StreamError::ConnectionError(format!("Failed to connect: {}", e)))?;

    info!("Connected to WebSocket: {:#?}", ws_response);

    Ok(ws_stream)
}

pub(crate) async fn connect(
    config: &Config,
    origins: &[String], // empty = single non-HA connection; populated = HA mode
    feed_ids: &[ID],
    stats: Arc<Stats>,
) -> Result<WebSocketConnection, StreamError> {
    if config.ws_ha == WebSocketHighAvailability::Enabled && origins.len() == 1 {
        info!("HA mode enabled but only 1 origin discovered; connection will not be redundant");
    }

    if config.ws_ha == WebSocketHighAvailability::Enabled && !origins.is_empty() {
        let mut streams = Vec::new();

        for origin in origins {
            match connect_to_origin(config, origin, feed_ids).await {
                Ok(stream) => {
                    streams.push((stream, origin.clone()));
                    stats.configured_connections.fetch_add(1, Ordering::SeqCst);
                    stats.active_connections.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => {
                    error!("Failed to connect to origin {}: {:?}", origin, e);
                }
            }
        }

        if streams.is_empty() {
            return Err(StreamError::ConnectionError(
                "Failed to connect to any WebSocket origins in HA mode".into(),
            ));
        }

        Ok(WebSocketConnection::Multiple(streams))
    } else {
        let stream = connect_to_origin(config, "", feed_ids).await?;
        stats.configured_connections.fetch_add(1, Ordering::SeqCst);
        stats.active_connections.fetch_add(1, Ordering::SeqCst);
        Ok(WebSocketConnection::Single(stream))
    }
}

pub(crate) async fn try_to_reconnect(
    stats: Arc<Stats>,
    config: &Config,
    origin: &str, // the X-Cll-Origin header value for this connection (empty = non-HA)
    feed_ids: &[ID],
) -> Result<TungsteniteWebSocketStream<MaybeTlsStream<TcpStream>>, StreamError> {
    let mut reconnect_attempts = 0;
    let max_reconnect_attempts = config.ws_max_reconnect;
    let mut backoff = MIN_WS_RECONNECT_INTERVAL;

    loop {
        info!("Attempting to reconnect (origin: {})", origin);
        reconnect_attempts += 1;
        match connect_to_origin(config, origin, feed_ids).await {
            Ok(new_stream) => {
                stats.active_connections.fetch_add(1, Ordering::SeqCst);
                return Ok(new_stream);
            }
            Err(e) => {
                error!(
                    "Reconnection attempt {} failed: {:?}.",
                    reconnect_attempts, e
                );

                if reconnect_attempts >= max_reconnect_attempts {
                    error!("Max reconnect attempts reached. Exiting.");
                    return Err(StreamError::ConnectionError(
                        "Max reconnect attempts reached".to_string(),
                    ));
                }

                error!("Retrying in {:?}.", backoff);
                sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_WS_RECONNECT_INTERVAL);
            }
        }
    }
}
