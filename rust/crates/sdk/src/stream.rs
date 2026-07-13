mod dedup;
mod establish_connection;
mod monitor_connection;

use dedup::FeedDeduplicator;
use establish_connection::connect;
use monitor_connection::run_stream;

use crate::auth::generate_auth_headers;
use crate::config::{Config, WebSocketHighAvailability};
use crate::endpoints::get_cll_avail_origins_header;

use chainlink_data_streams_report::feed_id::ID;
use chainlink_data_streams_report::report::Report;

use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc, Mutex},
    time::{sleep, Duration},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream as TungsteniteWebSocketStream};
use tracing::{debug, info, warn};

pub const DEFAULT_WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
pub const MIN_WS_RECONNECT_INTERVAL: Duration = Duration::from_millis(1000);
pub const MAX_WS_RECONNECT_INTERVAL: Duration = Duration::from_millis(10000);

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("WebSocket error: {0}")]
    WebSocketError(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Authentication error: {0}")]
    AuthError(#[from] crate::auth::HmacError),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Stream closed")]
    StreamClosed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebSocketReport {
    pub report: Report,
}

struct Stats {
    /// Total number of accepted reports
    accepted: AtomicUsize,
    /// Total number of deduplicated reports when in HA           
    deduplicated: AtomicUsize,
    /// Total number of out-of-order reports seen
    out_of_order: AtomicUsize,
    /// Total number of partial reconnects when in HA        
    partial_reconnects: AtomicUsize,
    /// Total number of full reconnects    
    full_reconnects: AtomicUsize,
    /// Number of configured connections if in HA      
    configured_connections: AtomicUsize,
    /// Current number of active connections     
    active_connections: AtomicUsize,
}

#[derive(Debug)]
pub enum WebSocketConnection {
    Single(TungsteniteWebSocketStream<MaybeTlsStream<TcpStream>>),
    Multiple(Vec<(TungsteniteWebSocketStream<MaybeTlsStream<TcpStream>>, String)>),
}

/// Stream represents a realtime report stream.
/// Safe for concurrent usage.
/// When HA mode is enabled and at least 2 origins are provided, the Stream will maintain at least 2 concurrent connections to different instances
/// to ensure high availability, fault tolerance and minimize the risk of report gaps.
pub struct Stream {
    config: Config,
    feed_ids: Vec<ID>,
    conn: Option<WebSocketConnection>,
    report_sender: mpsc::Sender<WebSocketReport>,
    report_receiver: mpsc::Receiver<WebSocketReport>,
    shutdown_sender: broadcast::Sender<()>,
    stats: Arc<Stats>,
    dedup: Arc<Mutex<FeedDeduplicator>>,
}

impl Stream {
    /// Establishes a streaming WebSocket connection that sends reports for the given feedID(s) after they are verified.
    ///
    /// # Arguments
    ///
    /// * `config` - A validated `Config` instance.
    /// * `feedIDs` - A comma-separated list of Data Streams feed IDs.
    ///
    /// # Endpoint:
    /// ```bash
    /// /api/v1/ws
    /// ```
    ///
    /// # Type:
    /// * WebSocket
    ///
    /// # Sample Request:
    /// ```bash
    /// GET /api/v1/ws?feedIDs=<feedID1>,<feedID2>,...
    /// ```
    ///
    /// # Sample Response:
    /// ```json
    /// {
    ///     "report": {
    ///         "feedID": "Hex encoded feedId.",
    ///         "fullReport": "A blob containing the report context and body. Encode the fee token into the payload before passing it to the contract for verification.",
    ///         "validFromTimestamp": "Report's earliest applicable timestamp (in seconds).",
    ///         "observationsTimestamp": "Report's latest applicable timestamp (in seconds)."
    ///     }
    /// }
    /// ```
    ///
    /// # Error Response Codes
    ///
    /// | Status Code | Description |
    /// |-------------|-------------|
    /// | **400 Bad Request** | This error is triggered when:<br>- There is any missing/malformed query argument.<br>- Required headers are missing or provided with incorrect values. |
    /// | **401 Unauthorized User** | This error is triggered when:<br>- Authentication fails, typically because the HMAC signature provided by the client doesn't match the one expected by the server.<br>- A user requests access to a feed without the appropriate permission or that does not exist. |
    /// | **500 Internal Server** | Indicates an unexpected condition encountered by the server, preventing it from fulfilling the request. This error typically points to issues on the server side. |
    pub async fn new(config: &Config, feed_ids: Vec<ID>) -> Result<Stream, StreamError> {
        let (report_sender, report_receiver) = mpsc::channel(100);
        let (shutdown_sender, _) = broadcast::channel(1);

        let stats = Arc::new(Stats {
            accepted: AtomicUsize::new(0),
            deduplicated: AtomicUsize::new(0),
            out_of_order: AtomicUsize::new(0),
            partial_reconnects: AtomicUsize::new(0),
            full_reconnects: AtomicUsize::new(0),
            configured_connections: AtomicUsize::new(0),
            active_connections: AtomicUsize::new(0),
        });

        let origins: Vec<String> = if config.ws_ha == WebSocketHighAvailability::Enabled {
            match fetch_ha_origins(config).await {
                Ok(o) if !o.is_empty() => {
                    info!("HA mode: discovered {} origins", o.len());
                    o
                }
                Ok(_) => {
                    warn!("HA mode: no origins returned from HEAD request, degrading to single connection");
                    vec![]
                }
                Err(e) => {
                    warn!("HA mode: origin discovery failed ({}), degrading to single connection", e);
                    vec![]
                }
            }
        } else {
            vec![]
        };

        let conn = connect(config, &origins, &feed_ids, stats.clone()).await?;

        let dedup = Arc::new(Mutex::new(FeedDeduplicator::new()));

        Ok(Stream {
            config: config.clone(),
            feed_ids,
            conn: Some(conn),
            report_sender,
            report_receiver,
            shutdown_sender,
            stats,
            dedup,
        })
    }

    /// Starts listening for reports on the Stream.
    /// This method will spawn a new task for each WebSocket connection.
    pub async fn listen(&mut self) -> Result<(), StreamError> {
        let conn = self
            .conn
            .take()
            .ok_or_else(|| StreamError::ConnectionError("No connection".into()))?;

        match conn {
            WebSocketConnection::Single(stream) => {
                let report_sender = self.report_sender.clone();
                let shutdown_receiver = self.shutdown_sender.subscribe();
                let stats = self.stats.clone();
                let dedup = self.dedup.clone();
                let config = self.config.clone();
                let feed_ids = self.feed_ids.clone();

                tokio::spawn(run_stream(
                    stream,
                    String::new(), // no X-Cll-Origin header for non-HA connections
                    report_sender,
                    shutdown_receiver,
                    stats,
                    dedup,
                    config,
                    feed_ids,
                ));
            }
            WebSocketConnection::Multiple(streams) => {
                for (stream, origin) in streams {
                    let report_sender = self.report_sender.clone();
                    let shutdown_receiver = self.shutdown_sender.subscribe();
                    let stats = self.stats.clone();
                    let dedup = self.dedup.clone();
                    let config = self.config.clone();
                    let feed_ids = self.feed_ids.clone();

                    tokio::spawn(run_stream(
                        stream,
                        origin,
                        report_sender,
                        shutdown_receiver,
                        stats,
                        dedup,
                        config,
                        feed_ids,
                    ));
                }
            }
        }

        Ok(())
    }

    /// Reads the next available report on the Stream.
    /// Reads blocks until a report is received, the context is canceled or all underlying connections are in a error state.
    ///
    /// # Returns
    ///
    /// * `WebSocketReport` - The next available report.
    pub async fn read(&mut self) -> Result<WebSocketReport, StreamError> {
        self.report_receiver
            .recv()
            .await
            .ok_or(StreamError::StreamClosed)
    }

    /// Closes the Stream.
    /// It is the caller's responsibility to call close when the stream is no longer needed.
    pub async fn close(&mut self) -> Result<(), StreamError> {
        info!("Closing stream...");

        // Send shutdown signal
        if let Err(e) = self.shutdown_sender.send(()) {
            debug!("Shutdown signal not sent (no active receivers). Stream may already be closed. Error received: {:?}", e);
        }

        // Allow tasks to shut down gracefully
        sleep(Duration::from_millis(100)).await;

        Ok(())
    }

    /// Returns basic stats about the Stream.
    ///
    /// # Returns
    ///
    /// * `StatsSnapshot` - A snapshot of the current Stream statistics.
    ///     * `accepted` - Total number of accepted reports.
    ///     * `deduplicated` - Total number of deduplicated reports when in HA.
    ///     * `total_received` - Total number of received reports.
    ///     * `partial_reconnects` - Total number of partial reconnects when in HA.
    ///     * `full_reconnects` - Total number of full reconnects.
    ///     * `configured_connections` - Number of configured connections if in HA.
    ///     * `active_connections` - Current number of active connections.
    pub fn get_stats(&self) -> StatsSnapshot {
        let accepted = self.stats.accepted.load(Ordering::SeqCst);
        let deduplicated = self.stats.deduplicated.load(Ordering::SeqCst);

        StatsSnapshot {
            accepted,
            deduplicated,
            out_of_order: self.stats.out_of_order.load(Ordering::SeqCst),
            total_received: accepted + deduplicated,
            partial_reconnects: self.stats.partial_reconnects.load(Ordering::SeqCst),
            full_reconnects: self.stats.full_reconnects.load(Ordering::SeqCst),
            configured_connections: self.stats.configured_connections.load(Ordering::SeqCst),
            active_connections: self.stats.active_connections.load(Ordering::SeqCst),
        }
    }
}

/// Snapshot of statistics for external consumption.
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    /// Total number of accepted reports
    pub accepted: usize,
    /// Total number of deduplicated reports when in HA
    pub deduplicated: usize,
    /// Total number of out-of-order reports seen
    pub out_of_order: usize,
    /// Total number of received reports
    pub total_received: usize,
    /// Total number of partial reconnects when in HA
    pub partial_reconnects: usize,
    /// Total number of full reconnects
    pub full_reconnects: usize,
    /// Number of configured connections if in HA
    pub configured_connections: usize,
    /// Current number of active connections
    pub active_connections: usize,
}

fn parse_origins_from_header(header_value: &str) -> Vec<String> {
    let inner = header_value
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(header_value);
    if inner.is_empty() {
        return vec![];
    }
    inner
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn convert_ws_to_http_scheme(ws_url: &str) -> String {
    if let Some(rest) = ws_url.strip_prefix("wss://") {
        format!("https://{}", rest)
    } else if let Some(rest) = ws_url.strip_prefix("ws://") {
        format!("http://{}", rest)
    } else {
        ws_url.to_string()
    }
}

async fn fetch_ha_origins(config: &Config) -> Result<Vec<String>, StreamError> {
    let http = HttpClient::builder()
        .danger_accept_invalid_certs(config.insecure_skip_verify.to_bool())
        .build()
        .map_err(|e| StreamError::ConnectionError(e.to_string()))?;

    // Parse URL, normalize path to "/", keep scheme+host+port so the HMAC-signed
    // path "/" matches the actual request path even when ws_url carries a subpath.
    let http_url = {
        let mut u = reqwest::Url::parse(&convert_ws_to_http_scheme(&config.ws_url))
            .map_err(|e| StreamError::ConnectionError(format!("Invalid ws_url: {}", e)))?;
        u.set_path("/");
        u.set_query(None);
        u.to_string()
    };

    let request_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time error")
        .as_millis();

    let auth_headers = generate_auth_headers(
        "HEAD",
        "/",
        b"",
        &config.api_key,
        &config.api_secret,
        request_timestamp,
    )?;

    let response = http
        .head(&http_url)
        .headers(auth_headers)
        .send()
        .await
        .map_err(|e| StreamError::ConnectionError(format!("HA origin discovery request failed: {}", e)))?;

    if !response.status().is_success() {
        return Err(StreamError::ConnectionError(format!(
            "HA origin discovery HEAD request returned status {}",
            response.status()
        )));
    }

    let header_value = response
        .headers()
        .get(get_cll_avail_origins_header())
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    Ok(parse_origins_from_header(&header_value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_origins_from_header_empty() {
        assert_eq!(parse_origins_from_header(""), Vec::<String>::new());
    }

    #[test]
    fn test_parse_origins_from_header_with_braces() {
        let result = parse_origins_from_header("{001,002}");
        assert_eq!(result, vec!["001".to_string(), "002".to_string()]);
    }

    #[test]
    fn test_parse_origins_from_header_without_braces() {
        let result = parse_origins_from_header("001,002");
        assert_eq!(result, vec!["001".to_string(), "002".to_string()]);
    }

    #[test]
    fn test_parse_origins_from_header_single_origin() {
        let result = parse_origins_from_header("{001}");
        assert_eq!(result, vec!["001".to_string()]);
    }

    #[test]
    fn test_parse_origins_from_header_empty_braces() {
        assert_eq!(parse_origins_from_header("{}"), Vec::<String>::new());
    }

    #[test]
    fn test_convert_ws_scheme_wss() {
        assert_eq!(
            convert_ws_to_http_scheme("wss://ws.dataengine.chain.link"),
            "https://ws.dataengine.chain.link"
        );
    }

    #[test]
    fn test_convert_ws_scheme_ws() {
        assert_eq!(
            convert_ws_to_http_scheme("ws://127.0.0.1:8080"),
            "http://127.0.0.1:8080"
        );
    }

    #[test]
    fn test_convert_ws_scheme_passthrough() {
        assert_eq!(
            convert_ws_to_http_scheme("https://already.https.com"),
            "https://already.https.com"
        );
    }
}
