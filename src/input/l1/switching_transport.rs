//! An RPC client with multiple remote (HTTP) providers that can switch between them.

use super::options::L1ClientOptions;
use crate::Result;
use alloy::{
    rpc::json_rpc::{ErrorPayload, RequestPacket, ResponsePacket},
    transports::{HttpError, RpcError, TransportErrorKind, http::Http},
};
use futures::Future;
use parking_lot::RwLock;
use std::{
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use tide_disco::Url;
use tokio::sync::Notify;
use tower_service::Service;

/// An RPC client with multiple remote (HTTP) providers.
///
/// This client utilizes one RPC provider at a time, but if it detects that the provider is in a
/// failing state, it will automatically switch to the next provider in its list.
#[derive(Clone, Debug)]
pub struct SwitchingTransport {
    /// The transport currently being used by the client
    current_transport: Arc<RwLock<SingleTransport>>,
    opt: Arc<L1ClientOptions>,
    pub(super) switch_notify: Arc<Notify>,
}

/// The state of the current provider being used by a [`SwitchingTransport`].
/// This is cloneable and returns a reference to the same underlying data.
#[derive(Debug, Clone)]
pub(crate) struct SingleTransport {
    generation: usize,
    client: Http<alloy::transports::http::Client>,
    status: Arc<RwLock<SingleTransportStatus>>,
    /// Time at which to revert back to the primary provider after a failover.
    revert_at: Option<Instant>,
}

/// The status of a single transport
#[derive(Debug, Default)]
pub(crate) struct SingleTransportStatus {
    last_failure: Option<Instant>,
    consecutive_failures: usize,
    consecutive_rate_limits: usize,
    rate_limited_until: Option<Instant>,
    /// Whether or not this current transport is being shut down (switching to the next transport)
    shutting_down: bool,
}

impl SwitchingTransport {
    /// Create a new `SwitchingTransport` with the given options and URLs
    pub fn new(opt: L1ClientOptions) -> Result<Self> {
        // Return early if there were no URLs provided
        let Some(first_url) = opt.http_providers.first().cloned() else {
            return Err(crate::Error::internal().context("No valid URLs provided"));
        };

        // Create a new `SingleTransport` for the first URL
        let first_transport = Arc::new(RwLock::new(SingleTransport::new(&first_url, 0, None)));

        Ok(Self {
            current_transport: first_transport,
            opt: Arc::new(opt),
            switch_notify: Arc::new(Notify::new()),
        })
    }

    fn urls(&self) -> &[Url] {
        &self.opt.http_providers
    }

    fn switch_to(&self, next_gen: usize, current_transport: SingleTransport) -> SingleTransport {
        let next_index = next_gen % self.urls().len();
        let url = self.urls()[next_index].clone();
        tracing::info!(%url, next_gen, "switch L1 transport");

        let revert_at = if next_gen.is_multiple_of(self.urls().len()) {
            // If we are reverting to the primary transport, clear our scheduled revert time.
            None
        } else if current_transport
            .generation
            .is_multiple_of(self.urls().len())
        {
            // If we are failing over from the primary transport, schedule a time to automatically
            // revert back.
            Some(Instant::now() + self.opt.l1_failover_revert)
        } else {
            // Otherwise keep the currently scheduled revert time.
            current_transport.revert_at
        };

        // Create a new transport from the next URL and index
        let new_transport = SingleTransport::new(&url, next_gen, revert_at);

        // Switch to the next URL
        *self.current_transport.write() = new_transport.clone();

        // Notify the transport that it has been switched
        self.switch_notify.notify_waiters();

        new_transport
    }
}

impl SingleTransport {
    /// Create a new `SingleTransport` with the given URL
    fn new(url: &Url, generation: usize, revert_at: Option<Instant>) -> Self {
        Self {
            generation,
            client: Http::new(url.clone()),
            status: Default::default(),
            revert_at,
        }
    }
}

impl SingleTransportStatus {
    /// Log a successful call to the inner transport
    fn log_success(&mut self) {
        self.consecutive_failures = 0;
        self.consecutive_rate_limits = 0;
    }

    /// Log a rate limit, backing off until `until`. Returns whether or not the transport should
    /// be switched to the next URL.
    ///
    /// Only a rate limit arriving after the previous backoff elapsed counts towards
    /// [`MAX_CONSECUTIVE_RATE_LIMITS`]. Requests already in flight when a backoff started say
    /// nothing new about the provider.
    fn log_rate_limit(&mut self, until: Instant) -> bool {
        let outlived_backoff = self
            .rate_limited_until
            .is_none_or(|prev| Instant::now() >= prev);
        self.rate_limited_until = Some(until);
        if !outlived_backoff {
            return false;
        }

        self.consecutive_rate_limits += 1;
        if self.shutting_down || self.consecutive_rate_limits < MAX_CONSECUTIVE_RATE_LIMITS {
            return false;
        }
        self.shutting_down = true;
        true
    }

    /// Log a failure to call the inner transport. Returns whether or not the transport should be switched to the next URL
    fn log_failure(&mut self, opt: &L1ClientOptions) -> bool {
        // Increment the consecutive failures
        self.consecutive_failures += 1;

        // Check if we should switch to the next URL
        let should_switch = self.should_switch(opt);

        // Update the last failure time
        self.last_failure = Some(Instant::now());

        // Return whether or not we should switch
        should_switch
    }

    /// Whether or not the transport should be switched to the next URL
    fn should_switch(&mut self, opt: &L1ClientOptions) -> bool {
        // If someone else already beat us to switching, return false
        if self.shutting_down {
            return false;
        }

        // If we've reached the max number of consecutive failures, switch to the next URL
        if self.consecutive_failures >= opt.l1_consecutive_failure_tolerance {
            self.shutting_down = true;
            return true;
        }

        // If we've failed recently, switch to the next URL
        let now = Instant::now();
        if let Some(prev) = self.last_failure
            && now.saturating_duration_since(prev) < opt.l1_frequent_failure_tolerance
        {
            self.shutting_down = true;
            return true;
        }

        false
    }

    /// Whether or not the transport should be switched back to the primary URL.
    fn should_revert(&mut self, revert_at: Option<Instant>) -> bool {
        if self.shutting_down {
            // We have already switched away from this transport in another thread.
            return false;
        }
        let Some(revert_at) = revert_at else {
            return false;
        };
        if Instant::now() >= revert_at {
            self.shutting_down = true;
            return true;
        }

        false
    }
}

/// Provider health signal of a single transport response.
#[derive(Debug, PartialEq, Eq)]
enum ResponseOutcome {
    Success,
    /// Back off rather than fail over, up to [`MAX_CONSECUTIVE_RATE_LIMITS`].
    RateLimited {
        retry_after: Option<Duration>,
    },
    /// The request itself is over the provider's result cap. Every provider rejects it the same
    /// way, so it counts as neither success nor failure.
    RequestRejected,
    Failure,
}

/// Rate limits, each arriving after the previous backoff elapsed, before a provider is treated as
/// failed. Bounds how long the client stays on a provider whose quota is exhausted.
const MAX_CONSECUTIVE_RATE_LIMITS: usize = 2;

/// Cap on a server-requested `Retry-After`, matching alloy's `MAX_BACKOFF_HINT`.
const MAX_RATE_LIMIT_BACKOFF: Duration = Duration::from_secs(300);

type TransportResult = std::result::Result<ResponsePacket, RpcError<TransportErrorKind>>;

/// Whether the request asks for more results than the provider's cap.
fn is_request_rejection(err: &ErrorPayload) -> bool {
    err.message.contains("query returned more than")
}

/// Score a transport result for provider health.
///
/// alloy returns a non-2xx response whose body parses as a JSON-RPC error as `Ok`, dropping the
/// HTTP status and `Retry-After`, so the payload decides the outcome.
fn classify(result: &TransportResult) -> ResponseOutcome {
    match result {
        Ok(res) => match res.as_error() {
            None => ResponseOutcome::Success,
            // Checked before `is_retry_err`, which treats every `-32005` as a rate limit.
            Some(err) if is_request_rejection(err) => ResponseOutcome::RequestRejected,
            Some(err) if err.is_retry_err() => ResponseOutcome::RateLimited { retry_after: None },
            Some(_) => ResponseOutcome::Failure,
        },
        Err(RpcError::Transport(kind))
            if kind
                .as_http_error()
                .is_some_and(HttpError::is_rate_limit_err) =>
        {
            ResponseOutcome::RateLimited {
                retry_after: kind.retry_after(),
            }
        }
        Err(_) => ResponseOutcome::Failure,
    }
}

impl Service<RequestPacket> for SwitchingTransport {
    type Error = RpcError<TransportErrorKind>;
    type Response = ResponsePacket;
    type Future = Pin<Box<dyn Future<Output = TransportResult> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        // Just poll the (current) inner client
        self.current_transport.read().clone().client.poll_ready(cx)
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        // Clone ourselves
        let self_clone = self.clone();

        // Pin and box, which turns this into a future
        Box::pin(async move {
            // Clone the current transport
            let mut current_transport = self_clone.current_transport.read().clone();

            // Revert back to the primary transport if it's time.
            let should_revert = current_transport
                .status
                .write()
                .should_revert(current_transport.revert_at);
            if should_revert {
                // Switch to the next generation which maps to index 0.
                let n = self_clone.urls().len();
                // Rounding down to a multiple of n gives us the last generation of the primary transport.
                let prev_primary_gen = (current_transport.generation / n) * n;
                // Adding n jumps to the next generation.
                let next_gen = prev_primary_gen + n;
                current_transport = self_clone.switch_to(next_gen, current_transport);
            }

            // If we've been rate limited, back off until the limit (hopefully) expires.
            let rate_limit_until = current_transport.status.read().rate_limited_until;
            if rate_limit_until.is_some_and(|t| t > Instant::now()) {
                // Return an error with a non-standard code to indicate client-side rate limit.
                return Err(RpcError::Transport(TransportErrorKind::Custom(
                    "Rate limit exceeded".into(),
                )));
            }

            let result = current_transport.client.call(req).await;
            let error = result.as_ref().ok().and_then(ResponsePacket::as_error);
            let transport_err = result.as_ref().err();
            let should_switch = match classify(&result) {
                ResponseOutcome::Success => {
                    current_transport.status.write().log_success();
                    false
                }
                ResponseOutcome::RequestRejected => {
                    tracing::warn!(?error, "L1 rejected the request");
                    false
                }
                ResponseOutcome::RateLimited { retry_after } => {
                    let backoff = retry_after.map_or(self_clone.opt.rate_limit_delay(), |d| {
                        d.min(MAX_RATE_LIMIT_BACKOFF)
                    });
                    let mut status = current_transport.status.write();
                    let should_switch = status.log_rate_limit(Instant::now() + backoff);
                    if should_switch {
                        tracing::warn!(
                            consecutive = status.consecutive_rate_limits,
                            "L1 still rate limiting after its own backoff, failing over"
                        );
                    } else {
                        tracing::debug!(?backoff, "L1 rate limited");
                    }
                    should_switch
                }
                ResponseOutcome::Failure => {
                    tracing::warn!(?error, ?transport_err, "L1 client error");
                    current_transport
                        .status
                        .write()
                        .log_failure(&self_clone.opt)
                }
            };

            // Only one caller sees `true`, so this switches at most once per generation.
            if should_switch {
                tracing::info!("Switching to next L1 provider");
                self_clone.switch_to(current_transport.generation + 1, current_transport);
            }

            result
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use alloy::rpc::json_rpc::{Id, Request};
    use std::{
        net::Ipv4Addr,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use tokio::net::TcpListener;
    use warp::{Filter, http::StatusCode};

    const SUCCESS: &str = r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#;
    const RATE_LIMITED: &str =
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":429,"message":"Too Many Requests"}}"#;
    const RESULT_CAP: &str = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"query returned more than 10000 results. Try with this block range [0x1500000, 0x15000FA]."}}"#;

    fn packet(body: &str) -> ResponsePacket {
        serde_json::from_str(body).expect("valid JSON-RPC response fixture")
    }

    fn request() -> RequestPacket {
        Request::new("eth_blockNumber", Id::Number(1), ())
            .serialize()
            .unwrap()
            .into()
    }

    /// Serve `body` with `status` on a local port and count the requests it receives.
    async fn mock_provider(status: StatusCode, body: &'static str) -> (Url, Arc<AtomicUsize>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        let route = warp::any().map(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            warp::reply::with_status(
                warp::reply::with_header(body, "content-type", "application/json"),
                status,
            )
        });
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(warp::serve(route).incoming(listener).run());
        (format!("http://{addr}").parse().unwrap(), hits)
    }

    /// A transport whose primary provider answers with `status` and `body`, backed by a healthy
    /// second provider.
    async fn transport(
        status: StatusCode,
        body: &'static str,
        opt: L1ClientOptions,
    ) -> (SwitchingTransport, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let (primary, primary_hits) = mock_provider(status, body).await;
        let (backup, backup_hits) = mock_provider(StatusCode::OK, SUCCESS).await;
        let transport = SwitchingTransport::new(L1ClientOptions {
            http_providers: vec![primary, backup],
            ..opt
        })
        .unwrap();
        (transport, primary_hits, backup_hits)
    }

    fn generation(transport: &SwitchingTransport) -> usize {
        transport.current_transport.read().generation
    }

    #[test]
    fn classify_success_response() {
        assert_eq!(classify(&Ok(packet(SUCCESS))), ResponseOutcome::Success);
    }

    #[test]
    fn classify_error_body_as_failure() {
        let res = packet(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"App is inactive"}}"#,
        );
        assert_eq!(classify(&Ok(res)), ResponseOutcome::Failure);
    }

    #[test]
    fn classify_rate_limit_body() {
        assert_eq!(
            classify(&Ok(packet(RATE_LIMITED))),
            ResponseOutcome::RateLimited { retry_after: None }
        );
        let infura = packet(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"project ID request rate exceeded"}}"#,
        );
        assert_eq!(
            classify(&Ok(infura)),
            ResponseOutcome::RateLimited { retry_after: None }
        );
    }

    #[test]
    fn classify_result_cap_as_rejection() {
        assert_eq!(
            classify(&Ok(packet(RESULT_CAP))),
            ResponseOutcome::RequestRejected
        );
    }

    #[test]
    fn classify_non_json_429_with_retry_after() {
        let retry_after = Duration::from_secs(7);
        let err = TransportErrorKind::http_error_with_retry_after(
            429,
            "rate limited".into(),
            Some(retry_after),
        );
        assert_eq!(
            classify(&Err(err)),
            ResponseOutcome::RateLimited {
                retry_after: Some(retry_after)
            }
        );
    }

    #[tokio::test]
    async fn test_dead_key_fails_over() {
        let dead_key =
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"invalid API key"}}"#;
        let (mut transport, primary_hits, backup_hits) =
            transport(StatusCode::UNAUTHORIZED, dead_key, Default::default()).await;

        // The second failure within `l1_frequent_failure_tolerance` triggers the switch.
        for _ in 0..2 {
            transport.call(request()).await.unwrap();
        }
        assert_eq!(generation(&transport), 1);

        let res = transport.call(request()).await.unwrap();
        assert!(!res.is_error());
        assert_eq!(primary_hits.load(Ordering::SeqCst), 2);
        assert_eq!(backup_hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_rate_limit_backs_off() {
        let opt = L1ClientOptions {
            l1_rate_limit_delay: Some(Duration::from_secs(60)),
            ..Default::default()
        };
        let (mut transport, primary_hits, backup_hits) =
            transport(StatusCode::TOO_MANY_REQUESTS, RATE_LIMITED, opt).await;

        transport.call(request()).await.unwrap();
        let err = transport.call(request()).await.unwrap_err();
        assert!(err.to_string().contains("Rate limit exceeded"), "{err}");

        assert_eq!(generation(&transport), 0);
        assert_eq!(primary_hits.load(Ordering::SeqCst), 1);
        assert_eq!(backup_hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_repeated_rate_limits_fail_over() {
        let delay = Duration::from_millis(50);
        let opt = L1ClientOptions {
            l1_rate_limit_delay: Some(delay),
            ..Default::default()
        };
        let (mut transport, primary_hits, backup_hits) =
            transport(StatusCode::TOO_MANY_REQUESTS, RATE_LIMITED, opt).await;

        for _ in 0..MAX_CONSECUTIVE_RATE_LIMITS {
            transport.call(request()).await.unwrap();
            tokio::time::sleep(delay).await;
        }
        assert_eq!(generation(&transport), 1);

        let res = transport.call(request()).await.unwrap();
        assert!(!res.is_error());
        assert_eq!(
            primary_hits.load(Ordering::SeqCst),
            MAX_CONSECUTIVE_RATE_LIMITS
        );
        assert_eq!(backup_hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_request_rejection_does_not_fail_over() {
        let opt = L1ClientOptions {
            l1_consecutive_failure_tolerance: 1,
            ..Default::default()
        };
        let (mut transport, primary_hits, backup_hits) =
            transport(StatusCode::OK, RESULT_CAP, opt).await;

        for _ in 0..3 {
            let res = transport.call(request()).await.unwrap();
            assert!(res.is_error());
        }
        assert_eq!(generation(&transport), 0);
        assert_eq!(primary_hits.load(Ordering::SeqCst), 3);
        assert_eq!(backup_hits.load(Ordering::SeqCst), 0);
    }
}
