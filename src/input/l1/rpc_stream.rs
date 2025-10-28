use super::{BlockInput, ResettableStream};
use crate::{
    Result,
    types::common::{L1BlockId, Timestamp},
};
use alloy::{
    eips::BlockNumberOrTag,
    providers::{Provider, ProviderBuilder},
    transports::http::reqwest::Url as ReqwestUrl,
};
use futures::stream::{self, Stream, StreamExt};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tide_disco::Url;
use tokio::{
    sync::mpsc,
    time::{Duration, sleep},
};
use tracing::{debug, error, info, warn};

/// An L1 event stream based on a standard JSON-RPC server.
pub struct RpcStream {
    receiver: mpsc::UnboundedReceiver<BlockInput>,
    reset_tx: mpsc::UnboundedSender<u64>,
}

impl std::fmt::Debug for RpcStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcStream").finish_non_exhaustive()
    }
}

impl RpcStream {
    /// Construct a new [`RpcStream`].
    ///
    /// The stream will establish a connection to the given providers over HTTP and WebSockets,
    /// respectively. It is recommended to provide both, so that WebSockets can be used to listen
    /// for new L1 heads, and then HTTP can be used to download corresponding block data. This is
    /// the cheapest and most efficient way to use JSON-RPC.
    ///
    /// If multiple providers are given for either protocol or both, the system will rotate between
    /// them if and when one provider fails.
    pub async fn new(
        http_providers: impl IntoIterator<Item = &Url>,
        ws_providers: impl IntoIterator<Item = &Url>,
    ) -> Result<Self> {
        let http_providers: Vec<Url> = http_providers.into_iter().cloned().collect();
        let ws_providers: Vec<Url> = ws_providers.into_iter().cloned().collect();

        if http_providers.is_empty() {
            return Err(tide_disco::Error::catch_all(
                tide_disco::StatusCode::BAD_REQUEST,
                "at least one HTTP provider is required".to_string(),
            ));
        }

        info!(
            "Initializing RpcStream with {} HTTP and {} WebSocket providers",
            http_providers.len(),
            ws_providers.len()
        );

        let (block_tx, block_rx) = mpsc::unbounded_channel();
        let (reset_tx, reset_rx) = mpsc::unbounded_channel();

        tokio::spawn(stream_task(
            http_providers,
            ws_providers,
            block_tx,
            reset_rx,
        ));

        Ok(Self {
            receiver: block_rx,
            reset_tx,
        })
    }
}

impl Stream for RpcStream {
    type Item = BlockInput;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl ResettableStream for RpcStream {
    async fn reset(&mut self, block: u64) {
        info!("Resetting stream to block {block}");
        let _ = self.reset_tx.send(block);
    }
}

async fn stream_task(
    http_providers: Vec<Url>,
    ws_providers: Vec<Url>,
    block_tx: mpsc::UnboundedSender<BlockInput>,
    mut reset_rx: mpsc::UnboundedReceiver<u64>,
) {
    let mut http_index = 0;
    let mut ws_index = 0;
    let retry_delay = Duration::from_secs(1);
    let polling_interval = Duration::from_secs(1);

    loop {
        let http = loop {
            match connect_http(&http_providers[http_index]).await {
                Ok(http) => break http,
                Err(e) => {
                    error!("Failed to connect HTTP provider: {e}");
                    http_index = (http_index + 1) % http_providers.len();
                    sleep(retry_delay).await;
                }
            }
        };

        let mut block_stream = loop {
            match establish_block_stream(
                http.clone(),
                &ws_providers,
                &mut ws_index,
                polling_interval,
            )
            .await
            {
                Ok(stream) => {
                    info!("Block stream established");
                    break stream;
                }
                Err(e) => {
                    error!("Failed to establish block stream: {e}");
                    sleep(retry_delay).await;
                }
            }
        };

        loop {
            tokio::select! {
                Some(reset_to) = reset_rx.recv() => {
                    info!("Processing reset to block {reset_to}");
                    // TODO
                    break;
                }

                Some(header) = block_stream.next() => {
                    debug!("Received block header: {}", header.hash);

                    match process_header(&http, header).await {
                        Some(block_input) => {
                            if block_tx.send(block_input).is_err() {
                                return;
                            }
                        }
                        None => {
                            error!("Failed to process block header");
                        }
                    }
                }

                else => {
                    warn!("Block stream ended, reconnecting");
                    break;
                }
            }
        }
    }
}

type HttpProvider = Arc<Box<dyn Provider + Send + Sync>>;
type HeaderStream = Pin<Box<dyn Stream<Item = alloy::rpc::types::Header> + Send>>;

async fn connect_http(url: &Url) -> Result<HttpProvider> {
    info!("Connecting to HTTP provider: {url}");
    let reqwest_url: ReqwestUrl = url.to_string().parse().map_err(|e| {
        tide_disco::Error::catch_all(
            tide_disco::StatusCode::BAD_REQUEST,
            format!("invalid HTTP URL: {e}"),
        )
    })?;
    let provider = ProviderBuilder::new().connect_http(reqwest_url);
    Ok(Arc::new(Box::new(provider)))
}

async fn connect_ws(url: &Url) -> Result<HeaderStream> {
    info!("Connecting to WebSocket provider: {url}");

    let ws_provider = ProviderBuilder::new()
        .connect_ws(alloy::providers::WsConnect::new(&url.to_string()))
        .await
        .map_err(|e| {
            tide_disco::Error::catch_all(
                tide_disco::StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to connect to WebSocket: {e}"),
            )
        })?;

    let subscription = ws_provider.subscribe_blocks().await.map_err(|e| {
        tide_disco::Error::catch_all(
            tide_disco::StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to subscribe to blocks: {e}"),
        )
    })?;

    let stream = subscription.into_stream();
    Ok(Box::pin(stream))
}

async fn establish_block_stream(
    http: HttpProvider,
    ws_providers: &[Url],
    ws_index: &mut usize,
    polling_interval: Duration,
) -> Result<HeaderStream> {
    if !ws_providers.is_empty() {
        let start_index = *ws_index;
        loop {
            match connect_ws(&ws_providers[*ws_index]).await {
                Ok(stream) => {
                    info!("Established WebSocket block stream");
                    return Ok(stream);
                }
                Err(e) => {
                    warn!("Failed to connect WebSocket provider: {e}");
                    *ws_index = (*ws_index + 1) % ws_providers.len();

                    if *ws_index == start_index {
                        warn!("All WebSocket providers failed, falling back to HTTP");
                        break;
                    }
                }
            }
        }
    }

    info!("Setting up HTTP polling block stream");
    let poller = http.watch_blocks().await.map_err(|e| {
        tide_disco::Error::catch_all(
            tide_disco::StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to create block watcher: {e}"),
        )
    })?;

    let hash_stream = poller.with_poll_interval(polling_interval).into_stream();
    let stream = hash_stream
        .map(stream::iter)
        .flatten()
        .filter_map(move |hash| {
            let http = http.clone();
            async move {
                match http.get_block_by_hash(hash).await {
                    Ok(Some(block)) => Some(block.header),
                    Ok(None) => {
                        warn!("Block {hash} not found during polling");
                        None
                    }
                    Err(e) => {
                        error!("Failed to fetch block {hash} during polling: {e}");
                        None
                    }
                }
            }
        });

    Ok(Box::pin(stream))
}

async fn process_header(
    http: &HttpProvider,
    header: alloy::rpc::types::Header,
) -> Option<BlockInput> {
    let finalized = match http.get_block_by_number(BlockNumberOrTag::Finalized).await {
        Ok(Some(finalized)) => finalized,
        Ok(None) => {
            warn!("Finalized block not found");
            return None;
        }
        Err(e) => {
            error!("Failed to fetch finalized block: {e}");
            return None;
        }
    };

    let events = Vec::new();

    Some(BlockInput {
        block: L1BlockId {
            number: header.number,
            hash: header.hash,
            parent: header.parent_hash,
        },
        finalized: L1BlockId {
            number: finalized.header.number,
            hash: finalized.header.hash,
            parent: finalized.header.parent_hash,
        },
        timestamp: header.timestamp as Timestamp,
        events,
    })
}

// async fn fetch_block_by_number(http: &HttpProvider, number: u64) -> Option<BlockInput> {

//     let block = match http
//         .get_block_by_number(BlockNumberOrTag::Number(number))
//         .await
//     {
//         Ok(Some(block)) => block,
//         Ok(None) => {
//
//             return None;
//         }
//         Err(e) => {
//             return None;
//         }
//     };

//     process_header(http, block.header).await
// }

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::{
        node_bindings::Anvil,
    };
    use futures::StreamExt;

    #[tokio::test]
    async fn test_rpc_stream_with_anvil() {
        let anvil = Anvil::new().block_time(1).spawn();
        let url = anvil.endpoint().parse::<Url>().unwrap();

        let mut stream = RpcStream::new(std::iter::once(&url), std::iter::empty())
            .await
            .unwrap();

        let mut last_block_number = 0;

        for i in 1..=10 {
            println!("Waiting for block {i}");
            let block_input = stream.next().await.expect("Stream ended unexpectedly");
            assert!(block_input.block.number > last_block_number);
            last_block_number = block_input.block.number;
        }
    }

    #[tokio::test]
    async fn test_rpc_stream_websocket() {
        let anvil = Anvil::new().block_time(1).spawn();
        let http_url = anvil.endpoint().parse::<Url>().unwrap();
        let ws_url = anvil.ws_endpoint().parse::<Url>().unwrap();

        println!("HTTP URL: {}", http_url);
        println!("WS URL: {}", ws_url);

        let mut stream = RpcStream::new(std::iter::once(&http_url), std::iter::once(&ws_url))
            .await
            .unwrap();

        println!("Stream created successfully");

        let mut last_block_number = 0;

        for i in 1..=10 {
            println!("Waiting for block {i}");
            let block_input = stream.next().await.expect("Stream ended unexpectedly");
            assert!(block_input.block.number > last_block_number);
            last_block_number = block_input.block.number;
            println!("Received block {i}");
        }
    }
}
