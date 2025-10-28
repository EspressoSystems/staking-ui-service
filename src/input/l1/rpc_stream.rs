//! An L1 event stream based on a standard JSON-RPC server.

use super::{
    BlockInput, ResettableStream, options::L1ClientOptions, switching_transport::SwitchingTransport,
};
use crate::{Result, types::common::L1BlockId};
use alloy::{
    eips::BlockId,
    network::Ethereum,
    primitives::BlockHash,
    providers::{Provider, ProviderBuilder, RootProvider, WsConnect},
    rpc::{client::RpcClient, types::Header},
};
use futures::{
    Future,
    stream::{self, BoxStream, Stream, StreamExt},
};
use parking_lot::RwLock;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tide_disco::Url;
use tokio::time::sleep;

/// An L1 event stream based on a standard JSON-RPC server.
pub struct RpcStream {
    /// Current block stream
    stream: BoxStream<'static, BlockInput>,
    /// Provider for making RPC calls
    provider: Arc<RootProvider<Ethereum>>,
    /// Transport for switching between HTTP providers
    transport: SwitchingTransport,
    /// WebSocket URLs for subscription (if any)
    ws_urls: Option<Vec<Url>>,
    /// Client options
    options: L1ClientOptions,
    /// Last finalized block
    /// todo:
    last_finalized: Arc<RwLock<L1BlockId>>,
    /// Pending reconnection future
    reconnecting:
        Option<Pin<Box<dyn Future<Output = Result<BoxStream<'static, BlockInput>>> + Send>>>,
}

impl std::fmt::Debug for RpcStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcStream")
            .field("stream", &"<stream>")
            .field("ws_urls", &self.ws_urls)
            .field("reconnecting", &self.reconnecting.is_some())
            .finish()
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
    pub async fn new(options: L1ClientOptions) -> Result<Self> {
        let http_urls = options.http_providers.clone();

        let transport = SwitchingTransport::new(options.clone(), http_urls)?;

        let rpc_client = RpcClient::new(transport.clone(), false);
        let provider = Arc::new(RootProvider::new(rpc_client));

        let ws_urls = options.l1_ws_provider.clone();

        // todo:
        let last_finalized = Arc::new(RwLock::new(L1BlockId {
            number: 0,
            hash: BlockHash::ZERO,
            parent: BlockHash::ZERO,
        }));

        let stream = Self::establish_stream(
            provider.clone(),
            transport.clone(),
            ws_urls.clone(),
            options.clone(),
            last_finalized.clone(),
        )
        .await?;

        Ok(Self {
            stream,
            provider,
            transport,
            ws_urls,
            options,
            last_finalized,
            reconnecting: None,
        })
    }

    async fn fetch_finalized(
        provider: &Arc<RootProvider<Ethereum>>,
        last_finalized: &Arc<RwLock<L1BlockId>>,
        retry_delay: Duration,
    ) -> L1BlockId {
        loop {
            match provider.get_block(BlockId::finalized()).await {
                Ok(Some(block)) => {
                    let finalized_id = L1BlockId {
                        number: block.header.number,
                        hash: block.header.hash,
                        parent: block.header.parent_hash,
                    };
                    *last_finalized.write() = finalized_id;
                    return finalized_id;
                }
                Ok(None) => {
                    tracing::warn!("finalized block is None, will retry");
                }
                Err(err) => {
                    tracing::warn!("Failed to fetch finalized block: {err}, will retry");
                }
            }
            sleep(retry_delay).await;
        }
    }

    async fn create_ws_stream(
        url: &Url,
        provider: Arc<RootProvider<Ethereum>>,
        last_finalized: Arc<RwLock<L1BlockId>>,
        retry_delay: Duration,
    ) -> Result<BoxStream<'static, BlockInput>> {
        let ws = ProviderBuilder::new()
            .connect_ws(WsConnect::new(url.clone()))
            .await
            .map_err(|err| {
                tracing::warn!("Failed to connect WebSockets provider: {err:#}");
                crate::Error::internal().context(format!("Failed to connect: {err}"))
            })?;

        let stream = ws.subscribe_blocks().await.map_err(|err| {
            tracing::warn!("Failed to subscribe to blocks: {err:#}");
            crate::Error::internal().context(format!("Failed to subscribe: {err}"))
        })?;

        Ok(stream
            .into_stream()
            .then(move |head| {
                let provider = provider.clone();
                let last_finalized = last_finalized.clone();
                async move {
                    let finalized =
                        Self::fetch_finalized(&provider, &last_finalized, retry_delay).await;
                    Self::block_to_input(head, finalized)
                }
            })
            .boxed())
    }

    async fn create_http_stream(
        provider: Arc<RootProvider<Ethereum>>,
        transport: &SwitchingTransport,
        last_finalized: Arc<RwLock<L1BlockId>>,
        polling_interval: Duration,
        retry_delay: Duration,
    ) -> Result<BoxStream<'static, BlockInput>> {
        let poller_builder = provider.watch_blocks().await.map_err(|err| {
            crate::Error::internal().context(format!("Failed to watch blocks: {err}"))
        })?;

        let stream = poller_builder
            .with_poll_interval(polling_interval)
            .into_stream();
        let rpc_clone = provider.clone();
        let provider_for_finalized = provider.clone();
        let last_finalized_clone = last_finalized.clone();
        let switch_notify = transport.switch_notify.clone();

        Ok(stream
            .map(stream::iter)
            .flatten()
            .filter_map(move |hash| {
                let rpc = rpc_clone.clone();
                async move {
                    match rpc.get_block(BlockId::hash(hash)).await {
                        Ok(Some(block)) => Some(block.header),
                        Ok(None) => {
                            tracing::warn!(%hash, "HTTP stream yielded a block hash that was not available");
                            None
                        }
                        Err(err) => {
                            tracing::warn!(%hash, "Error fetching block from HTTP stream: {err:#}");
                            None
                        }
                    }
                }
            })
            .take_until(async move { switch_notify.notified().await })
            .then(move |head| {
                let provider = provider_for_finalized.clone();
                let last_finalized = last_finalized_clone.clone();
                async move {
                    let finalized = Self::fetch_finalized(&provider, &last_finalized, retry_delay).await;
                    Self::block_to_input(head, finalized)
                }
            })
            .boxed())
    }

    /// Establish a new block stream connection. Returns the stream on success.
    pub fn establish_stream(
        provider: Arc<RootProvider<Ethereum>>,
        transport: SwitchingTransport,
        ws_urls: Option<Vec<Url>>,
        opt: L1ClientOptions,
        last_finalized: Arc<RwLock<L1BlockId>>,
    ) -> Pin<Box<dyn Future<Output = Result<BoxStream<'static, BlockInput>>> + Send>> {
        Box::pin(async move {
            let retry_delay = opt.l1_retry_delay;
            let polling_interval = opt.l1_polling_interval;

            // todo:
            Self::fetch_finalized(&provider, &last_finalized, retry_delay).await;

            // Try to establish connection with retries
            for i in 0.. {
                let res = match &ws_urls {
                    Some(urls) => {
                        let provider_index = i % urls.len();
                        let url = &urls[provider_index];
                        Self::create_ws_stream(
                            url,
                            provider.clone(),
                            last_finalized.clone(),
                            retry_delay,
                        )
                        .await
                    }
                    None => {
                        Self::create_http_stream(
                            provider.clone(),
                            &transport,
                            last_finalized.clone(),
                            polling_interval,
                            retry_delay,
                        )
                        .await
                    }
                };

                match res {
                    Ok(stream) => return Ok(stream),
                    Err(err) => {
                        tracing::warn!("Failed to establish stream: {err}, retrying...");
                        sleep(retry_delay).await;
                    }
                }
            }

            unreachable!("Infinite loop")
        })
    }

    fn block_to_input(head: Header, finalized: L1BlockId) -> BlockInput {
        let block = L1BlockId {
            number: head.number,
            hash: head.hash,
            parent: head.parent_hash,
        };

        BlockInput {
            block,
            finalized,
            timestamp: head.timestamp,
            events: vec![],
        }
    }
}

impl Stream for RpcStream {
    type Item = BlockInput;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(mut reconnect_future) = self.reconnecting.take() {
            match reconnect_future.as_mut().poll(cx) {
                Poll::Ready(Ok(new_stream)) => {
                    tracing::info!("Successfully reconnected to L1 block stream");
                    self.stream = new_stream;
                }
                Poll::Ready(Err(err)) => {
                    panic!(
                        "This should not happen because establish_stream retries indefinitely. err={err}"
                    );
                }
                Poll::Pending => {
                    self.reconnecting = Some(reconnect_future);
                    return Poll::Pending;
                }
            }
        }

        match self.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(input)) => {
                tracing::debug!(block = input.block.number, "Yielding L1 block");
                Poll::Ready(Some(input))
            }
            Poll::Ready(None) => {
                tracing::warn!("L1 block stream ended, initiating reconnection");

                let reconnect_future = Self::establish_stream(
                    self.provider.clone(),
                    self.transport.clone(),
                    self.ws_urls.clone(),
                    self.options.clone(),
                    self.last_finalized.clone(),
                );

                self.reconnecting = Some(reconnect_future);

                Poll::Pending
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl ResettableStream for RpcStream {
    async fn reset(&mut self, _block: u64) {
        // todo
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::node_bindings::Anvil;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_rpc_stream_with_anvil() {
        let anvil = Anvil::new().block_time(1).spawn();
        let url = anvil.endpoint().parse::<Url>().unwrap();

        let mut options = L1ClientOptions::default();
        options.http_providers = vec![http_url];

        let mut stream = RpcStream::new(options).await.unwrap();

        let mut last_block_number = 0;

        for i in 1..=10 {
            println!("Waiting for block {i}");
            let block_input = stream.next().await.expect("Stream ended unexpectedly");

            assert!(block_input.block.number == last_block_number + 1);
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

        let mut options = L1ClientOptions::default();
        options.http_providers = vec![http_url];
        options.l1_ws_provider = Some(vec![ws_url]);

        let mut stream = RpcStream::new(options).await.unwrap();

        println!("Stream created successfully");

        let mut last_block_number = 0;

        for i in 1..=10 {
            println!("Waiting for block {i}");
            let block_input = stream.next().await.expect("Stream ended unexpectedly");
            assert!(block_input.block.number == last_block_number + 1);
            last_block_number = block_input.block.number;
            println!("Received block {i}");
        }
    }
}
