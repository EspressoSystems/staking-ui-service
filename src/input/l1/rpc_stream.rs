//! An L1 event stream based on a standard JSON-RPC server.

use super::{
    BlockInput, ResettableStream, options::L1ClientOptions, switching_transport::SwitchingTransport,
};
use crate::types::common::{Address, Timestamp};
use crate::{Result, types::common::L1BlockId};
use alloy::{
    eips::BlockId,
    network::Ethereum,
    providers::{Provider, ProviderBuilder, RootProvider, WsConnect},
    rpc::{client::RpcClient, types::Header},
};
use futures::stream::{self, BoxStream, Stream, StreamExt};
use parking_lot::RwLock;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tide_disco::Url;
use tokio::time::sleep;

/// Builder for creating an RpcStream.
struct RpcStreamBuilder {
    /// Provider for making RPC calls
    provider: Arc<RootProvider<Ethereum>>,
    /// Transport for switching between HTTP providers
    transport: SwitchingTransport,
    options: L1ClientOptions,
    last_block_number: Arc<RwLock<Option<u64>>>,
}

/// An L1 event stream based on a standard JSON-RPC server.
pub struct RpcStream {
    /// block stream
    stream: BoxStream<'static, BlockInput>,
}

impl std::fmt::Debug for RpcStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcStream")
            .field("stream", &"<stream>")
            .finish()
    }
}

impl RpcStreamBuilder {
    /// Create a new builder from L1 client options.
    fn new(options: L1ClientOptions) -> Result<Self> {
        let http_urls = options.http_providers.clone();
        let transport = SwitchingTransport::new(options.clone(), http_urls)?;
        let rpc_client = RpcClient::new(transport.clone(), false);
        let provider = Arc::new(RootProvider::new(rpc_client));

        let last_block_number = Arc::new(RwLock::new(None));

        Ok(Self {
            provider,
            transport,
            options,
            last_block_number,
        })
    }

    /// Build the RpcStream with automatic reconnection.
    async fn build(self) -> RpcStream {
        let builder = Arc::new(self);
        let stream = builder.establish_stream().await;

        let stream = stream::unfold(
            (builder.clone(), stream),
            |(builder, mut stream)| async move {
                loop {
                    match stream.next().await {
                        Some(item) => return Some((item, (builder, stream))),
                        None => {
                            tracing::warn!("L1 block stream ended, reconnecting...");
                            stream = builder.establish_stream().await;
                            tracing::info!("Successfully reconnected to L1 block stream");
                        }
                    }
                }
            },
        )
        .boxed();

        RpcStream { stream }
    }

    async fn create_ws_stream(&self, url: &Url) -> Result<BoxStream<'static, BlockInput>> {
        let ws = ProviderBuilder::new()
            .connect_ws(WsConnect::new(url.clone()))
            .await
            .map_err(|err| {
                tracing::warn!("Failed to connect WebSockets provider: {err:#}");
                crate::Error::internal().context(format!("Failed to connect: {err}"))
            })?;

        let block_stream = ws.subscribe_blocks().await.map_err(|err| {
            tracing::warn!("Failed to subscribe to blocks: {err:#}");
            crate::Error::internal().context(format!("Failed to subscribe using ws: {err}"))
        })?;

        let provider = self.provider.clone();
        let last_block_number = self.last_block_number.clone();
        let retry_delay = self.options.l1_retry_delay;

        let provider_for_fetch = provider.clone();
        let provider = provider.clone();

        Ok(block_stream
            .into_stream()
            .then(move |head| {
                let provider = provider_for_fetch.clone();
                let last_block_number = last_block_number.clone();

                async move {
                    let new_block_number = head.number;
                    let prev_block_number = *last_block_number.read();
                    let mut new_blocks = Vec::new();

                    if let Some(prev) = prev_block_number {
                        if new_block_number <= prev {
                            return Vec::new();
                        }

                        if new_block_number != prev + 1 {
                            if let Some(missing) =
                                fetch_missing_blocks(&provider, prev, new_block_number, retry_delay)
                                    .await
                            {
                                new_blocks.extend(missing);
                            } else {
                                return Vec::new();
                            }
                        }
                    }

                    new_blocks.push(head);
                    *last_block_number.write() = Some(new_block_number);
                    new_blocks
                }
            })
            .map(stream::iter)
            .flatten()
            .then(move |head| {
                let provider = provider.clone();
                async move {
                    let finalized = fetch_finalized(&provider, retry_delay).await;
                    block_to_input(head, finalized)
                }
            })
            .boxed())
    }

    async fn create_http_stream(&self) -> Result<BoxStream<'static, BlockInput>> {
        let poller = self
            .provider
            .watch_blocks()
            .await
            .map_err(|err| {
                crate::Error::internal().context(format!("Failed to watch blocks: {err}"))
            })?
            .with_poll_interval(self.options.l1_polling_interval)
            .into_stream();

        let provider = self.provider.clone();
        let last_block_number = self.last_block_number.clone();
        let retry_delay = self.options.l1_retry_delay;
        let switch_notify = self.transport.switch_notify.clone();

        let provider_for_fetch = provider.clone();

        Ok(poller
            .map(stream::iter)
            .flatten()
            .then(move |hash| {
                let provider = provider_for_fetch.clone();
                let last_block_number = last_block_number.clone();

                async move {
                    let block = match provider.get_block(BlockId::hash(hash)).await {
                        Ok(Some(block)) => block,
                        Ok(None) => {
                            tracing::warn!(%hash, "HTTP stream: Block not available");
                            return Vec::new();
                        }
                        Err(err) => {
                            tracing::warn!(%hash, "HTTP stream: Failed to fetch block: {err:#}");
                            return Vec::new();
                        }
                    };

                    let new_block_number = block.header.number;
                    let prev_block_number = *last_block_number.read();

                    let mut new_blocks = Vec::new();

                    if let Some(prev) = prev_block_number {
                        if new_block_number <= prev {
                            return Vec::new();
                        }

                        if new_block_number != prev + 1 {
                            if let Some(missing) =
                                fetch_missing_blocks(&provider, prev, new_block_number, retry_delay)
                                    .await
                            {
                                new_blocks.extend(missing);
                            } else {
                                return Vec::new();
                            }
                        }
                    }

                    new_blocks.push(block.header);
                    *last_block_number.write() = Some(new_block_number);
                    new_blocks
                }
            })
            .map(stream::iter)
            .flatten()
            .take_until(async move { switch_notify.notified().await })
            .then(move |head| {
                let provider = provider.clone();
                async move {
                    let finalized = fetch_finalized(&provider, retry_delay).await;
                    block_to_input(head, finalized)
                }
            })
            .boxed())
    }

    /// Establish a new block stream connection
    async fn establish_stream(&self) -> BoxStream<'static, BlockInput> {
        // Try to establish connection with retries
        for i in 0.. {
            let res = match &self.options.l1_ws_provider {
                Some(urls) => {
                    let provider_index = i % urls.len();
                    let url = &urls[provider_index];
                    self.create_ws_stream(url).await
                }
                None => self.create_http_stream().await,
            };

            match res {
                Ok(stream) => return stream,
                Err(err) => {
                    tracing::warn!("Failed to establish stream: {err}, retrying...");
                    sleep(self.options.l1_retry_delay).await;
                }
            }
        }

        unreachable!("Infinite loop")
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
        let builder = RpcStreamBuilder::new(options)?;
        Ok(builder.build().await)
    }

    /// Get the Espresso stake table genesis block.
    pub async fn genesis(&self, _stake_table: Address) -> Result<(L1BlockId, Timestamp)> {
        todo!()
    }
}

impl Stream for RpcStream {
    type Item = BlockInput;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

impl ResettableStream for RpcStream {
    async fn reset(&mut self, _block: u64) {
        // todo
    }
}

/// Fetch finalized block with retry logic.
async fn fetch_finalized(
    provider: &Arc<RootProvider<Ethereum>>,
    retry_delay: Duration,
) -> L1BlockId {
    loop {
        match provider.get_block(BlockId::finalized()).await {
            Ok(Some(block)) => {
                return L1BlockId {
                    number: block.header.number,
                    hash: block.header.hash,
                    parent: block.header.parent_hash,
                };
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

/// Fetch missing blocks with retry logic.
async fn fetch_missing_blocks(
    provider: &Arc<RootProvider<Ethereum>>,
    prev_block: u64,
    new_block: u64,
    retry_delay: Duration,
) -> Option<Vec<Header>> {
    let mut headers = Vec::new();

    if new_block > prev_block + 1 {
        tracing::warn!(
            "Fetching missing blocks from {} to {}",
            prev_block + 1,
            new_block - 1
        );

        for block_num in (prev_block + 1)..new_block {
            loop {
                match provider.get_block(BlockId::number(block_num)).await {
                    Ok(Some(block)) => {
                        headers.push(block.header);
                        break;
                    }
                    Ok(None) => {
                        tracing::warn!("Missing block {block_num} not found, retrying...");
                        sleep(retry_delay).await;
                    }
                    Err(err) => {
                        tracing::warn!(
                            "Failed to fetch missing block {block_num}: {err}, retrying..."
                        );
                        sleep(retry_delay).await;
                    }
                }
            }
        }
    }

    Some(headers)
}

/// Convert header and finalized block to BlockInput.
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
#[cfg(test)]
mod tests {
    use super::*;
    use alloy::node_bindings::Anvil;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_rpc_stream_with_anvil() {
        let anvil = Anvil::new().block_time(1).spawn();
        let url = anvil.endpoint().parse::<Url>().unwrap();

        let options = L1ClientOptions {
            http_providers: vec![url],
            ..Default::default()
        };

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

        let options = L1ClientOptions {
            http_providers: vec![http_url],
            l1_ws_provider: Some(vec![ws_url]),
            ..Default::default()
        };

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

    #[tokio::test]
    async fn test_l1_stream_reconnect() {
        // Start two Anvil instances
        let anvil1 = Anvil::new().block_time(1).spawn();
        let http_url1 = anvil1.endpoint().parse::<Url>().unwrap();

        let anvil2 = Anvil::new().block_time(1).spawn();
        let http_url2 = anvil2.endpoint().parse::<Url>().unwrap();

        let options = L1ClientOptions {
            http_providers: vec![http_url1.clone(), http_url2.clone()],
            l1_retry_delay: Duration::from_millis(100),
            ..Default::default()
        };

        let mut stream = RpcStream::new(options).await.unwrap();

        for _i in 1..=3 {
            let block_input = stream.next().await.unwrap();
            assert!(block_input.block.number > 0);
        }

        drop(anvil1);

        for _i in 1..=5 {
            let block_input = stream.next().await.unwrap();

            assert!(block_input.block.number > 0);
        }
    }
}
