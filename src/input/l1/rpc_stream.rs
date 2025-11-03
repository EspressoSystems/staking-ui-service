//! An L1 event stream based on a standard JSON-RPC server.

use super::{
    BlockInput, L1Event, ResettableStream, options::L1ClientOptions,
    switching_transport::SwitchingTransport,
};
use crate::types::common::{Address, Timestamp};
use crate::{Error, Result, types::common::L1BlockId};
use alloy::{
    eips::BlockId,
    network::Ethereum,
    providers::{Provider, ProviderBuilder, RootProvider, WsConnect},
    rpc::{
        client::RpcClient,
        types::{Filter, Header},
    },
    sol_types::SolEventInterface,
};
use futures::stream::{self, BoxStream, Stream, StreamExt};
use hotshot_contract_adapter::sol_types::StakeTableV2::StakeTableV2Events;
use hotshot_contract_adapter::sol_types::{RewardClaim::RewardClaimEvents, StakeTable};
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
    stake_table_address: Address,
    reward_contract_address: Address,
}

/// An L1 event stream based on a standard JSON-RPC server.
pub struct RpcStream {
    /// block stream
    stream: BoxStream<'static, BlockInput>,
    /// Builder for recreating the stream
    builder: Arc<RpcStreamBuilder>,
}

impl std::fmt::Debug for RpcStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcStream")
            .field("stream", &"<stream>")
            .finish()
    }
}

impl RpcStreamBuilder {
    /// Create a new builder from L1 client options and contract addresses.
    fn new(
        options: L1ClientOptions,
        stake_table_address: Address,
        reward_contract_address: Address,
    ) -> Result<Self> {
        let http_urls = options.http_providers.clone();
        let transport = SwitchingTransport::new(options.clone(), http_urls)?;
        let rpc_client = RpcClient::new(transport.clone(), false);
        let provider = Arc::new(RootProvider::new(rpc_client));

        Ok(Self {
            provider,
            transport,
            options,
            stake_table_address,
            reward_contract_address,
        })
    }

    /// Build the RpcStream
    async fn build(self) -> RpcStream {
        let builder = Arc::new(self);
        let stream = builder.clone().stream_with_reconnect(None).await;
        RpcStream { stream, builder }
    }

    /// Create the stream wrapper with reconnection
    async fn stream_with_reconnect(
        self: Arc<Self>,
        last_block: Option<u64>,
    ) -> BoxStream<'static, BlockInput> {
        let stream = self.establish_stream(last_block).await;

        stream::unfold(
            (self.clone(), stream, last_block),
            |(builder, mut stream, mut last_block)| async move {
                loop {
                    match stream.next().await {
                        Some(item) => {
                            last_block = Some(item.block.number);
                            return Some((item, (builder, stream, last_block)));
                        }
                        None => {
                            sleep(builder.options.l1_retry_delay).await;
                            tracing::warn!("L1 block stream ended, reconnecting...");
                            stream = builder.establish_stream(last_block).await;
                            tracing::info!("Successfully reconnected to L1 block stream");
                        }
                    }
                }
            },
        )
        .boxed()
    }

    async fn create_ws_stream(
        &self,
        url: &Url,
        last_block: Option<u64>,
    ) -> Result<BoxStream<'static, BlockInput>> {
        let ws = ProviderBuilder::new()
            .connect_ws(WsConnect::new(url.clone()))
            .await
            .map_err(|err| {
                tracing::warn!("Failed to connect WebSockets provider: {err:#}");
                Error::internal().context(format!("Failed to connect: {err}"))
            })?;

        let block_stream = ws.subscribe_blocks().await.map_err(|err| {
            tracing::warn!("Failed to subscribe to blocks: {err:#}");
            Error::internal().context(format!("Failed to subscribe using ws: {err}"))
        })?;
        tracing::info!(%url, "Successfully connected to WebSocket provider and subscribed to blocks");

        let retry_delay = self.options.l1_retry_delay;
        let provider = self.provider.clone();
        let stake_table_address = self.stake_table_address;
        let reward_contract_address = self.reward_contract_address;

        let provider_for_fetch = provider.clone();
        let block_stream = block_stream.into_stream();

        Ok(stream::unfold(
            (block_stream, last_block, ws),
            move |(mut stream, mut last_block_number, ws)| {
                let provider = provider.clone();

                async move {
                    let head = stream.next().await?;
                    let blocks =
                        process_block_header(&provider, &mut last_block_number, head, retry_delay)
                            .await;
                    Some((stream::iter(blocks), (stream, last_block_number, ws)))
                }
            },
        )
        .flatten()
        .then(move |head| {
            let provider = provider_for_fetch.clone();
            async move {
                create_block_input(
                    head,
                    &provider,
                    retry_delay,
                    stake_table_address,
                    reward_contract_address,
                )
                .await
            }
        })
        .boxed())
    }

    async fn create_http_stream(
        &self,
        last_block: Option<u64>,
    ) -> Result<BoxStream<'static, BlockInput>> {
        let poller = self
            .provider
            .watch_blocks()
            .await
            .map_err(|err| Error::internal().context(format!("Failed to watch blocks: {err}")))?
            .with_poll_interval(self.options.l1_polling_interval)
            .into_stream();

        let provider = self.provider.clone();
        let retry_delay = self.options.l1_retry_delay;
        let switch_notify = self.transport.switch_notify.clone();
        let stake_table_address = self.stake_table_address;
        let reward_contract_address = self.reward_contract_address;

        let provider_for_fetch = provider.clone();
        let poller_stream = poller.map(stream::iter).flatten();

        Ok(stream::unfold(
            (poller_stream, last_block),
            move |(mut stream, mut last_block_number)| {
                let provider = provider_for_fetch.clone();

                async move {
                    let hash = stream.next().await?;

                    let block = match provider.get_block(BlockId::hash(hash)).await {
                        Ok(Some(block)) => block,
                        Ok(None) => {
                            tracing::warn!(%hash, "HTTP stream: Block not available");
                            return Some((stream::iter(Vec::new()), (stream, last_block_number)));
                        }
                        Err(err) => {
                            tracing::warn!(%hash, "HTTP stream: Failed to fetch block: {err:#}");
                            return Some((stream::iter(Vec::new()), (stream, last_block_number)));
                        }
                    };

                    let blocks = process_block_header(
                        &provider,
                        &mut last_block_number,
                        block.header,
                        retry_delay,
                    )
                    .await;
                    Some((stream::iter(blocks), (stream, last_block_number)))
                }
            },
        )
        .flatten()
        .take_until(async move {
            switch_notify.notified().await;
            tracing::warn!("HTTP stream shutting down due to provider switch");
        })
        .then(move |head| {
            let provider = provider.clone();
            async move {
                create_block_input(
                    head,
                    &provider,
                    retry_delay,
                    stake_table_address,
                    reward_contract_address,
                )
                .await
            }
        })
        .boxed())
    }

    /// Establish a new block stream connection
    async fn establish_stream(&self, last_block: Option<u64>) -> BoxStream<'static, BlockInput> {
        // Try to establish connection with retries
        for i in 0.. {
            let res = match &self.options.l1_ws_provider {
                Some(urls) => {
                    let provider_index = i % urls.len();
                    let url = &urls[provider_index];
                    self.create_ws_stream(url, last_block).await
                }
                None => self.create_http_stream(last_block).await,
            };

            match res {
                Ok(stream) => {
                    tracing::info!(attempt = i, "Successfully established L1 block stream");
                    return stream;
                }
                Err(err) => {
                    tracing::warn!(
                        attempt = i,
                        "Failed to establish stream: {err}, retrying..."
                    );
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
    pub async fn new(
        options: L1ClientOptions,
        stake_table: Address,
        reward_contract: Address,
    ) -> Result<Self> {
        let builder = RpcStreamBuilder::new(options, stake_table, reward_contract)?;
        Ok(builder.build().await)
    }

    /// Get the Espresso stake table genesis block.
    pub async fn genesis(&self, stake_table: Address) -> Result<(L1BlockId, Timestamp)> {
        let stake_table_contract = StakeTable::new(stake_table, &self.builder.provider);

        // Get the block number when the contract was initialized
        let initialized_at_block = stake_table_contract
            .initializedAtBlock()
            .call()
            .await
            .map_err(|err| {
                Error::internal().context(format!("Failed to retrieve initialization block: {err}"))
            })?
            .to::<u64>();

        // Fetch the finalized block to verify the initialized block is less than finalized
        let finalized_block = self
            .builder
            .provider
            .get_block(BlockId::finalized())
            .await
            .map_err(|err| {
                Error::internal().context(format!("Failed to fetch finalized block: {err}"))
            })?
            .ok_or_else(|| Error::internal().context("Finalized block not found"))?;

        let finalized_block_number = finalized_block.header.number;

        if initialized_at_block >= finalized_block_number {
            panic!(
                "Initialized block {initialized_at_block} must be less than finalized block {finalized_block_number}",
            );
        }

        let block = self
            .builder
            .provider
            .get_block(BlockId::number(initialized_at_block))
            .await
            .map_err(|err| {
                Error::internal().context(format!(
                    "Failed to fetch init block {initialized_at_block}: {err}"
                ))
            })?
            .ok_or_else(|| {
                Error::internal().context(format!("Init block {initialized_at_block} not found"))
            })?;

        // Fetch the exitEscrowPeriod at the initialized block
        let _exit_escrow_period = stake_table_contract
            .exitEscrowPeriod()
            .block(BlockId::number(initialized_at_block))
            .call()
            .await
            .map_err(|err| {
                Error::internal().context(format!("Failed to fetch exitEscrowPeriod: {err}"))
            })?
            .to::<u64>();

        let l1_block_id = L1BlockId {
            number: initialized_at_block,
            hash: block.header.hash,
            parent: block.header.parent_hash,
        };

        Ok((l1_block_id, block.header.timestamp))
    }
}

impl Stream for RpcStream {
    type Item = BlockInput;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

impl ResettableStream for RpcStream {
    async fn reset(&mut self, block: u64) {
        tracing::info!("Resetting RpcStream to block {block}");

        // Recreate the stream to discard any buffered blocks from the flatten().
        // The missing fetch logic returns a vector which gets flattened into individual blocks.
        // Without recreating the stream, next() would continue with buffered blocks
        // instead of triggering the missing fetch from the reset point.
        self.stream = self
            .builder
            .clone()
            .stream_with_reconnect(Some(block))
            .await;

        tracing::warn!(
            "Reset RpcStream to block {block} (will resume from block {})",
            block + 1
        );
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

/// Process a new block header, handling missing blocks
/// and updating last_block_number.
/// Returns a vector of headers in order, including any missing blocks that were fetched.
async fn process_block_header(
    provider: &Arc<RootProvider<Ethereum>>,
    last_block_number: &mut Option<u64>,
    new_header: Header,
    retry_delay: Duration,
) -> Vec<Header> {
    let new_block_number = new_header.number;
    let prev_block_number = *last_block_number;

    let mut new_blocks = Vec::new();

    if let Some(prev) = prev_block_number {
        if new_block_number <= prev {
            tracing::info!(
                "Skipping block {new_block_number} as it's not newer than previous block {prev}"
            );
            return Vec::new();
        }

        if new_block_number != prev + 1 {
            let missing_blocks =
                fetch_missing_blocks(provider, prev, new_block_number, retry_delay).await;

            tracing::info!(
                fetched_count = missing_blocks.len(),
                "Successfully fetched missing blocks"
            );
            new_blocks.extend(missing_blocks);
        }
    }

    new_blocks.push(new_header);
    *last_block_number = Some(new_block_number);
    new_blocks
}

/// Fetch missing blocks with retry logic.
async fn fetch_missing_blocks(
    provider: &Arc<RootProvider<Ethereum>>,
    prev_block: u64,
    new_block: u64,
    retry_delay: Duration,
) -> Vec<Header> {
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

    headers
}

/// Fetch events from L1 for a specific block with retry logic.
async fn fetch_block_events(
    provider: &RootProvider<Ethereum>,
    header: &Header,
    stake_table_address: Address,
    reward_contract_address: Address,
    retry_delay: Duration,
) -> Vec<L1Event> {
    let stake_table_may_have_logs = header.logs_bloom.contains_raw_log(stake_table_address, &[]);
    let reward_contract_may_have_logs = header
        .logs_bloom
        .contains_raw_log(reward_contract_address, &[]);

    if !stake_table_may_have_logs && !reward_contract_may_have_logs {
        // Bloom filter indicates no logs for our addresses in this block
        return Vec::new();
    }

    let filter = Filter::new()
        .at_block_hash(header.hash)
        .address(vec![stake_table_address, reward_contract_address]);

    loop {
        match provider.get_logs(&filter).await {
            Ok(logs) => {
                let mut events = Vec::new();

                for log in logs {
                    // Try to decode stake table event
                    if log.address() == stake_table_address {
                        match StakeTableV2Events::decode_raw_log(log.topics(), &log.data().data) {
                            Ok(st_event) => {
                                let event = st_event.try_into().unwrap();
                                events.push(L1Event::StakeTable(Arc::new(event)));
                            }
                            Err(e) => {
                                tracing::warn!(
                                    block = header.number,
                                    tx_hash = ?log.transaction_hash,
                                    "Failed to decode stake table log: {e:?}"
                                );
                            }
                        }
                        continue;
                    }

                    // Try to decode reward claim event
                    if log.address() == reward_contract_address {
                        match RewardClaimEvents::decode_raw_log(log.topics(), &log.data().data) {
                            Ok(event) => {
                                events.push(L1Event::Reward(Arc::new(event)));
                            }
                            Err(e) => {
                                tracing::warn!(
                                    block = header.number,
                                    tx_hash = ?log.transaction_hash,
                                    "Failed to decode reward claim log: {e:?}"
                                );
                            }
                        }
                        continue;
                    }
                }

                return events;
            }
            Err(err) => {
                tracing::warn!(
                    block = header.number,
                    "Failed to fetch logs from provider: {err}, retrying..."
                );
                sleep(retry_delay).await;
            }
        }
    }
}

/// Fetch all necessary data and construct a BlockInput.
async fn create_block_input(
    head: Header,
    provider: &Arc<RootProvider<Ethereum>>,
    retry_delay: Duration,
    stake_table_address: Address,
    reward_contract_address: Address,
) -> BlockInput {
    let finalized = fetch_finalized(provider, retry_delay).await;
    let events = fetch_block_events(
        provider,
        &head,
        stake_table_address,
        reward_contract_address,
        retry_delay,
    )
    .await;

    BlockInput {
        block: L1BlockId {
            number: head.number,
            hash: head.hash,
            parent: head.parent_hash,
        },
        finalized,
        timestamp: head.timestamp,
        events,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use alloy::rpc::types::TransactionReceipt;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol_types::SolEvent;
    use alloy::{
        network::EthereumWallet,
        node_bindings::Anvil,
        primitives::U256,
        providers::{ProviderBuilder, WalletProvider, ext::AnvilApi},
    };
    use committable::Committable;
    use espresso_contract_deployer::HttpProviderWithWallet;
    use espresso_contract_deployer::{
        Contract, Contracts, build_signer, builder::DeployerArgsBuilder,
        network_config::light_client_genesis_from_stake_table,
    };
    use espresso_types::v0::StakeTableState;
    use futures::StreamExt;
    use hotshot_contract_adapter::sol_types::StakeTableV2::{
        CommissionUpdated, ConsensusKeysUpdated, ConsensusKeysUpdatedV2, Delegated, Undelegated,
        ValidatorExit, ValidatorRegistered, ValidatorRegisteredV2,
    };
    use hotshot_contract_adapter::sol_types::{G1PointSol, StakeTableV2};
    use hotshot_contract_adapter::stake_table::{
        StateSignatureSol, sign_address_bls, sign_address_schnorr,
    };
    use hotshot_state_prover::v3::mock_ledger::STAKE_TABLE_CAPACITY_FOR_TEST;
    use hotshot_types::{light_client::StateKeyPair, signature_key::BLSKeyPair};
    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use staking_cli::demo::{DelegationConfig, StakingTransactions};
    use std::time::Duration;

    const DEV_MNEMONIC: &str = "test test test test test test test test test test test junk";

    #[derive(Debug, Clone)]
    pub struct DeploymentConfig {
        pub mnemonic: String,
        pub deployer_index: u32,
        pub blocks_per_epoch: u64,
        pub epoch_start_block: u64,
        pub exit_escrow_period_secs: u64,
        pub token_name: String,
        pub token_symbol: String,
        pub initial_token_supply: u64,
        pub ops_timelock_delay_secs: u64,
        pub safe_exit_timelock_delay_secs: u64,
    }

    impl Default for DeploymentConfig {
        fn default() -> Self {
            Self {
                mnemonic: "test test test test test test test test test test test junk".to_string(),
                deployer_index: 0,
                blocks_per_epoch: 100,
                epoch_start_block: 1,
                exit_escrow_period_secs: 250,
                token_name: "Espresso".to_string(),
                token_symbol: "ESP".to_string(),
                initial_token_supply: 3_590_000_000,
                ops_timelock_delay_secs: 0,
                safe_exit_timelock_delay_secs: 10,
            }
        }
    }

    pub struct ContractDeployment {
        pub rpc_url: Url,
        pub stake_table_addr: Address,
        pub reward_claim_addr: Address,
        pub token_addr: Address,
    }

    impl ContractDeployment {
        pub async fn deploy(rpc_url: Url) -> Result<Self> {
            Self::deploy_with_config(rpc_url, DeploymentConfig::default()).await
        }

        pub async fn deploy_with_config(rpc_url: Url, config: DeploymentConfig) -> Result<Self> {
            let provider = ProviderBuilder::new()
                .wallet(EthereumWallet::from(build_signer(
                    &config.mnemonic,
                    config.deployer_index,
                )))
                .connect_http(rpc_url.clone());

            let deployer_address = provider.default_signer_address();

            let (genesis_state, genesis_stake) = light_client_genesis_from_stake_table(
                &Default::default(),
                STAKE_TABLE_CAPACITY_FOR_TEST,
            )
            .unwrap();

            let args = DeployerArgsBuilder::default()
                .deployer(provider.clone())
                .rpc_url(rpc_url.clone())
                .mock_light_client(true)
                .genesis_lc_state(genesis_state)
                .genesis_st_state(genesis_stake)
                .blocks_per_epoch(config.blocks_per_epoch)
                .epoch_start_block(config.epoch_start_block)
                .multisig_pauser(deployer_address)
                .exit_escrow_period(U256::from(config.exit_escrow_period_secs))
                .token_name(config.token_name)
                .token_symbol(config.token_symbol)
                .initial_token_supply(U256::from(config.initial_token_supply))
                .ops_timelock_delay(U256::from(config.ops_timelock_delay_secs))
                .ops_timelock_admin(deployer_address)
                .ops_timelock_proposers(vec![deployer_address])
                .ops_timelock_executors(vec![deployer_address])
                .safe_exit_timelock_delay(U256::from(config.safe_exit_timelock_delay_secs))
                .safe_exit_timelock_admin(deployer_address)
                .safe_exit_timelock_proposers(vec![deployer_address])
                .safe_exit_timelock_executors(vec![deployer_address])
                .use_timelock_owner(false)
                .build()
                .map_err(|err| {
                    Error::internal().context(format!("Failed to build deployer args: {err}"))
                })?;

            let mut contracts = Contracts::new();
            args.deploy_all(&mut contracts).await.map_err(|err| {
                Error::internal().context(format!("Failed to deploy contracts: {err}"))
            })?;

            let stake_table_addr = contracts
                .address(Contract::StakeTableProxy)
                .ok_or_else(|| Error::internal().context("StakeTable address not found"))?;
            let reward_claim_addr = contracts
                .address(Contract::RewardClaimProxy)
                .ok_or_else(|| Error::internal().context("RewardClaim address not found"))?;
            let token_addr = contracts
                .address(Contract::EspTokenProxy)
                .ok_or_else(|| Error::internal().context("Token address not found"))?;

            println!("Deployed contracts:");
            println!("StakeTable: {stake_table_addr}");
            println!("RewardClaim: {reward_claim_addr}");
            println!("Token: {token_addr}");

            Ok(Self {
                rpc_url,
                stake_table_addr,
                reward_claim_addr,
                token_addr,
            })
        }

        pub fn create_test_validators(
            count: usize,
        ) -> Vec<(PrivateKeySigner, BLSKeyPair, StateKeyPair)> {
            (0..count)
                .map(|i| {
                    let index = i as u32;
                    let seed = [index as u8; 32];
                    let signer = build_signer(DEV_MNEMONIC, index);
                    let bls_key_pair = BLSKeyPair::generate(&mut StdRng::from_seed(seed));
                    let state_key_pair =
                        StateKeyPair::generate_from_seed_indexed(seed, index as u64);
                    (signer, bls_key_pair, state_key_pair)
                })
                .collect()
        }

        pub async fn register_validators(
            &self,
            validators: Vec<(PrivateKeySigner, BLSKeyPair, StateKeyPair)>,
            delegation_config: DelegationConfig,
        ) -> Result<Vec<TransactionReceipt>> {
            let provider = ProviderBuilder::new()
                .wallet(EthereumWallet::from(build_signer(DEV_MNEMONIC, 0)))
                .connect_http(self.rpc_url.clone());

            let mut staking_txns = StakingTransactions::create(
                self.rpc_url.clone(),
                &provider,
                self.stake_table_addr,
                validators,
                delegation_config,
            )
            .await
            .map_err(|err| {
                Error::internal().context(format!("Failed to create staking transactions: {err}"))
            })?;

            let receipts = staking_txns.apply_all().await.map_err(|err| {
                Error::internal().context(format!("Failed to apply transactions: {err}"))
            })?;

            Ok(receipts)
        }

        pub fn spawn_task(&self) -> BackgroundStakeTableOps {
            BackgroundStakeTableOps::spawn(self.rpc_url.clone(), self.stake_table_addr)
        }
    }

    /// Spawns a background task that continuously performs random stake table operations.
    /// Operations include: registering validators, updating consensus keys, undelegating, and deregistering.
    /// Used in tests to have some activity on L1 and validate events fetching
    pub struct BackgroundStakeTableOps {
        task_handle: Option<tokio::task::JoinHandle<()>>,
    }

    struct DelegatorInfo {
        address: Address,
        provider: HttpProviderWithWallet,
    }

    struct ValidatorInfo {
        index: u64,
        address: Address,
        provider: HttpProviderWithWallet,
        delegators: Vec<DelegatorInfo>,
    }

    impl BackgroundStakeTableOps {
        pub fn spawn(rpc_url: Url, stake_table_addr: Address) -> Self {
            let task_handle = tokio::spawn(async move {
                let mut validator_index = 0u64;
                let mut registered_validators = Vec::<ValidatorInfo>::new();
                let mut rng = StdRng::from_entropy();

                for i in 0u64.. {
                    let operation = rng.gen_range(0..4);

                    match operation {
                        0 => {
                            let seed = [validator_index as u8; 32];
                            let signer = build_signer(DEV_MNEMONIC, validator_index as u32);
                            let bls_key = BLSKeyPair::generate(&mut StdRng::from_seed(seed));
                            let schnorr_key =
                                StateKeyPair::generate_from_seed_indexed(seed, validator_index);

                            let provider = ProviderBuilder::new()
                                .wallet(EthereumWallet::from(build_signer(DEV_MNEMONIC, 0)))
                                .connect_http(rpc_url.clone());

                            match StakingTransactions::create(
                                rpc_url.clone(),
                                &provider,
                                stake_table_addr,
                                vec![(signer.clone(), bls_key.clone(), schnorr_key.clone())],
                                DelegationConfig::MultipleDelegators,
                            )
                            .await
                            {
                                Ok(mut staking_txns) => {
                                    let validator_provider =
                                        staking_txns.provider(signer.address()).unwrap().clone();

                                    let delegator_infos: Vec<DelegatorInfo> = staking_txns
                                        .delegations()
                                        .iter()
                                        .filter(|d| d.validator == signer.address())
                                        .filter_map(|d| {
                                            let provider = staking_txns.provider(d.from)?.clone();

                                            Some(DelegatorInfo {
                                                address: d.from,
                                                provider,
                                            })
                                        })
                                        .collect();

                                    if staking_txns.apply_all().await.is_ok() {
                                        println!(
                                            "Background: Registered validator #{validator_index} with {} delegators",
                                            delegator_infos.len(),
                                        );

                                        registered_validators.push(ValidatorInfo {
                                            index: validator_index,
                                            address: signer.address(),
                                            provider: validator_provider,
                                            delegators: delegator_infos,
                                        });
                                    }
                                }
                                Err(e) => {
                                    eprintln!(
                                        "Background: Failed to create staking transactions for validator #{validator_index}: {e:?}"
                                    );
                                }
                            }
                            validator_index += 1;
                        }

                        1 => {
                            // Update consensus keys on a random registered validator
                            if registered_validators.is_empty() {
                                continue;
                            }
                            let idx = rng.gen_range(0..registered_validators.len());
                            let validator = &registered_validators[idx];

                            let seed = (validator.index * 10000 + i).to_le_bytes();
                            let mut new_seed = [0u8; 32];
                            new_seed[..8].copy_from_slice(&seed);

                            let new_bls_key =
                                BLSKeyPair::generate(&mut StdRng::from_seed(new_seed));
                            let new_schnorr_key =
                                StateKeyPair::generate_from_seed_indexed(new_seed, validator.index);

                            let bls_sig: G1PointSol =
                                sign_address_bls(&new_bls_key, validator.address).into();
                            let schnorr_sig: StateSignatureSol =
                                sign_address_schnorr(&new_schnorr_key, validator.address).into();

                            match StakeTableV2::new(stake_table_addr, &validator.provider)
                                .updateConsensusKeysV2(
                                    new_bls_key.ver_key().into(),
                                    new_schnorr_key.ver_key().into(),
                                    bls_sig.into(),
                                    schnorr_sig.into(),
                                )
                                .send()
                                .await
                            {
                                Ok(pending) => match pending.get_receipt().await {
                                    Ok(receipt) => println!(
                                        "Background: Updated keys for validator #{} (tx: {:?})",
                                        validator.index, receipt.transaction_hash
                                    ),
                                    Err(e) => println!(
                                        "Background: Failed to get receipt for validator #{}: {e:?}",
                                        validator.index
                                    ),
                                },
                                Err(e) => println!(
                                    "Background: Failed to update keys for validator #{}: {e:?}",
                                    validator.index
                                ),
                            }
                        }

                        2 => {
                            // Undelegate from a random delegator of a random validator
                            if registered_validators.is_empty() {
                                continue;
                            }
                            let val_idx = rng.gen_range(0..registered_validators.len());
                            let validator = &registered_validators[val_idx];

                            if !validator.delegators.is_empty() {
                                let del_idx = rng.gen_range(0..validator.delegators.len());
                                let delegator = &validator.delegators[del_idx];

                                let provider = ProviderBuilder::new().connect_http(rpc_url.clone());
                                let stake_table = StakeTableV2::new(stake_table_addr, &provider);

                                if let Ok(delegated_amount) = stake_table
                                    .delegations(validator.address, delegator.address)
                                    .call()
                                    .await
                                    && delegated_amount > U256::ZERO
                                {
                                    let undelegate_amount = delegated_amount / U256::from(2);

                                    match StakeTableV2::new(stake_table_addr, &delegator.provider)
                                        .undelegate(validator.address, undelegate_amount)
                                        .send()
                                        .await
                                    {
                                        Ok(pending) => match pending.get_receipt().await {
                                            Ok(receipt) => println!(
                                                "Background: Undelegated {} ESP from validator #{} by delegator {:?} (tx: {:?})",
                                                undelegate_amount
                                                    / U256::from(10).pow(U256::from(18)),
                                                validator.index,
                                                delegator.address,
                                                receipt.transaction_hash
                                            ),
                                            Err(e) => println!(
                                                "Background: Failed to get undelegate receipt for validator #{}: {e:?}",
                                                validator.index
                                            ),
                                        },
                                        Err(e) => println!(
                                            "Background: Failed to undelegate from validator #{}: {e:?}",
                                            validator.index
                                        ),
                                    }
                                }
                            }
                        }

                        3 => {
                            // Deregister a random validator
                            if registered_validators.is_empty() {
                                continue;
                            }
                            let idx = rng.gen_range(0..registered_validators.len());
                            let validator = &registered_validators[idx];

                            match StakeTableV2::new(stake_table_addr, &validator.provider)
                                .deregisterValidator()
                                .send()
                                .await
                            {
                                Ok(pending) => match pending.get_receipt().await {
                                    Ok(_) => {
                                        println!(
                                            "Background: Validator #{} deregistered",
                                            validator.index,
                                        );

                                        registered_validators.remove(idx);
                                    }
                                    Err(e) => println!(
                                        "Background: Failed to get deregister receipt for validator #{}: {e:?}",
                                        validator.index
                                    ),
                                },
                                Err(e) => {
                                    println!(
                                        "Background: Failed to deregister validator #{}: {e:?}",
                                        validator.index
                                    );
                                }
                            }
                        }

                        _ => unreachable!(),
                    }

                    sleep(Duration::from_millis(500)).await;
                }
            });

            Self {
                task_handle: Some(task_handle),
            }
        }
    }

    impl Drop for BackgroundStakeTableOps {
        fn drop(&mut self) {
            if let Some(handle) = self.task_handle.take() {
                handle.abort();
            }
        }
    }

    #[tokio::test]
    async fn test_rpc_stream_with_anvil() {
        let anvil = Anvil::new().block_time(1).spawn();
        let url = anvil.endpoint().parse::<Url>().unwrap();

        let options = L1ClientOptions {
            http_providers: vec![url],
            ..Default::default()
        };

        let mut stream = RpcStream::new(options, Address::ZERO, Address::ZERO)
            .await
            .unwrap();

        let mut last_block_number = 0;

        for i in 1..=10 {
            println!("Waiting for block {i}");
            let block_input = stream.next().await.expect("Stream ended unexpectedly");

            assert!(block_input.block.number == last_block_number + 1);
            last_block_number = block_input.block.number;
        }
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_rpc_stream_websocket() {
        let anvil = Anvil::new().block_time(1).spawn();
        let http_url = anvil.endpoint().parse::<Url>().unwrap();
        let ws_url = anvil.ws_endpoint().parse::<Url>().unwrap();

        let options = L1ClientOptions {
            http_providers: vec![http_url],
            l1_ws_provider: Some(vec![ws_url]),
            ..Default::default()
        };

        let mut stream = RpcStream::new(options, Address::ZERO, Address::ZERO)
            .await
            .unwrap();

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
    #[test_log::test]
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

        let mut stream = RpcStream::new(options, Address::ZERO, Address::ZERO)
            .await
            .unwrap();

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

    /// Helper function to test reset functionality for RpcStream
    async fn test_reset_logic(stream: &mut RpcStream) {
        let mut last_block_number = 0;
        for i in 1..=10 {
            println!("Waiting for block {i}");
            let block_input = stream.next().await.expect("Stream ended unexpectedly");
            assert_eq!(block_input.block.number, last_block_number + 1);
            last_block_number = block_input.block.number;
        }

        println!("Reached block 10, resetting to block 5");
        stream.reset(5).await;

        let block_input = stream.next().await.expect("Stream ended unexpectedly");
        println!("After reset, received block: {}", block_input.block.number);
        assert_eq!(
            block_input.block.number, 6,
            "Expected block 6 after reset to block 5"
        );

        for i in 7..=15 {
            let block_input = stream.next().await.expect("Stream ended unexpectedly");
            assert_eq!(block_input.block.number, i, "Expected block {i}");
        }

        println!("Reset to genesis");
        stream.reset(0).await;
        let block = stream.next().await.expect("Stream ended unexpectedly");
        assert_eq!(
            block.block.number, 1,
            "Expected block 1 after reset to genesis"
        );

        println!("Reset to block 1");
        stream.reset(1).await;
        let block = stream.next().await.expect("Stream ended unexpectedly");
        assert_eq!(
            block.block.number, 2,
            "Expected block 2 after reset to block 1"
        );

        println!("Reset to current block");
        for _ in 3..=10 {
            stream.next().await.expect("Stream ended unexpectedly");
        }
        stream.reset(10).await;
        let block = stream.next().await.expect("Stream ended unexpectedly");
        assert_eq!(
            block.block.number, 11,
            "Expected block 11 after reset to current"
        );

        stream.reset(5).await;
        let block = stream.next().await.expect("Stream ended unexpectedly");
        assert_eq!(block.block.number, 6);

        stream.reset(3).await;
        let block = stream.next().await.expect("Stream ended unexpectedly");
        assert_eq!(block.block.number, 4);

        stream.reset(15).await;
        let block = stream.next().await.expect("Stream ended unexpectedly");
        assert_eq!(block.block.number, 16);

        for _ in 17..=30 {
            stream.next().await.expect("Stream ended unexpectedly");
        }
        stream.reset(10).await;

        for expected in 11..=20 {
            let block = stream.next().await.expect("Stream ended unexpectedly");
            assert_eq!(block.block.number, expected);
        }

        println!("Reset to future block");
        stream.reset(50).await;
        let block = stream.next().await.expect("Stream ended unexpectedly");
        assert_eq!(
            block.block.number, 51,
            "Expected block 51 after reset to block 50"
        );
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_rpc_stream_reset_http() {
        let anvil = Anvil::new()
            .block_time(2)
            .args(["--slots-in-an-epoch", "0"])
            .spawn();
        let url = anvil.endpoint().parse::<Url>().unwrap();

        let options = L1ClientOptions {
            http_providers: vec![url],
            ..Default::default()
        };

        let mut stream = RpcStream::new(options, Address::ZERO, Address::ZERO)
            .await
            .unwrap();
        println!("Testing HTTP stream reset");

        test_reset_logic(&mut stream).await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_rpc_stream_reset_websocket() {
        let anvil = Anvil::new()
            .block_time(2)
            .args(["--slots-in-an-epoch", "0"])
            .spawn();
        let http_url = anvil.endpoint().parse::<Url>().unwrap();
        let ws_url = anvil.ws_endpoint().parse::<Url>().unwrap();

        let options = L1ClientOptions {
            http_providers: vec![http_url],
            l1_ws_provider: Some(vec![ws_url]),
            ..Default::default()
        };

        let mut stream = RpcStream::new(options, Address::ZERO, Address::ZERO)
            .await
            .unwrap();
        println!("Testing WebSocket stream reset");

        test_reset_logic(&mut stream).await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_genesis_with_deployed_contracts() {
        let anvil = Anvil::new().args(["--slots-in-an-epoch", "0"]).spawn();
        let rpc_url: Url = anvil.endpoint().parse().unwrap();

        let deployment = ContractDeployment::deploy(rpc_url.clone()).await.unwrap();
        let stake_table = deployment.stake_table_addr;

        let provider = ProviderBuilder::new().connect_http(rpc_url.clone());

        provider.anvil_mine(Some(50), None).await.unwrap();

        let options = L1ClientOptions {
            http_providers: vec![rpc_url.clone()],
            ..Default::default()
        };

        let stream = RpcStream::new(options, stake_table, Address::ZERO)
            .await
            .unwrap();

        let (block_id, _timestamp) = stream.genesis(stake_table).await.unwrap();

        assert!(block_id.number > 0, "Block number should be greater than 0");
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_deploy_contracts_helper() {
        let anvil = Anvil::new().args(["--slots-in-an-epoch", "0"]).spawn();
        let rpc_url: Url = anvil.endpoint().parse().unwrap();

        let deployment = ContractDeployment::deploy(rpc_url.clone())
            .await
            .expect("Failed to deploy contracts");

        let stake_table = deployment.stake_table_addr;
        let token = deployment.token_addr;
        let reward_claim = deployment.reward_claim_addr;

        assert_ne!(
            stake_table,
            Address::ZERO,
            "Stake table address should not be zero"
        );
        assert_ne!(token, Address::ZERO, "Token address should not be zero");
        assert_ne!(
            reward_claim,
            Address::ZERO,
            "Reward claim address should not be zero"
        );
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_stake_table_events_basic() {
        let anvil = Anvil::new().args(["--slots-in-an-epoch", "0"]).spawn();
        let rpc_url: Url = anvil.endpoint().parse().unwrap();

        let deployment = ContractDeployment::deploy(rpc_url).await.unwrap();

        let validators = ContractDeployment::create_test_validators(2);

        let options = L1ClientOptions {
            http_providers: vec![deployment.rpc_url.clone()],
            ..Default::default()
        };

        let mut stream = RpcStream::new(
            options,
            deployment.stake_table_addr,
            deployment.reward_claim_addr,
        )
        .await
        .unwrap();

        let _receipts = deployment
            .register_validators(validators, DelegationConfig::VariableAmounts)
            .await
            .expect("Failed to register validators");

        let mut found_stake_event = false;
        'outer: for _ in 0..20 {
            let block_input = stream.next().await.expect("Stream ended unexpectedly");

            for event in &block_input.events {
                match event {
                    L1Event::StakeTable(_) => {
                        found_stake_event = true;
                        break 'outer;
                    }
                    L1Event::Reward(_) => {}
                }
            }
        }

        assert!(
            found_stake_event,
            "Should have found at least one stake event from validator registration"
        );
    }

    /// Test that verifies events correctness
    /// - Spawns a background task that performs random stake table operations (register, delegate, etc.)
    /// - Processes 150 blocks through the RPC stream, applying events to a StakeTableState
    /// - Stops the background task and fetches all events using a range query directly from the endpoint
    /// - Applies the queried events to a fresh StakeTableState
    /// - Compares the state commit of both states they must match
    #[tokio::test]
    #[test_log::test]
    async fn test_stake_table_events() {
        let anvil = Anvil::new()
            .block_time(1)
            .args(["--slots-in-an-epoch", "0"])
            .spawn();
        let rpc_url: Url = anvil.endpoint().parse().unwrap();

        let deployment = ContractDeployment::deploy(rpc_url.clone()).await.unwrap();

        let options = L1ClientOptions {
            http_providers: vec![rpc_url.clone()],
            ..Default::default()
        };

        // Create the rpc stream BEFORE starting the background task.
        // because we are not handling the snapshots here so  the stream starts processing blocks
        // and then events begin appearing in those blocks.
        let mut stream = RpcStream::new(
            options.clone(),
            deployment.stake_table_addr,
            deployment.reward_claim_addr,
        )
        .await
        .unwrap();

        let background_task = deployment.spawn_task();

        let mut stake_table_state_from_stream = StakeTableState::new();
        let start_block = 1u64;
        let mut end_block = start_block;

        for _i in 1..=150 {
            let block_input = stream.next().await.expect("Stream ended unexpectedly");
            end_block = block_input.block.number;

            println!("Block {}", block_input.block.number);

            for event in &block_input.events {
                match event {
                    L1Event::StakeTable(stake_event) => {
                        println!("Stream event: {stake_event:?}");
                        let result =
                            stake_table_state_from_stream.apply_event((**stake_event).clone());
                        match result {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
                                println!("Expected error: {e:?}");
                            }
                            Err(err) => {
                                panic!("Critical stake table error: {err:?}");
                            }
                        }
                    }
                    L1Event::Reward(_) => {}
                }
            }
        }

        drop(background_task);

        println!(
            "Fetching events from blocks {start_block} to {end_block} directly from rpc provider"
        );

        let provider = ProviderBuilder::new().connect_http(rpc_url.clone());

        let filter = Filter::new()
            .events([
                ValidatorRegistered::SIGNATURE,
                ValidatorRegisteredV2::SIGNATURE,
                ValidatorExit::SIGNATURE,
                Delegated::SIGNATURE,
                Undelegated::SIGNATURE,
                ConsensusKeysUpdated::SIGNATURE,
                ConsensusKeysUpdatedV2::SIGNATURE,
                CommissionUpdated::SIGNATURE,
            ])
            .address(deployment.stake_table_addr)
            .from_block(start_block)
            .to_block(end_block);

        let logs = provider
            .get_logs(&filter)
            .await
            .expect("Failed to fetch logs");

        // Apply events from range query to a fresh state
        let mut stake_table_state_from_provider = StakeTableState::new();

        for log in logs {
            let st_event =
                StakeTableV2Events::decode_raw_log(log.topics(), &log.data().data).unwrap();
            let event = st_event.try_into().unwrap();
            let result = stake_table_state_from_provider.apply_event(event);
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    println!("Expected error: {err:?}");
                }
                Err(err) => {
                    panic!("Critical err: {err:?}");
                }
            }
        }

        let stream_commit = stake_table_state_from_stream.commit();
        let provider_commit = stake_table_state_from_provider.commit();

        assert_eq!(stream_commit, provider_commit, "commit mismatch",);
    }
}
