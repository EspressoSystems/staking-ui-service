//! Ad hoc L1 client functions.

use std::{collections::BTreeMap, sync::Arc};

use crate::{
    Error, Result,
    error::{ResultExt, ensure},
    input::l1::{L1BlockSnapshot, L1Event},
    types::common::{Address, ESPTokenAmount, L1BlockId, Timestamp},
};
use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    primitives::utils::format_ether,
    providers::Provider,
    rpc::types::{Filter, Log},
    sol_types::SolEventInterface,
};
use hotshot_contract_adapter::sol_types::{
    EspToken,
    RewardClaim::RewardClaimEvents,
    StakeTableV2::{self, StakeTableV2Events},
};
use tracing::instrument;

/// Get the Espresso stake table genesis block.
pub async fn load_genesis(
    provider: &impl Provider,
    stake_table: Address,
) -> Result<L1BlockSnapshot> {
    let stake_table_contract = StakeTableV2::new(stake_table, provider);

    // Fetch the finalized block first.
    // This avoids a race condition where the initialized block could change
    // due to a reorg between fetching it and fetching the finalized block.
    let finalized_block = provider
        .get_block(BlockId::finalized())
        .await
        .map_err(|err| {
            Error::internal().context(format!("Failed to fetch finalized block: {err}"))
        })?
        .ok_or_else(|| Error::internal().context("Finalized block not found"))?;
    // Get the block number when the contract was initialized
    let initialized_at_block = stake_table_contract
        .initializedAtBlock()
        .call()
        .await
        .map_err(|err| {
            Error::internal().context(format!("Failed to retrieve initialization block: {err}"))
        })?
        .to::<u64>();

    let finalized_block_number = finalized_block.header.number;

    ensure!(
        initialized_at_block <= finalized_block_number,
        Error::internal().context(format!(
            "Initialized block {initialized_at_block} must be less than finalized block \
                {finalized_block_number}"
        ))
    );

    let block = provider
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
    let exit_escrow_period = stake_table_contract
        .exitEscrowPeriod()
        .block(BlockId::number(initialized_at_block))
        .call()
        .await
        .map_err(|err| {
            Error::internal().context(format!("Failed to fetch exitEscrowPeriod: {err}"))
        })?
        .to::<u64>();

    let id = L1BlockId {
        number: initialized_at_block,
        hash: block.header.hash,
        parent: block.header.parent_hash,
    };

    Ok(L1BlockSnapshot {
        id,
        timestamp: block.header.timestamp,
        exit_escrow_period,
    })
}

/// Get the amount of Espresso tokens issued in the initial mint event.
#[instrument(skip(provider))]
pub async fn get_initial_token_supply(
    provider: &impl Provider,
    stake_table: Address,
    chunk_size: u64,
) -> Result<ESPTokenAmount> {
    // Get the token contract from the stake table contract.
    let stake_table_contract = StakeTableV2::new(stake_table, provider);
    let token_address =
        stake_table_contract.token().call().await.context(|| {
            Error::internal().context("getting token address from stake table contract")
        })?;
    let token = EspToken::new(token_address, provider);

    // Try a full-range query first, falling back to a chunked backwards scan if the provider
    // rejects it (some enforce a max block range). The mint happens in the original `initializer`,
    // which emits `Initialized(1)`; later versions (e.g. the V2 upgrade) emit higher versions
    // without minting, so we match on version 1 rather than event order.
    let init_log = match token
        .Initialized_filter()
        .from_block(0)
        .to_block(BlockNumberOrTag::Finalized)
        .query()
        .await
    {
        Ok(init_logs) => {
            init_logs
                .into_iter()
                .find(|(event, _)| event.version == 1)
                .ok_or_else(|| Error::internal().context("missing token initialized event"))?
                .1
        }
        Err(err) => {
            tracing::warn!(%err, "full-range token Initialized query failed, falling back to scan");
            let stake_table_init_block = stake_table_contract
                .initializedAtBlock()
                .call()
                .await
                .context(|| Error::internal().context("getting stake table initialization block"))?
                .to::<u64>();
            scan_token_contract_initialized_event_log(
                provider,
                token_address,
                stake_table_init_block,
                chunk_size,
            )
            .await?
        }
    };

    let init_block = init_log
        .block_number
        .ok_or_else(|| Error::internal().context("missing token initialization block number"))?;
    let init_tx_hash = init_log.transaction_hash.ok_or_else(|| {
        Error::internal().context("missing token initialization transaction hash")
    })?;

    // Query Transfer events in the initialization block instead of fetching the transaction
    // receipt, which pruned L1 nodes may not have.
    let transfer_logs = token
        .Transfer_filter()
        .from_block(init_block)
        .to_block(init_block)
        .query()
        .await
        .context(|| Error::internal().context("getting Transfer logs at token init block"))?;

    let (mint_transfer, _) = transfer_logs
        .iter()
        .find(|(_, log)| log.transaction_hash == Some(init_tx_hash))
        .ok_or_else(|| {
            Error::internal().context(format!(
                "token initialization transaction {init_tx_hash} is missing mint transfer"
            ))
        })?;

    tracing::debug!(?mint_transfer, "mint transfer event");
    ensure!(
        mint_transfer.from == Address::ZERO,
        Error::internal().context(format!(
            "mint transfer is from address {}, not zero address",
            mint_transfer.from
        ))
    );

    let initial_supply = mint_transfer.value;
    tracing::info!("Initial token amount: {} ESP", format_ether(initial_supply));
    Ok(initial_supply)
}

/// Bounds the backwards scan. Real deployments have a gap of a handful of blocks (24 on mainnet, 3
/// on decaf), but on slow-deploy testnets with 1s blocks the gap can grow to many days.
const MAX_BLOCKS_SCANNED: u64 = 500_000;

/// Scan backwards from the stake table init block to find the token's `Initialized(1)` event (the
/// original mint). Used when a full-range query is rejected for exceeding the provider's max block
/// range.
async fn scan_token_contract_initialized_event_log(
    provider: &impl Provider,
    token_address: Address,
    stake_table_init_block: u64,
    chunk_size: u64,
) -> Result<Log> {
    let token = EspToken::new(token_address, provider);
    let mut total_scanned = 0u64;
    let mut to_block = stake_table_init_block;
    let mut from_block = stake_table_init_block.saturating_sub(chunk_size);

    loop {
        if total_scanned >= MAX_BLOCKS_SCANNED {
            return Err(Error::internal().context(format!(
                "exceeded max scan range ({MAX_BLOCKS_SCANNED}) while searching for token \
                 Initialized event"
            )));
        }

        let init_logs = token
            .Initialized_filter()
            .from_block(from_block)
            .to_block(to_block)
            .query()
            .await
            .context(|| {
                Error::internal().context(format!(
                    "scanning Initialized events [{from_block}, {to_block}]"
                ))
            })?;

        if let Some((_, init_log)) = init_logs.into_iter().find(|(event, _)| event.version == 1) {
            tracing::info!(from_block, "found token Initialized event during scan");
            return Ok(init_log);
        }

        total_scanned += chunk_size;
        to_block = to_block.saturating_sub(chunk_size);
        from_block = from_block.saturating_sub(chunk_size);
    }
}

pub(super) async fn get_events(
    provider: &impl Provider,
    filter: Filter,
    stake_table_address: Address,
    reward_contract_address: Address,
) -> Result<BTreeMap<L1BlockId, (Timestamp, Vec<L1Event>)>> {
    let filter = filter.address(vec![stake_table_address, reward_contract_address]);
    let logs = provider
        .get_logs(&filter)
        .await
        .context(|| Error::internal().context("getting L1 logs"))?;

    // Decode events from logs
    let mut events = BTreeMap::new();

    for log in logs {
        let hash = log.block_hash.ok_or_else(|| {
            Error::internal().context(format!("event log missing block hash: {log:?}"))
        })?;
        let block = provider
            .get_block(hash.into())
            .await
            .context(|| Error::internal().context(format!("getting header for log {log:?}")))?
            .ok_or_else(|| {
                Error::internal().context(format!("header for log {log:?} not available"))
            })?;
        let id = L1BlockId {
            number: block.number(),
            hash,
            parent: block.header.parent_hash,
        };
        let timestamp = block.header.timestamp;
        let (_, events_for_block) = events.entry(id).or_insert((timestamp, vec![]));

        // Try to decode stake table event
        if log.address() == stake_table_address {
            let event = StakeTableV2Events::decode_raw_log(log.topics(), &log.data().data)
                .unwrap_or_else(|e| {
                    // This is a panic, not an error, as it should be impossible to successfully
                    // retrieve an event from the stake table address but not be able to decode it.
                    panic!(
                        "failed to decode event from stake table {stake_table_address}, tx {:?}: {e:#}",
                        log.transaction_hash
                    );
                });
            events_for_block.push(L1Event::StakeTable(Arc::new(event)));
            continue;
        }

        // Try to decode reward claim event
        if log.address() == reward_contract_address {
            let event = RewardClaimEvents::decode_raw_log(log.topics(), &log.data().data)
                .unwrap_or_else(|e| {
                    panic!(
                        "failed to decode event from reward contract {reward_contract_address}, tx {:?}: {e:#}",
                        log.transaction_hash
                    );
                });
            events_for_block.push(L1Event::Reward(Arc::new(event)));
            continue;
        }

        tracing::warn!(
            ?log,
            %stake_table_address,
            %reward_contract_address,
            "filter returned event which is not from either contract"
        );
    }

    Ok(events)
}

#[cfg(test)]
mod test {
    use alloy::{
        node_bindings::Anvil,
        primitives::{U256, utils::parse_ether},
        providers::{ProviderBuilder, WalletProvider, ext::AnvilApi},
        signers::local::MnemonicBuilder,
    };
    use futures::future::join_all;
    use hotshot_contract_adapter::sol_types::EspTokenV2;
    use rand::{SeedableRng, rngs::StdRng};
    use staking_cli::DEV_MNEMONIC;
    use tide_disco::Url;

    use crate::input::l1::testing::{
        ContractDeployment, DeploymentConfig, assert_events_eq,
        validator_registered_event_with_account,
    };

    use super::*;

    #[tokio::test]
    #[test_log::test]
    async fn test_genesis_with_deployed_contracts() {
        let anvil = Anvil::new().args(["--slots-in-an-epoch", "0"]).spawn();
        let rpc_url: Url = anvil.endpoint().parse().unwrap();

        let deployment = ContractDeployment::deploy(rpc_url.clone()).await.unwrap();
        let stake_table = deployment.stake_table_addr;

        let provider = ProviderBuilder::new().connect_http(rpc_url.clone());

        provider.anvil_mine(Some(50), None).await.unwrap();

        let block = load_genesis(&provider, stake_table).await.unwrap();

        assert!(block.number() > 0, "Block number should be greater than 0");
    }

    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn test_genesis_exit_escrow_period() {
        let anvil = Anvil::new()
            .args(["--slots-in-an-epoch", "0"])
            .block_time(1)
            .spawn();
        let deployment = ContractDeployment::deploy(anvil.endpoint_url())
            .await
            .unwrap();
        let provider = ProviderBuilder::new()
            .wallet(
                MnemonicBuilder::english()
                    .phrase(DEV_MNEMONIC)
                    .build()
                    .unwrap(),
            )
            .connect_http(anvil.endpoint_url());

        let stake_table_address = deployment.stake_table_addr;
        let contract = StakeTableV2::new(stake_table_address, &provider);

        // Change the exit escrow period, to verify that the genesis snapshot loads the exit escrow
        // period from the time when the contract was initialized, not what it is now.
        let genesis_exit_escrow_period: u64 = contract
            .exitEscrowPeriod()
            .call()
            .await
            .unwrap()
            .try_into()
            .unwrap();
        let receipt = contract
            // Add one day
            .updateExitEscrowPeriod(genesis_exit_escrow_period + 86_400)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        assert!(receipt.status());

        let genesis = load_genesis(&provider, *contract.address()).await.unwrap();
        assert_eq!(genesis.exit_escrow_period, genesis_exit_escrow_period);
    }

    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn test_get_events_multiple_events_per_block() {
        // Start Anvil with on-demand mining, so the contract deployment is fast.
        let anvil = Anvil::new().spawn();
        let rpc_url: Url = anvil.endpoint().parse().unwrap();
        let deployment = ContractDeployment::deploy(rpc_url.clone()).await.unwrap();

        // Once contracts are deployed, set a pretty long block time, so multiple transactions can
        // end up in the same block.
        let provider = ProviderBuilder::new().connect_http(rpc_url);
        provider.anvil_set_interval_mining(12).await.unwrap();

        // Register two nodes at the same time and wait for the transactions to get mined, then do
        // it again in another block.
        let mut blocks = [0; 2];
        let mut events = [vec![], vec![]];
        for (i, block) in blocks.iter_mut().enumerate() {
            let mut results = join_all((0..2).map(|j| {
                let index = 2 * i + j;
                let provider = ProviderBuilder::new()
                    .wallet(
                        MnemonicBuilder::english()
                            .phrase(DEV_MNEMONIC)
                            .index(index as u32)
                            .unwrap()
                            .build()
                            .unwrap(),
                    )
                    .connect_http(anvil.endpoint_url());
                let address = provider.default_signer_address();
                let node = validator_registered_event_with_account(
                    StdRng::seed_from_u64(index as u64),
                    address,
                );
                let stake_table = StakeTableV2::new(deployment.stake_table_addr, provider.clone());
                tracing::info!(index, %address, "submitting registration");
                async move {
                    let tx = stake_table
                        .registerValidatorV2(
                            node.blsVK,
                            node.schnorrVK,
                            node.blsSig,
                            node.schnorrSig.clone(),
                            node.commission,
                            "https://example.com/validator-metadata.json".to_string(),
                        )
                        .send()
                        .await
                        .unwrap();
                    tracing::info!(index, "transaction submitted, waiting for receipt");
                    let receipt = tx.get_receipt().await.unwrap();
                    assert!(receipt.status());
                    tracing::info!(index, "transaction mined");

                    let expected_event = L1Event::StakeTable(Arc::new(
                        StakeTableV2Events::ValidatorRegisteredV2(node),
                    ));
                    (receipt, expected_event)
                }
            }))
            .await;

            // Put the transaction results in the order they appeared within the block (the order in
            // which we expect to see the events later when we query them from the provider).
            results.sort_by_key(|(receipt, _)| receipt.transaction_index);
            let (receipts, block_events): (Vec<_>, Vec<_>) = results.into_iter().unzip();

            // Sanity check the transactions did get included in the same block.
            assert_eq!(receipts[0].block_number, receipts[1].block_number);
            tracing::info!("two registrations mined in block {block}");

            // Remember the block, and the events we expect to have been emitted, so we can later
            // check against the provider.
            *block = receipts[0].block_number.unwrap();
            events[i] = block_events;
        }
        assert_ne!(blocks[0], blocks[1]);

        // Now we have two different blocks with two events each. Retrieve the events and see if it
        // matches.
        let events_from_provider = get_events(
            &provider,
            Filter::new().from_block(blocks[0]),
            deployment.stake_table_addr,
            deployment.reward_claim_addr,
        )
        .await
        .unwrap();
        tracing::info!("retrieved events from provider: {events_from_provider:#?}");
        assert_eq!(events_from_provider.len(), 2);

        let (id, (_, block_events)) = events_from_provider.first_key_value().unwrap();
        assert_eq!(id.number, blocks[0]);
        assert_eq!(block_events.len(), 2);
        assert_events_eq(&block_events[0], &events[0][0]);
        assert_events_eq(&block_events[1], &events[0][1]);

        let (id, (_, block_events)) = events_from_provider.last_key_value().unwrap();
        assert_eq!(id.number, blocks[1]);
        assert_eq!(block_events.len(), 2);
        assert_events_eq(&block_events[0], &events[1][0]);
        assert_events_eq(&block_events[1], &events[1][1]);
    }

    #[test_log::test(tokio::test)]
    async fn test_initial_token_supply() {
        let initial_token_supply = 42;

        let anvil = Anvil::new().args(["--slots-in-an-epoch", "0"]).spawn();
        let rpc_url: Url = anvil.endpoint().parse().unwrap();
        let config = DeploymentConfig {
            initial_token_supply,
            ..Default::default()
        };
        let deployment = ContractDeployment::deploy_with_config(rpc_url.clone(), config)
            .await
            .unwrap();
        let provider = ProviderBuilder::new().connect_http(rpc_url);

        // Send a couple of other token transfer events with different amounts, including one mint,
        // to be sure that `get_initial_token_supply` correctly fetches the initial mint event.
        let token = EspTokenV2::new(deployment.token_addr, &provider);
        let decimals = token.decimals().call().await.unwrap();

        provider.anvil_auto_impersonate_account(true).await.unwrap();

        // Normal transfer event.
        token
            .transfer(Address::random(), ESPTokenAmount::ONE)
            .from(deployment.admin)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        // Send ETH to the reward claim contract so we can send a mint transaction from that address.
        provider
            .anvil_set_balance(deployment.reward_claim_addr, U256::MAX)
            .await
            .unwrap();

        // Mint event.
        token
            .mint(Address::random(), ESPTokenAmount::ONE)
            .from(deployment.reward_claim_addr)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        provider
            .anvil_auto_impersonate_account(false)
            .await
            .unwrap();

        assert_eq!(
            get_initial_token_supply(&provider, deployment.stake_table_addr, 10_000)
                .await
                .unwrap(),
            U256::try_from(initial_token_supply).unwrap()
                * U256::try_from(10)
                    .unwrap()
                    .pow(decimals.try_into().unwrap())
        );

        // The chunked scan fallback finds the same Initialized event as the full-range query.
        let full_init_log = token
            .Initialized_filter()
            .from_block(0)
            .to_block(BlockNumberOrTag::Finalized)
            .query()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .1;
        let stake_table_init_block = StakeTableV2::new(deployment.stake_table_addr, &provider)
            .initializedAtBlock()
            .call()
            .await
            .unwrap()
            .to::<u64>();
        let scan_init_log = scan_token_contract_initialized_event_log(
            &provider,
            deployment.token_addr,
            stake_table_init_block,
            5,
        )
        .await
        .unwrap();
        assert_eq!(scan_init_log.block_number, full_init_log.block_number);
        assert_eq!(
            scan_init_log.transaction_hash,
            full_init_log.transaction_hash
        );

        // Regression: the token is upgraded to V2 via `reinitializer(2)`, which emits a second
        // `Initialized(2)` event at a higher block than the original mint's `Initialized(1)`. When
        // the scan starts above the V2 event, it must skip it and return the version 1 event with
        // the mint, not the most recent one.
        let all_init_logs = token
            .Initialized_filter()
            .from_block(0)
            .to_block(BlockNumberOrTag::Finalized)
            .query()
            .await
            .unwrap();
        let v2_block = all_init_logs
            .iter()
            .find(|(event, _)| event.version == 2)
            .expect("token V2 upgrade emits Initialized(2)")
            .1
            .block_number
            .unwrap();
        let scan_above_v2 = scan_token_contract_initialized_event_log(
            &provider,
            deployment.token_addr,
            v2_block,
            1,
        )
        .await
        .unwrap();
        assert_eq!(scan_above_v2.block_number, full_init_log.block_number);
        assert_eq!(
            scan_above_v2.transaction_hash,
            full_init_log.transaction_hash
        );
    }

    #[ignore]
    #[test_log::test(tokio::test)]
    async fn test_get_initial_token_supply_decaf() {
        let provider = ProviderBuilder::new()
            .connect_http("https://ethereum-sepolia.publicnode.com".parse().unwrap());
        let stake_table: Address = "0x40304fbe94d5e7d1492dd90c53a2d63e8506a037"
            .parse()
            .unwrap();
        let supply = get_initial_token_supply(&provider, stake_table, 100)
            .await
            .unwrap();
        assert_eq!(supply, parse_ether("10000000000").unwrap());
    }

    #[ignore]
    #[test_log::test(tokio::test)]
    async fn test_get_initial_token_supply_mainnet() {
        let provider = ProviderBuilder::new()
            .connect_http("https://ethereum-rpc.publicnode.com".parse().unwrap());
        let stake_table: Address = "0xcef474d372b5b09defe2af187bf17338dc704451"
            .parse()
            .unwrap();
        let supply = get_initial_token_supply(&provider, stake_table, 100)
            .await
            .unwrap();
        assert_eq!(supply, parse_ether("3590000000").unwrap());
    }
}
