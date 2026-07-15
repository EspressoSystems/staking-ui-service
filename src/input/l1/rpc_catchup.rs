//! L1 catchup based on a JSON-RPC provider.

use std::{cmp::min, collections::BTreeMap, time::Duration};

use alloy::{
    eips::BlockId,
    providers::{Provider, RootProvider},
    rpc::types::Filter,
};
use tokio::time::sleep;

use crate::{
    Error, Result,
    error::ResultExt,
    input::l1::{L1Catchup, L1Event, decaf, options::L1ClientOptions, provider::get_events},
    types::common::{Address, L1BlockId, Timestamp},
};

/// L1 catchup based on a JSON-RPC provider.
pub struct RpcCatchup {
    provider: RootProvider,
    stake_table_addr: Address,
    reward_addr: Address,
    chunk_size: u64,
    retry_delay: Duration,

    /// Whether this catchup targets the Decaf stake table (see [`decaf`]). If so, the V1-era
    /// block range is served from embedded events instead of the RPC provider.
    decaf: bool,
}

impl RpcCatchup {
    /// A catchup provider configured from the CLI.
    pub fn new(opt: &L1ClientOptions) -> Result<Self> {
        let provider = opt.provider()?.0;
        Ok(Self {
            provider,
            stake_table_addr: opt.stake_table_address,
            reward_addr: opt.reward_contract_address,
            chunk_size: opt.l1_events_max_block_range,
            retry_delay: opt.l1_retry_delay,
            decaf: opt.stake_table_address == decaf::STAKE_TABLE,
        })
    }
}

impl L1Catchup for RpcCatchup {
    async fn fast_forward(
        &self,
        from: u64,
    ) -> Result<BTreeMap<L1BlockId, (Timestamp, Vec<L1Event>)>> {
        if self.decaf {
            // The embedded V1 events carry Sepolia block hashes; injecting them on any other
            // chain would corrupt the state.
            let chain_id = self
                .provider
                .get_chain_id()
                .await
                .context(|| Error::internal().context("getting chain ID"))?;
            if chain_id != decaf::CHAIN_ID {
                return Err(Error::internal().context(format!(
                    "stake table address matches Decaf ({}) but chain ID is {chain_id}, \
                     expected Sepolia ({})",
                    decaf::STAKE_TABLE,
                    decaf::CHAIN_ID,
                )));
            }
        }

        let finalized = self
            .provider
            .get_block(BlockId::finalized())
            .await
            .context(|| Error::internal().context("getting finalized block"))?;
        let Some(finalized) = finalized else {
            // If there is no finalized L1 block yet, then our starting block `from` cannot possibly
            // be behind. This occurs when the L1 has just been started, as in testing environments.
            tracing::info!("no finalized L1 block");
            return Ok(Default::default());
        };
        if finalized.number() <= from {
            tracing::info!(from, ?finalized, "current state is not behind");
            return Ok(Default::default());
        }

        let target = finalized.number();
        // On Decaf, the V1 range is served from embedded data (its final block emits an
        // undecodable `Upgrade` event), so the RPC scan is clamped to start after it.
        let rpc_from = if self.decaf {
            from.max(decaf::CUTOFF_BLOCK)
        } else {
            from
        };

        // Fetch events from the starting block to the new finalized block.
        tracing::info!(
            from,
            rpc_from,
            to = target,
            "fetching L1 events for catchup"
        );

        // To avoid making large RPC calls, divide the range into smaller chunks.
        let chunks = block_range_chunks(rpc_from + 1, target, self.chunk_size);

        let max_delay = self.retry_delay * 32;
        let mut events = BTreeMap::new();
        for (from, to) in chunks {
            tracing::debug!(from, to, target, "catchup progress");
            let mut delay = self.retry_delay;
            let mut attempt = 0u32;
            let chunk_events = loop {
                match get_events(
                    &self.provider,
                    Filter::new().from_block(from).to_block(to),
                    self.stake_table_addr,
                    self.reward_addr,
                )
                .await
                {
                    Ok(events) => break events,
                    Err(err) => {
                        attempt += 1;
                        tracing::warn!(from, to, attempt, ?err, "fetch L1 events failed, retrying");
                        sleep(delay).await;
                        delay = (delay * 2).min(max_delay);
                    }
                }
            };
            events.extend(chunk_events);
        }

        if self.decaf {
            events.extend(decaf::events_between(from, target));
        }

        Ok(events)
    }
}

fn block_range_chunks(
    from_block: u64,
    to_block: u64,
    chunk_size: u64,
) -> impl Iterator<Item = (u64, u64)> {
    let mut start = from_block;
    let end = to_block;
    std::iter::from_fn(move || {
        let chunk_end = min(start + chunk_size - 1, end);
        if chunk_end < start {
            return None;
        }
        let chunk = (start, chunk_end);
        start = chunk_end + 1;
        Some(chunk)
    })
}

#[cfg(test)]
mod test {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use alloy::{node_bindings::Anvil, providers::ProviderBuilder};
    use futures::StreamExt;
    use pretty_assertions::assert_eq;
    use tide_disco::Url;
    use tokio::time::sleep;

    use crate::input::l1::{
        ResettableStream, RpcStream, Snapshot,
        provider::load_genesis,
        testing::{BackgroundStakeTableOps, ContractDeployment, NoMetadata, assert_events_eq},
    };

    use super::*;

    #[test_log::test]
    fn test_block_range_chunks_exact_multiple() {
        let chunks = block_range_chunks(0, 3, 2).collect::<Vec<_>>();
        assert_eq!(chunks, vec![(0, 1), (2, 3)]);
    }

    #[test_log::test]
    fn test_block_range_chunks_partial_chunk() {
        let chunks = block_range_chunks(0, 4, 2).collect::<Vec<_>>();
        assert_eq!(chunks, vec![(0, 1), (2, 3), (4, 4)]);
    }

    #[test_log::test]
    fn test_block_range_chunks_small() {
        let chunks = block_range_chunks(1, 1, 10).collect::<Vec<_>>();
        assert_eq!(chunks, vec![(1, 1)]);
    }

    /// A dummy provider URL. Constructing an [`RpcCatchup`] never connects eagerly, so this never
    /// needs to be reachable.
    fn dummy_options(stake_table_address: Address) -> L1ClientOptions {
        L1ClientOptions {
            http_providers: vec!["http://localhost:1".parse().unwrap()],
            stake_table_address,
            reward_contract_address: Address::ZERO,
            ..Default::default()
        }
    }

    #[test_log::test]
    fn test_decaf_gate_address_match() {
        let catchup = RpcCatchup::new(&dummy_options(decaf::STAKE_TABLE)).unwrap();
        assert!(catchup.decaf);
    }

    #[test_log::test]
    fn test_decaf_gate_other_address() {
        let catchup = RpcCatchup::new(&dummy_options(Address::random())).unwrap();
        assert!(!catchup.decaf);
    }

    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn test_fast_forward_not_behind() {
        let anvil = Anvil::new()
            .block_time(1)
            .args(["--slots-in-an-epoch", "0"])
            .spawn();
        let deployment = ContractDeployment::deploy(anvil.endpoint_url())
            .await
            .unwrap();
        let options = L1ClientOptions {
            http_providers: vec![anvil.endpoint_url()],
            stake_table_address: deployment.stake_table_addr,
            reward_contract_address: deployment.reward_claim_addr,
            ..Default::default()
        };
        let catchup = RpcCatchup::new(&options).unwrap();
        let res = catchup.fast_forward(1000000).await.unwrap();
        assert!(res.is_empty(), "expected empty list, got {res:?}");
    }

    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn test_fast_forward_consistency() {
        // Spawn an L1 and generate a bunch of events.
        let anvil = Anvil::new()
            .block_time(1)
            .args(["--slots-in-an-epoch", "0"])
            .spawn();
        let rpc_url = anvil.endpoint_url();

        let deployment = ContractDeployment::deploy(rpc_url.clone()).await.unwrap();
        let options = L1ClientOptions {
            http_providers: vec![rpc_url.clone()],
            stake_table_address: deployment.stake_table_addr,
            reward_contract_address: deployment.reward_claim_addr,
            l1_events_max_block_range: 3,
            ..Default::default()
        };
        let provider = options.provider().unwrap().0;

        // Generate events for about 100 blocks.
        let cancel: Arc<AtomicBool> = Arc::new(false.into());
        let task = BackgroundStakeTableOps::spawn(
            rpc_url,
            deployment.stake_table_addr,
            cancel.clone(),
            None,
        );
        sleep(Duration::from_secs(100)).await;
        cancel.store(true, Ordering::SeqCst);
        task.join().await;
        let end_block = provider.get_block_number().await.unwrap();
        tracing::info!(end_block, "finished generating events");

        // Stream events from genesis.
        let mut stream = RpcStream::new(options.clone()).await.unwrap();
        stream.reset(0).await;
        let events_from_stream = stream.take(end_block as usize).collect::<Vec<_>>().await;
        tracing::info!(
            "finished streaming events from {} blocks",
            events_from_stream.len()
        );

        // Fast-forward to the finalized block.
        let catchup = RpcCatchup::new(&options).unwrap();
        let mut catchup_events = catchup.fast_forward(0).await.unwrap();
        // let catchup_finalized = catchup_events.last().unwrap();
        // let events = catchup_events.iter().flat_map(|(_, _, events)| events);
        tracing::info!(
            "fast forwarded events from {} non-empty blocks",
            catchup_events.len()
        );

        // We get the same events either way.
        for input in events_from_stream {
            // We don't skip any inputs during catchup, except empty ones.
            // Remove the input from `catchup_events` so that we can check that it does not contain
            // any extra inputs (at the end it should be empty).
            let Some((timestamp, events)) = catchup_events.remove(&input.block) else {
                assert!(
                    input.events.is_empty(),
                    "missing input with non-empty events {input:?}"
                );
                continue;
            };
            assert_eq!(timestamp, input.timestamp);
            assert_eq!(input.events.len(), events.len());
            for (i, (event, event_from_stream)) in input.events.iter().zip(&events).enumerate() {
                tracing::info!("checking events at input {:?} position {i}", input.block);
                assert_events_eq(event, event_from_stream);
            }
        }
    }

    /// Replays the real Decaf stake table from genesis through the current finalized block,
    /// injecting the embedded V1 events. This exercises the post-upgrade
    /// `WithdrawalClaimed(undelegationId=0)` events (claims of V1-era undelegations) against the
    /// embedded V1 state.
    #[ignore]
    #[test_log::test(tokio::test)]
    async fn test_decaf_sepolia_full_replay() {
        let rpc_url: Url = std::env::var("DECAF_SEPOLIA_RPC_URL")
            .unwrap_or_else(|_| "https://sepolia.gateway.tenderly.co".into())
            .parse()
            .unwrap();

        let provider = ProviderBuilder::new().connect_http(rpc_url.clone());
        let genesis_block = load_genesis(&provider, decaf::STAKE_TABLE).await.unwrap();
        let mut snapshot = Snapshot::empty(genesis_block);

        let options = L1ClientOptions {
            http_providers: vec![rpc_url],
            stake_table_address: decaf::STAKE_TABLE,
            reward_contract_address: Address::ZERO,
            ..Default::default()
        };
        let catchup = RpcCatchup::new(&options).unwrap();
        let events = catchup.fast_forward(snapshot.block.number()).await.unwrap();
        tracing::info!(blocks = events.len(), "replaying catchup events");

        for (id, (timestamp, block_events)) in events {
            snapshot
                .apply(&NoMetadata, id, timestamp, &block_events)
                .await;
        }

        tracing::info!(
            nodes = snapshot.node_set.len(),
            wallets = snapshot.wallets.len(),
            "replay finished without panicking"
        );
        assert!(!snapshot.node_set.is_empty());
        assert!(!snapshot.wallets.is_empty());
    }
}
