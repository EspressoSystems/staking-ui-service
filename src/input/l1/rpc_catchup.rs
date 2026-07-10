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

    /// Pre-upgrade Decaf events, present only when catching up the Decaf stake table (see
    /// [`decaf`]). The V1-era block range is served from here instead of the RPC provider.
    legacy: Option<&'static BTreeMap<L1BlockId, (Timestamp, Vec<L1Event>)>>,
}

impl RpcCatchup {
    /// A catchup provider configured from the CLI.
    pub fn new(opt: &L1ClientOptions) -> Result<Self> {
        let provider = opt.provider()?.0;
        let legacy =
            (opt.stake_table_address == decaf::DECAF_STAKE_TABLE).then(decaf::legacy_events);
        Ok(Self {
            provider,
            stake_table_addr: opt.stake_table_address,
            reward_addr: opt.reward_contract_address,
            chunk_size: opt.l1_events_max_block_range,
            retry_delay: opt.l1_retry_delay,
            legacy,
        })
    }
}

impl L1Catchup for RpcCatchup {
    async fn fast_forward(
        &self,
        from: u64,
    ) -> Result<BTreeMap<L1BlockId, (Timestamp, Vec<L1Event>)>> {
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

        // Fetch events from the starting block to the new finalized block.
        tracing::info!(
            from,
            to = finalized.number(),
            "fetching L1 events for catchup"
        );

        // To avoid making large RPC calls, divide the range into smaller chunks. When replaying a
        // legacy-gated stake table, the V1-era range is served from committed data instead (see
        // `legacy_clamped_from`), so the RPC scan is clamped to start after it.
        let target = finalized.number();
        let rpc_from = legacy_clamped_from(from, self.legacy.is_some());
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

        if self.legacy.is_some() {
            events.extend(decaf::legacy_events_between(from, target));
        }

        Ok(events)
    }
}

/// Clamp the RPC scan's starting block so the legacy V1 event range is never fetched from the
/// provider when legacy replay is active for this stake table (its final block emits an
/// undecodable `Upgrade` event; see `decaf`).
fn legacy_clamped_from(from: u64, legacy_active: bool) -> u64 {
    if legacy_active {
        from.max(decaf::LEGACY_CUTOFF_BLOCK)
    } else {
        from
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
        let catchup = RpcCatchup::new(&dummy_options(decaf::DECAF_STAKE_TABLE)).unwrap();
        assert!(catchup.legacy.is_some());
    }

    #[test_log::test]
    fn test_decaf_gate_other_address() {
        let catchup = RpcCatchup::new(&dummy_options(Address::random())).unwrap();
        assert!(catchup.legacy.is_none());
    }

    #[test_log::test]
    fn test_legacy_clamped_from_inactive() {
        // Without the gate active, the RPC scan range is untouched.
        assert_eq!(legacy_clamped_from(0, false), 0);
        assert_eq!(
            legacy_clamped_from(decaf::LEGACY_CUTOFF_BLOCK + 1, false),
            decaf::LEGACY_CUTOFF_BLOCK + 1
        );
    }

    #[test_log::test]
    fn test_legacy_clamped_from_cutoff_boundary() {
        // Starting from genesis (or anywhere inside the legacy range), the RPC scan is clamped to
        // begin exactly at the cutoff, so `block_range_chunks(rpc_from + 1, ..)` never requests
        // the cutoff block itself (its `Upgrade` log cannot be decoded).
        assert_eq!(legacy_clamped_from(0, true), decaf::LEGACY_CUTOFF_BLOCK);
        assert_eq!(
            legacy_clamped_from(decaf::LEGACY_CUTOFF_BLOCK, true),
            decaf::LEGACY_CUTOFF_BLOCK
        );
    }

    #[test_log::test]
    fn test_legacy_clamped_from_past_cutoff() {
        // Restarting from a snapshot already past the cutoff leaves the RPC range unchanged.
        let from = decaf::LEGACY_CUTOFF_BLOCK + 1000;
        assert_eq!(legacy_clamped_from(from, true), from);
    }

    #[test_log::test]
    fn test_catchup_skips_legacy_range() {
        // With the gate active, no chunk ever starts at or below the cutoff block.
        let rpc_from = legacy_clamped_from(0, true);
        let chunks = block_range_chunks(rpc_from + 1, decaf::LEGACY_CUTOFF_BLOCK + 10_000, 3);
        for (from, _) in chunks {
            assert!(from > decaf::LEGACY_CUTOFF_BLOCK);
        }
    }

    #[test_log::test]
    fn test_catchup_merges_legacy_events() {
        let (&first, _) = decaf::legacy_events().iter().next().unwrap();
        let (&last, _) = decaf::legacy_events().iter().next_back().unwrap();

        let merged: BTreeMap<_, _> =
            decaf::legacy_events_between(first.number - 1, last.number).collect();
        assert_eq!(merged.len(), decaf::legacy_events().len());
        // Ordered by block number, since `L1BlockId`'s `Ord` compares `number` first.
        assert!(merged.keys().is_sorted_by_key(|id| id.number));
    }

    #[test_log::test]
    fn test_catchup_restart_mid_legacy() {
        let (&first, _) = decaf::legacy_events().iter().next().unwrap();
        let (&last, _) = decaf::legacy_events().iter().next_back().unwrap();

        // Restarting right at `first` merges everything strictly after it, and never re-applies
        // `first` itself.
        let merged: Vec<_> = decaf::legacy_events_between(first.number, last.number).collect();
        assert!(merged.iter().all(|(id, _)| id.number > first.number));
        assert_eq!(merged.len(), decaf::legacy_events().len() - 1);
    }

    #[test_log::test]
    fn test_catchup_finalized_inside_legacy() {
        let (&first, _) = decaf::legacy_events().iter().next().unwrap();

        // A finalized head below the cutoff yields no RPC chunks (the clamped start is past the
        // finalized target), and only legacy entries up to that head merge in.
        let target = first.number;
        let rpc_from = legacy_clamped_from(0, true);
        let chunks = block_range_chunks(rpc_from + 1, target, 3).collect::<Vec<_>>();
        assert!(chunks.is_empty());

        let merged: Vec<_> = decaf::legacy_events_between(0, target).collect();
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].0, first);
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

    /// The decisive proof that the Decaf-specific `release-decaf` branch can be retired: replaying
    /// the real Decaf stake table from genesis through the current finalized block, live legacy
    /// injection and all, must not panic. This is the only test that exercises the post-upgrade
    /// `WithdrawalClaimed(undelegationId=0)` events (claims of V1-era undelegations) against the
    /// hardcoded legacy state.
    #[ignore]
    #[test_log::test(tokio::test)]
    async fn test_decaf_sepolia_full_replay() {
        let rpc_url: Url = std::env::var("DECAF_SEPOLIA_RPC_URL")
            .unwrap_or_else(|_| "https://sepolia.gateway.tenderly.co".into())
            .parse()
            .unwrap();

        let provider = ProviderBuilder::new().connect_http(rpc_url.clone());
        let genesis_block = load_genesis(&provider, decaf::DECAF_STAKE_TABLE)
            .await
            .unwrap();
        let mut snapshot = Snapshot::empty(genesis_block);

        let options = L1ClientOptions {
            http_providers: vec![rpc_url],
            stake_table_address: decaf::DECAF_STAKE_TABLE,
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
    }
}
