//! Pre-upgrade StakeTable V1 events for the Decaf testnet, embedded as JSON.
//!
//! Extracted once by `extract-decaf-events` and injected during L1 catchup instead of fetched
//! over RPC (see [`CUTOFF_BLOCK`]).

use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock},
};

use alloy::primitives::{Address, BlockHash, address};
use hotshot_contract_adapter::sol_types::StakeTableV3::StakeTableV3Events;
use serde::{Deserialize, Serialize};

use super::L1Event;
use crate::types::common::{L1BlockId, Timestamp};

/// The Decaf StakeTable deployment.
pub const STAKE_TABLE: Address = address!("0x40304fbe94d5e7d1492dd90c53a2d63e8506a037");

/// Sepolia, the only chain the embedded block hashes are valid on.
pub const CHAIN_ID: u64 = 11_155_111;

/// Deployment block of the Decaf StakeTable. Excluded from [`events`]: catchup scans from
/// `from + 1` and must never replay it.
pub const GENESIS_BLOCK: u64 = 8_077_808;

/// Last block of the V1 deployment, containing the V1 -> V3 upgrade transaction. Blocks up to and
/// including this one are served from [`events`] and never fetched over RPC: the V1 encoding,
/// notably the `Upgrade(address)` log in this block, is not decodable by
/// `StakeTableV3Events::decode_raw_log`.
pub const CUTOFF_BLOCK: u64 = 9_803_910;

/// One block's V1 events, as serialized in `decaf_v1_events.json` by `extract-decaf-events`.
#[derive(Clone, Serialize, Deserialize)]
pub struct Block {
    pub number: u64,
    pub hash: BlockHash,
    pub parent: BlockHash,
    pub timestamp: Timestamp,
    pub events: Vec<StakeTableV3Events>,
}

static EVENTS: LazyLock<BTreeMap<L1BlockId, (Timestamp, Vec<L1Event>)>> = LazyLock::new(|| {
    let blocks: Vec<Block> = serde_json::from_str(include_str!("decaf_v1_events.json"))
        .expect("decaf_v1_events.json must deserialize as Vec<Block>");
    blocks
        .into_iter()
        .map(|block| {
            let id = L1BlockId {
                number: block.number,
                hash: block.hash,
                parent: block.parent,
            };
            let events = block
                .events
                .into_iter()
                .map(|event| L1Event::StakeTable(Arc::new(event)))
                .collect();
            (id, (block.timestamp, events))
        })
        .collect()
});

/// Pre-upgrade V1 events, keyed by L1 block.
pub fn events() -> &'static BTreeMap<L1BlockId, (Timestamp, Vec<L1Event>)> {
    &EVENTS
}

/// V1 events in blocks `(from_exclusive, to_inclusive]`.
pub fn events_between(
    from_exclusive: u64,
    to_inclusive: u64,
) -> impl Iterator<Item = (L1BlockId, (Timestamp, Vec<L1Event>))> {
    events()
        .iter()
        .filter(move |(id, _)| id.number > from_exclusive && id.number <= to_inclusive)
        .map(|(id, (timestamp, events))| (*id, (*timestamp, events.clone())))
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::{
        input::l1::{
            BlockInput, L1BlockSnapshot, Snapshot, State,
            provider::GENESIS_EXIT_ESCROW_PERIOD,
            testing::{CatchupFromEvents, MemoryStorage, NoMetadata},
        },
        metrics::PrometheusMetrics,
    };

    #[test_log::test]
    fn test_events_embed_ok() {
        let mut delegated = 0;
        let mut registered = 0;
        let mut undelegated = 0;
        let mut withdrawal_claimed = 0;
        let mut validator_exit = 0;

        for (_, events) in events().values() {
            for event in events {
                let L1Event::StakeTable(event) = event else {
                    panic!("unexpected reward event in V1 data: {event:?}");
                };
                match event.as_ref() {
                    StakeTableV3Events::Delegated(_) => delegated += 1,
                    StakeTableV3Events::ValidatorRegistered(_) => registered += 1,
                    StakeTableV3Events::Undelegated(_) => undelegated += 1,
                    StakeTableV3Events::WithdrawalClaimed(_) => withdrawal_claimed += 1,
                    StakeTableV3Events::ValidatorExit(_) => validator_exit += 1,
                    // `StakeTableV3Events` implements `Serialize` but not `Debug`.
                    other => panic!(
                        "unexpected event type in V1 data: {}",
                        serde_json::to_string(other).unwrap()
                    ),
                }
            }
        }

        assert_eq!(delegated, 168);
        assert_eq!(registered, 140);
        // 3 of the 50 undelegations were claimed; the extractor replaces each claim's V1
        // `Withdrawal` event with a synthetic `WithdrawalClaimed`.
        assert_eq!(undelegated, 50);
        assert_eq!(withdrawal_claimed, 3);
        assert_eq!(validator_exit, 1);
    }

    #[test_log::test]
    fn test_events_between_ok() {
        let (&first, _) = events().iter().next().expect("V1 events must be non-empty");
        let (&last, _) = events()
            .iter()
            .next_back()
            .expect("V1 events must be non-empty");

        let all = events_between(first.number - 1, last.number).count();
        assert_eq!(all, events().len());

        let excluding_first = events_between(first.number, last.number).count();
        assert_eq!(excluding_first, events().len() - 1);

        let none = events_between(first.number - 1, first.number - 1).count();
        assert_eq!(none, 0);
    }

    #[test_log::test(tokio::test)]
    async fn test_replay_clean_state() {
        let genesis = Snapshot::empty(L1BlockSnapshot {
            id: L1BlockId {
                number: GENESIS_BLOCK,
                hash: BlockHash::ZERO,
                parent: BlockHash::ZERO,
            },
            timestamp: 0,
            exit_escrow_period: GENESIS_EXIT_ESCROW_PERIOD,
        });

        let blocks = events().iter().map(|(id, (timestamp, events))| BlockInput {
            block: *id,
            finalized: *id,
            timestamp: *timestamp,
            events: events.clone(),
        });
        let catchup = CatchupFromEvents::from_blocks(blocks);

        let state = State::new(
            MemoryStorage::default(),
            NoMetadata,
            genesis,
            &catchup,
            PrometheusMetrics::default(),
        )
        .await
        .expect("replaying V1 events must not panic or error");

        let snapshot = &state.blocks[0].state;

        // 140 registered, 1 exited. A bad registration parse would have panicked on the first
        // `Delegated` event referencing it.
        assert_eq!(snapshot.node_set.len(), 139);

        // 50 undelegations, 3 claimed, 47 pending.
        let pending_undelegations: usize = snapshot
            .wallets
            .values()
            .map(|wallet| wallet.pending_undelegations.len())
            .sum();
        assert_eq!(pending_undelegations, 47);
    }
}
