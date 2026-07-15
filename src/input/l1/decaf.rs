//! Pre-upgrade StakeTable V1 event history for the Decaf testnet, committed as JSON.
//!
//! Decaf's stake table was upgraded from a V1 to a V3 deployment partway through its history (see
//! [`CUTOFF_BLOCK`]). The V1 event encoding is not decodable by
//! `StakeTableV3Events::decode_raw_log` (notably the V1 `Upgrade(address)` event has no V3
//! equivalent), so replaying that range live from the RPC provider would panic. Instead, the
//! events in that range are extracted once (see `src/bin/extract-decaf-events.rs`) and embedded
//! here, to be injected during L1 catchup (see `rpc_catchup.rs`) rather than fetched.

use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock},
};

use alloy::primitives::{Address, BlockHash, address};
use hotshot_contract_adapter::sol_types::StakeTableV3::StakeTableV3Events;
use serde::{Deserialize, Serialize};

use super::L1Event;
use crate::types::common::{L1BlockId, Timestamp};

/// The Decaf StakeTable contract deployment carrying pre-upgrade V1 event history.
pub const STAKE_TABLE: Address = address!("0x40304fbe94d5e7d1492dd90c53a2d63e8506a037");

/// The chain the Decaf StakeTable is deployed on (Sepolia). The embedded block hashes are only
/// valid on this chain.
pub const CHAIN_ID: u64 = 11_155_111;

/// The block the Decaf StakeTable contract was initialized at. Dropped from [`events`] entirely,
/// since `fast_forward` starts scanning at `from + 1` and must never replay it.
pub const GENESIS_BLOCK: u64 = 8_077_808;

/// Last block using the V1 event encoding, i.e. the V1 -> V3 upgrade transaction. Blocks up to
/// and including this one are served from [`events`] instead of fetched over RPC.
pub const CUTOFF_BLOCK: u64 = 9_803_910;

/// One L1 block's worth of pre-upgrade events, as embedded in `decaf_v1_events.json`.
///
/// Shared with `extract-decaf-events`, so serialization and deserialization cannot drift.
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
        // 50 undelegations total; 3 were later claimed, each replacing a V1 `Withdrawal` event
        // (which carries no validator) with a synthetic `WithdrawalClaimed` event. The other 47
        // undelegations remain pending in state after replay (see `test_replay_clean_state`).
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

        // Everything is included when the range covers the whole dataset.
        let all = events_between(first.number - 1, last.number).count();
        assert_eq!(all, events().len());

        // Nothing at or before `from_exclusive` is included.
        let excluding_first = events_between(first.number, last.number).count();
        assert_eq!(excluding_first, events().len() - 1);

        // Nothing past `to_inclusive` is included.
        let none = events_between(first.number - 1, first.number - 1).count();
        assert_eq!(none, 0);
    }

    #[test_log::test(tokio::test)]
    async fn test_replay_clean_state() {
        // `exitEscrowPeriod()` at the time of the real deployment's genesis.
        const EXIT_ESCROW_PERIOD: u64 = 604_800;

        let genesis = Snapshot::empty(L1BlockSnapshot {
            id: L1BlockId {
                number: GENESIS_BLOCK,
                hash: BlockHash::ZERO,
                parent: BlockHash::ZERO,
            },
            timestamp: 0,
            exit_escrow_period: EXIT_ESCROW_PERIOD,
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

        // All 140 registrations parsed (a bad parse would have panicked on the first `Delegated`
        // event referencing an unregistered validator); 1 later exits, leaving 139 active.
        assert_eq!(snapshot.node_set.len(), 139);

        // 50 undelegations total, 3 claimed by the synthetic `WithdrawalClaimed` events, leaving
        // 47 pending.
        let pending_undelegations: usize = snapshot
            .wallets
            .values()
            .map(|wallet| wallet.pending_undelegations.len())
            .sum();
        assert_eq!(pending_undelegations, 47);
    }
}
