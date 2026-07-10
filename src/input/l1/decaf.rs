//! Pre-upgrade StakeTable V1 event history for the Decaf testnet, committed as JSON.
//!
//! Decaf's stake table was upgraded from a V1 to a V3 deployment partway through its history (see
//! [`LEGACY_CUTOFF_BLOCK`]). The V1 event encoding is not decodable by
//! `StakeTableV3Events::decode_raw_log` (notably the legacy `Upgrade(address)` event has no V3
//! equivalent), so replaying that range live from the RPC provider would panic. Instead, the
//! events in that range are extracted once (see `src/bin/extract-decaf-legacy-events.rs`) and
//! embedded here, to be injected during L1 catchup (see `rpc_catchup.rs`) rather than fetched.

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
pub const DECAF_STAKE_TABLE: Address = address!("0x40304fbe94d5e7d1492dd90c53a2d63e8506a037");

/// Last block using the V1 event encoding, i.e. the V1 -> V3 upgrade transaction. Blocks up to
/// and including this one are served from [`legacy_events`] instead of fetched over RPC.
pub const LEGACY_CUTOFF_BLOCK: u64 = 9_803_910;

/// One L1 block's worth of pre-upgrade events, as embedded in `decaf_legacy_events.json`.
///
/// Shared with `extract-decaf-legacy-events`, so serialization and deserialization cannot drift.
#[derive(Clone, Serialize, Deserialize)]
pub struct LegacyBlock {
    pub number: u64,
    pub hash: BlockHash,
    pub parent: BlockHash,
    pub timestamp: Timestamp,
    pub events: Vec<StakeTableV3Events>,
}

static LEGACY_EVENTS: LazyLock<BTreeMap<L1BlockId, (Timestamp, Vec<L1Event>)>> =
    LazyLock::new(|| {
        let blocks: Vec<LegacyBlock> =
            serde_json::from_str(include_str!("decaf_legacy_events.json"))
                .expect("decaf_legacy_events.json must deserialize as Vec<LegacyBlock>");
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

/// Pre-upgrade legacy events, keyed by L1 block.
pub fn legacy_events() -> &'static BTreeMap<L1BlockId, (Timestamp, Vec<L1Event>)> {
    &LEGACY_EVENTS
}

/// Legacy events in blocks `(from_exclusive, to_inclusive]`.
pub fn legacy_events_between(
    from_exclusive: u64,
    to_inclusive: u64,
) -> impl Iterator<Item = (L1BlockId, (Timestamp, Vec<L1Event>))> {
    legacy_events()
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
    fn test_legacy_events_embed_ok() {
        let mut delegated = 0;
        let mut registered = 0;
        let mut undelegated = 0;
        let mut withdrawal_claimed = 0;
        let mut validator_exit = 0;

        for (_, events) in legacy_events().values() {
            for event in events {
                let L1Event::StakeTable(event) = event else {
                    panic!("unexpected reward event in legacy data: {event:?}");
                };
                match event.as_ref() {
                    StakeTableV3Events::Delegated(_) => delegated += 1,
                    StakeTableV3Events::ValidatorRegistered(_) => registered += 1,
                    StakeTableV3Events::Undelegated(_) => undelegated += 1,
                    StakeTableV3Events::WithdrawalClaimed(_) => withdrawal_claimed += 1,
                    StakeTableV3Events::ValidatorExit(_) => validator_exit += 1,
                    other => panic!(
                        "unexpected event type in legacy data: {}",
                        serde_json::to_string(other).unwrap_or_default()
                    ),
                }
            }
        }

        assert_eq!(delegated, 168);
        assert_eq!(registered, 140);
        // 50 undelegations total; 3 were later claimed, each replacing a legacy `Withdrawal` event
        // (which carries no validator) with a synthetic `WithdrawalClaimed` event. The other 47
        // undelegations remain pending in state after replay (see `test_legacy_replay_clean_state`).
        assert_eq!(undelegated, 50);
        assert_eq!(withdrawal_claimed, 3);
        assert_eq!(validator_exit, 1);
    }

    #[test_log::test]
    fn test_legacy_events_between_ok() {
        let (&first, _) = legacy_events()
            .iter()
            .next()
            .expect("legacy events must be non-empty");
        let (&last, _) = legacy_events()
            .iter()
            .next_back()
            .expect("legacy events must be non-empty");

        // Everything is included when the range covers the whole dataset.
        let all = legacy_events_between(first.number - 1, last.number).count();
        assert_eq!(all, legacy_events().len());

        // Nothing at or before `from_exclusive` is included.
        let excluding_first = legacy_events_between(first.number, last.number).count();
        assert_eq!(excluding_first, legacy_events().len() - 1);

        // Nothing past `to_inclusive` is included.
        let none = legacy_events_between(first.number - 1, first.number - 1).count();
        assert_eq!(none, 0);
    }

    #[test_log::test(tokio::test)]
    async fn test_legacy_replay_clean_state() {
        // The real init block, dropped from `legacy_events` (`fast_forward` starts scanning at
        // `from + 1`, so it must never be replayed).
        const GENESIS_BLOCK: u64 = 8_077_808;
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

        let blocks = legacy_events()
            .iter()
            .map(|(id, (timestamp, events))| BlockInput {
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
        .expect("replaying legacy events must not panic or error");

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
