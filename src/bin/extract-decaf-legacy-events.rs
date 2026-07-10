//! Extract pre-upgrade StakeTable V1 events from the Decaf Sepolia deployment.
//!
//! Decaf's stake table was upgraded from a V1 to a V3 deployment at block
//! [`LEGACY_CUTOFF_BLOCK`]. This one-off script fetches the V1-era logs, decodes the ones this
//! service cares about, and writes them as an ordered JSON array shared with
//! `src/input/l1/decaf.rs`, which embeds the output at `src/input/l1/decaf_legacy_events.json` and
//! replays it during L1 catchup instead of fetching it live (the V1 encoding is not decodable by
//! `StakeTableV3Events::decode_raw_log`).
//!
//! Run with `cargo run --bin extract-decaf-legacy-events -- --output
//! src/input/l1/decaf_legacy_events.json`.

use std::{collections::BTreeMap, path::PathBuf};

use alloy::{
    consensus::Transaction,
    eips::BlockId,
    providers::{Provider, ProviderBuilder},
    rpc::types::{Filter, Log},
    sol_types::{SolCall, SolEventInterface},
};
use anyhow::{Context, ensure};
use clap::Parser;
use hotshot_contract_adapter::sol_types::StakeTableV3::{
    self, StakeTableV3Events, Withdrawal, WithdrawalClaimed,
};
use staking_ui_service::input::l1::decaf::{DECAF_STAKE_TABLE, LEGACY_CUTOFF_BLOCK, LegacyBlock};
use tide_disco::Url;
use tracing_subscriber::EnvFilter;

/// The block the Decaf StakeTable contract was initialized at. Dropped from the output entirely,
/// since `fast_forward` starts scanning at `from + 1` and must never replay it.
const GENESIS_BLOCK: u64 = 8_077_808;

#[derive(Parser)]
struct Options {
    /// L1 RPC URL to query for legacy Decaf StakeTable events.
    #[clap(
        long,
        env = "EXTRACT_DECAF_LEGACY_EVENTS_RPC_URL",
        default_value = "https://sepolia.gateway.tenderly.co"
    )]
    rpc_url: Url,

    /// Output path for the JSON array of legacy blocks. Defaults to stdout.
    #[clap(long)]
    output: Option<PathBuf>,

    /// Maximum number of blocks scanned per `eth_getLogs` query.
    #[clap(long, default_value = "50000")]
    chunk_size: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let opt = Options::parse();
    let provider = ProviderBuilder::new().connect_http(opt.rpc_url.clone());

    let mut events_by_block: BTreeMap<u64, Vec<StakeTableV3Events>> = BTreeMap::new();
    for (from, to) in block_chunks(GENESIS_BLOCK, LEGACY_CUTOFF_BLOCK, opt.chunk_size) {
        tracing::info!(from, to, "fetching stake table logs");
        let filter = Filter::new()
            .address(DECAF_STAKE_TABLE)
            .from_block(from)
            .to_block(to);
        let logs = provider
            .get_logs(&filter)
            .await
            .context("fetching stake table logs")?;

        for log in logs {
            let Ok(event) = StakeTableV3Events::decode_raw_log(log.topics(), &log.data().data)
            else {
                // Includes the legacy `Upgrade(address)` event, which the V3 bindings cannot
                // decode; there is no other kind of event in this range that we can't decode.
                tracing::debug!(tx = ?log.transaction_hash, "skipping undecodable legacy log");
                continue;
            };
            if !is_state_relevant(&event) {
                continue;
            }

            let number = log
                .block_number
                .context("event log is missing a block number")?;
            if number == GENESIS_BLOCK {
                continue;
            }

            let event = match event {
                StakeTableV3Events::Withdrawal(withdrawal) => {
                    withdrawal_claimed(&provider, &log, withdrawal).await?
                }
                other => other,
            };
            events_by_block.entry(number).or_default().push(event);
        }
    }

    let mut blocks = Vec::with_capacity(events_by_block.len());
    for (number, events) in events_by_block {
        let header = provider
            .get_block(BlockId::number(number))
            .await
            .with_context(|| format!("fetching header for block {number}"))?
            .with_context(|| format!("block {number} not found"))?;
        blocks.push(LegacyBlock {
            number,
            hash: header.header.hash,
            parent: header.header.parent_hash,
            timestamp: header.header.timestamp,
            events,
        });
    }

    let event_count: usize = blocks.iter().map(|block| block.events.len()).sum();
    tracing::info!(
        blocks = blocks.len(),
        events = event_count,
        "extraction complete"
    );

    let json = serde_json::to_string_pretty(&blocks).context("serializing legacy blocks")?;
    match opt.output {
        Some(path) => std::fs::write(&path, json)
            .with_context(|| format!("writing output to {}", path.display()))?,
        None => println!("{json}"),
    }

    Ok(())
}

/// The subset of stake table events this service tracks, and which are kept in the legacy JSON
/// (`Withdrawal` is transformed into a synthetic `WithdrawalClaimed`, see [`withdrawal_claimed`]).
fn is_state_relevant(event: &StakeTableV3Events) -> bool {
    matches!(
        event,
        StakeTableV3Events::Delegated(_)
            | StakeTableV3Events::ValidatorRegistered(_)
            | StakeTableV3Events::Undelegated(_)
            | StakeTableV3Events::ValidatorExit(_)
            | StakeTableV3Events::Withdrawal(_)
    )
}

/// Replace a legacy `Withdrawal(account, amount)` event, which main's `handle_event` cannot
/// process (it has no validator), with a synthetic `WithdrawalClaimed` carrying the validator
/// address recovered from the claiming transaction's calldata.
async fn withdrawal_claimed(
    provider: &impl Provider,
    log: &Log,
    withdrawal: Withdrawal,
) -> anyhow::Result<StakeTableV3Events> {
    let tx_hash = log
        .transaction_hash
        .context("Withdrawal log is missing a transaction hash")?;
    let tx = provider
        .get_transaction_by_hash(tx_hash)
        .await
        .with_context(|| format!("fetching withdrawal claim transaction {tx_hash}"))?
        .with_context(|| format!("withdrawal claim transaction {tx_hash} not found"))?;

    let input = tx.input();
    ensure!(
        input.get(..4) == Some(StakeTableV3::claimWithdrawalCall::SELECTOR.as_slice()),
        "withdrawal claim transaction {tx_hash} does not call claimWithdrawal(address), \
         got selector {:?}",
        input.get(..4),
    );
    let call = StakeTableV3::claimWithdrawalCall::abi_decode(input)
        .with_context(|| format!("decoding claimWithdrawal calldata for tx {tx_hash}"))?;

    Ok(StakeTableV3Events::WithdrawalClaimed(WithdrawalClaimed {
        delegator: withdrawal.account,
        validator: call.validator,
        undelegationId: 0,
        amount: withdrawal.amount,
    }))
}

/// Split `[from, to]` into chunks of at most `chunk_size` blocks each.
fn block_chunks(from: u64, to: u64, chunk_size: u64) -> impl Iterator<Item = (u64, u64)> {
    let mut start = from;
    std::iter::from_fn(move || {
        if start > to {
            return None;
        }
        let end = (start + chunk_size - 1).min(to);
        let chunk = (start, end);
        start = end + 1;
        Some(chunk)
    })
}
