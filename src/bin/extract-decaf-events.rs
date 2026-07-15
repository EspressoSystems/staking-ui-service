//! Extract pre-upgrade StakeTable V1 events from the Decaf Sepolia deployment.
//!
//! Decaf's stake table was upgraded from a V1 to a V3 deployment at block
//! [`decaf::CUTOFF_BLOCK`]. This one-off script fetches the V1-era logs, decodes the ones this
//! service cares about, and writes them as an ordered JSON array shared with
//! `src/input/l1/decaf.rs`, which embeds the output at `src/input/l1/decaf_v1_events.json` and
//! replays it during L1 catchup instead of fetching it live (the V1 encoding is not decodable by
//! `StakeTableV3Events::decode_raw_log`).
//!
//! Run with `cargo run --bin extract-decaf-events -- --output
//! src/input/l1/decaf_v1_events.json`.

use std::{collections::BTreeMap, path::PathBuf};

use alloy::{
    consensus::Transaction,
    eips::BlockId,
    primitives::keccak256,
    providers::{Provider, ProviderBuilder},
    rpc::types::{Filter, Log},
    sol_types::{SolCall, SolEventInterface},
};
use anyhow::{Context, bail, ensure};
use clap::Parser;
use hotshot_contract_adapter::sol_types::StakeTableV3::{
    self, StakeTableV3Events, Withdrawal, WithdrawalClaimed,
};
use staking_ui_service::input::l1::decaf::{self, Block};
use tide_disco::Url;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
struct Options {
    /// L1 RPC URL to query for V1 Decaf StakeTable events.
    #[clap(
        long,
        env = "EXTRACT_DECAF_EVENTS_RPC_URL",
        default_value = "https://sepolia.gateway.tenderly.co"
    )]
    rpc_url: Url,

    /// Output path for the JSON array of V1 blocks. Defaults to stdout.
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

    // The V1 `Upgrade(address)` event has no V3 equivalent, so it is the only log in the V1
    // range that `decode_raw_log` is allowed to fail on.
    let upgrade_topic0 = keccak256(b"Upgrade(address)");

    let mut events_by_block: BTreeMap<u64, Vec<StakeTableV3Events>> = BTreeMap::new();
    for (from, to) in block_chunks(decaf::GENESIS_BLOCK, decaf::CUTOFF_BLOCK, opt.chunk_size) {
        tracing::info!(from, to, "fetching stake table logs");
        let filter = Filter::new()
            .address(decaf::STAKE_TABLE)
            .from_block(from)
            .to_block(to);
        let logs = provider
            .get_logs(&filter)
            .await
            .context("fetching stake table logs")?;

        for log in logs {
            let Ok(event) = StakeTableV3Events::decode_raw_log(log.topics(), &log.data().data)
            else {
                ensure!(
                    log.topic0() == Some(&upgrade_topic0),
                    "undecodable log with unexpected topic0 {:?} in tx {:?}",
                    log.topic0(),
                    log.transaction_hash,
                );
                tracing::debug!(tx = ?log.transaction_hash, "skipping V1 Upgrade log");
                continue;
            };
            if !keep(&event)? {
                continue;
            }

            let number = log
                .block_number
                .context("event log is missing a block number")?;
            if number == decaf::GENESIS_BLOCK {
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
        blocks.push(Block {
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

    let json = serde_json::to_string_pretty(&blocks).context("serializing V1 blocks")?;
    match opt.output {
        Some(path) => std::fs::write(&path, json)
            .with_context(|| format!("writing output to {}", path.display()))?,
        None => println!("{json}"),
    }

    Ok(())
}

/// Triage a decoded event: keep it, drop it, or fail the extraction.
///
/// `Ok(true)`: events this service tracks, kept in the V1 JSON (`Withdrawal` is transformed into
/// a synthetic `WithdrawalClaimed`, see [`withdrawal_claimed`]).
///
/// `Ok(false)`: the events `handle_event` in `src/input/l1.rs` explicitly ignores as not relevant
/// to this service.
///
/// Anything else mutates service state (`handle_event` applies it to the node set or wallets), so
/// silently dropping it would diverge the replayed state: fail instead.
fn keep(event: &StakeTableV3Events) -> anyhow::Result<bool> {
    match event {
        StakeTableV3Events::Delegated(_)
        | StakeTableV3Events::ValidatorRegistered(_)
        | StakeTableV3Events::Undelegated(_)
        | StakeTableV3Events::ValidatorExit(_)
        | StakeTableV3Events::Withdrawal(_) => Ok(true),
        StakeTableV3Events::X25519KeyUpdated(_)
        | StakeTableV3Events::P2pAddrUpdated(_)
        | StakeTableV3Events::MaxCommissionIncreaseUpdated(_)
        | StakeTableV3Events::MinDelegateAmountUpdated(_)
        | StakeTableV3Events::MinCommissionUpdateIntervalUpdated(_)
        | StakeTableV3Events::OwnershipTransferred(_)
        | StakeTableV3Events::Paused(_)
        | StakeTableV3Events::Unpaused(_)
        | StakeTableV3Events::Initialized(_)
        | StakeTableV3Events::RoleAdminChanged(_)
        | StakeTableV3Events::RoleGranted(_)
        | StakeTableV3Events::RoleRevoked(_)
        | StakeTableV3Events::Upgraded(_) => Ok(false),
        // `StakeTableV3Events` implements `Serialize` but not `Debug`.
        other => bail!(
            "unsupported state-mutating event in V1 range: {}",
            serde_json::to_string(other).context("serializing unsupported event")?
        ),
    }
}

/// Replace a V1 `Withdrawal(account, amount)` event, which main's `handle_event` cannot
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
