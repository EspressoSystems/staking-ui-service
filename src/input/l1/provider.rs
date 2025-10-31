//! Ad hoc L1 client functions.

use super::L1BlockSnapshot;
use crate::{
    Error, Result,
    error::ResultExt,
    types::common::{Address, L1BlockId},
};
use alloy::providers::Provider;

/// Get the Espresso stake table genesis block.
pub async fn load_genesis(
    provider: &impl Provider,
    _stake_table: Address,
) -> Result<L1BlockSnapshot> {
    // TODO get the block number where the stake table contract was initialized.
    let number = 0;

    let genesis_block = provider
        .get_block(number.into())
        .await
        .context(Error::internal)?
        .ok_or_else(|| {
            Error::not_found().context(format!("unable to fetch genesis block {number}"))
        })?;
    let id = L1BlockId {
        number,
        hash: genesis_block.hash(),
        parent: genesis_block.header.parent_hash,
    };

    // TODO get the exit escrow period from the contract at the genesis block.
    let exit_escrow_period = 0;

    Ok(L1BlockSnapshot {
        id,
        timestamp: genesis_block.header.timestamp,
        exit_escrow_period,
    })
}
