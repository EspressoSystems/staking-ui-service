//! Ad hoc L1 client functions.

use super::L1BlockSnapshot;
use crate::{
    Error, Result,
    error::ResultExt,
    types::common::{Address, L1BlockId},
};
use alloy::providers::Provider;
use hotshot_contract_adapter::sol_types::StakeTableV2;

/// Get the Espresso stake table genesis block.
pub async fn load_genesis(
    provider: &impl Provider,
    stake_table_address: Address,
) -> Result<L1BlockSnapshot> {
    let stake_table = StakeTableV2::new(stake_table_address, provider);

    // Get the block number where the stake table contract was initialized.
    let number: u64 = stake_table
        .initializedAtBlock()
        .call()
        .await
        .context(|| Error::internal().context("error getting stake table initialization block"))?
        .try_into()
        .context(|| Error::internal().context("genesis block number does not fit in a u64"))?;

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

    // Get the exit escrow period from the contract at the genesis block.
    let exit_escrow_period = stake_table
        .exitEscrowPeriod()
        .block(number.into())
        .call()
        .await
        .context(|| Error::internal().context("error getting genesis exit escrow period"))?
        .try_into()
        .context(|| {
            Error::internal().context("genesis exit escrow period does not fit in a u64")
        })?;

    Ok(L1BlockSnapshot {
        id,
        timestamp: genesis_block.header.timestamp,
        exit_escrow_period,
    })
}

#[cfg(test)]
mod test {
    use alloy::{node_bindings::Anvil, providers::ProviderBuilder};
    use espresso_contract_deployer::{Contract, Contracts};

    use crate::input::l1::testing::{deploy_contracts, funded_wallet};

    use super::*;

    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn test_genesis_exit_escrow_period() {
        let anvil = Anvil::new().block_time(1).spawn();
        let provider = ProviderBuilder::new()
            .wallet(funded_wallet())
            .connect_http(anvil.endpoint_url());

        let mut contracts = Contracts::new();
        deploy_contracts(&provider, &anvil, &mut contracts).await;

        let stake_table_address = contracts[&Contract::StakeTableProxy];
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
}
