//! SQL-based persistent storage.

use crate::{
    Result,
    input::l1::{L1Persistence, PersistentSnapshot},
    types::{
        common::{Address, L1BlockId, Timestamp},
        global::FullNodeSetDiff,
        wallet::WalletDiff,
    },
};
use std::path::Path;

#[derive(Debug)]
pub struct Persistence;

impl Persistence {
    /// Create a new SQLite database at the given file location.
    pub async fn new(_file: &Path) -> Result<Self> {
        todo!()
    }
}

impl L1Persistence for Persistence {
    async fn finalized_snapshots(&self) -> Result<PersistentSnapshot> {
        todo!()
    }

    async fn apply_events<'a>(
        &self,
        _block: L1BlockId,
        _timestamp: Timestamp,
        _node_set_diff: impl IntoIterator<Item = &'a FullNodeSetDiff> + Send,
        _wallets_diff: impl IntoIterator<Item = (Address, &'a WalletDiff)> + Send,
    ) -> Result<()> {
        todo!()
    }
}
