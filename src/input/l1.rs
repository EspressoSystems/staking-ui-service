//! Input data from L1.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use alloy::primitives::BlockHash;
use async_lock::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use futures::stream::{Stream, StreamExt};
use hotshot_contract_adapter::sol_types::{
    RewardClaim::RewardClaimEvents, StakeTableV2::StakeTableV2Events,
};
use tracing::instrument;

use crate::{
    error::{Error, Result, ensure},
    types::{
        common::{Address, L1BlockId, L1BlockInfo, Timestamp},
        global::{FullNodeSetDiff, FullNodeSetSnapshot, FullNodeSetUpdate},
        wallet::{WalletDiff, WalletSnapshot, WalletUpdate},
    },
};

mod rpc_stream;
pub use rpc_stream::RpcStream;

/// In-memory state populated by the L1 input source.
#[derive(Clone, Debug)]
pub struct State<S> {
    /// Information about each L1 block since the finalized one.
    ///
    /// This forms a chain of updates and snapshots dating back to the persisted/finalized snapshot.
    /// These blocks are kept in order, corresponding 1:1 with L1 blocks. As such, `blocks[i]`
    /// corresponds to the state after L1 block `finalized + i` (where the finalized block is
    /// always `blocks[0].block.number`).
    ///
    /// This list always contains at least one entry: `blocks[0]` is the finalized state. This is
    /// either loaded from persistent storage on startup, or initialized to a genesis state.
    blocks: Vec<BlockData>,

    /// Maps L1 block hash to block number for all blocks in `blocks`.
    blocks_by_hash: HashMap<BlockHash, u64>,

    /// Persistent storage handle.
    storage: S,
}

impl<S: L1Persistence> State<S> {
    /// Create a new L1 input data source.
    pub async fn new(storage: S) -> Result<Self> {
        let snapshot = storage.finalized_snapshots().await?;
        let finalized = BlockData {
            block: snapshot.block,
            timestamp: snapshot.timestamp,
            node_set: snapshot.node_set,
            wallets: snapshot.wallets,

            // The updates are the difference between this snapshot and the previous one; for the
            // initial snapshot, there is no such difference.
            node_set_update: None,
            wallets_update: None,
        };
        let blocks_by_hash = [(finalized.block.hash, finalized.block.number)]
            .into_iter()
            .collect();
        let blocks = vec![finalized];
        Ok(Self {
            blocks,
            blocks_by_hash,
            storage,
        })
    }

    /// Get the latest known L1 block.
    pub fn latest_l1_block(&self) -> L1BlockId {
        self.blocks[self.blocks.len() - 1].block
    }

    /// Get the hashes identifying a particular L1 block.
    pub fn l1_block(&self, number: u64) -> Result<L1BlockId> {
        Ok(self.block(number)?.block)
    }

    /// Get a snapshot of the full node set at the given L1 block.
    pub fn full_node_set(&self, hash: BlockHash) -> Result<FullNodeSetSnapshot> {
        let number = self.block_number(hash)?;
        Ok(self.block(number)?.node_set.clone())
    }

    /// Get the update applied to the full node set due to the given L1 block.
    pub fn full_node_set_update(&self, hash: BlockHash) -> Result<FullNodeSetUpdate> {
        let number = self.block_number(hash)?;
        let block = self.block(number)?;
        let diff = block.node_set_update.clone().ok_or_else(Error::gone)?;
        let update = FullNodeSetUpdate {
            l1_block: block.block_info(),
            diff,
        };
        Ok(update)
    }

    /// Get a snapshot of the requested wallet state at the given L1 block.
    pub fn wallet(&self, address: Address, hash: BlockHash) -> Result<WalletSnapshot> {
        let number = self.block_number(hash)?;
        let block = self.block(number)?;
        Ok(block
            .wallets
            .get(&address)
            .ok_or_else(|| Error::not_found().context(format!("unknown account {address}")))?
            .clone())
    }

    /// Get the update applied to the requested wallet due to the given L1 block.
    pub fn wallet_update(&self, address: Address, hash: BlockHash) -> Result<WalletUpdate> {
        let number = self.block_number(hash)?;
        let block = self.block(number)?;

        // Check that this account even exists.
        ensure!(
            block.wallets.contains_key(&address),
            Error::not_found().context(format!("unknown account {address}"))
        );

        // Get the update. If there was none for this block, generate a trivial one.
        let diff = block
            .wallets_update
            .as_ref()
            .ok_or_else(Error::gone)?
            .get(&address)
            .cloned()
            .unwrap_or_default();
        Ok(WalletUpdate {
            l1_block: block.block_info(),
            diff,
        })
    }

    /// Get the block with the requested block number.
    fn block(&self, number: u64) -> Result<&BlockData> {
        let finalized = self.blocks[0].block.number;
        ensure!(
            number >= finalized,
            Error::gone().context(format!(
                "requested L1 block {number}; earliest available is {finalized}"
            ))
        );

        let offset = (number - finalized) as usize;
        ensure!(
            offset < self.blocks.len(),
            Error::not_found().context(format!(
                "requested L1 block {number}; latest available is {}",
                finalized + (offset as u64)
            ))
        );

        Ok(&self.blocks[offset])
    }

    /// Get the block number with the requested hash.
    ///
    /// This function will succeed if and only if the resulting block number is in our in-memory
    /// state (i.e. if [`Self::block`] would also succeed).
    fn block_number(&self, hash: BlockHash) -> Result<u64> {
        self.blocks_by_hash
            .get(&hash)
            .copied()
            .ok_or_else(|| Error::not_found().context(format!("unknown block hash {hash}")))
    }

    /// Subscribe to a stream of L1 events and populate `state` accordingly.
    ///
    /// Unless there is some catastrophic error, this future will never resolve. It is best spawned
    /// as a background task.
    #[instrument(skip(state, stream))]
    pub async fn subscribe(state: Arc<RwLock<Self>>, mut stream: impl ResettableStream) {
        while let Some(block) = stream.next().await {
            // Retry on any errors until we have successfully handled this input.
            loop {
                let state = state.upgradable_read().await;
                if let Err(err) = Self::handle_block(state, &mut stream, &block).await {
                    tracing::error!(?block, "error processing block input: {err}");
                } else {
                    break;
                }
            }
        }
        tracing::error!("L1 block stream ended unexpectedly");
    }

    /// Handle a single block input from L1.
    ///
    /// Returns an error if some transient error occurred which prevented the block from being
    /// processed (e.g. error reaching database). If a reorg occurs, the `state` and `stream` are
    /// reset appropriately, but the result will be [`Ok`], indicating that the next (old) block
    /// should be consumed from the stream.
    #[instrument(skip(state, stream))]
    async fn handle_block(
        mut state: RwLockUpgradableReadGuard<'_, Self>,
        stream: &mut impl ResettableStream,
        block: &BlockInput,
    ) -> Result<()> {
        tracing::debug!("received L1 input");

        // Handle a new finalized block if necessary.
        if block.finalized.number > state.blocks[0].block.number {
            let mut write_state = RwLockUpgradableReadGuard::upgrade(state).await;

            // Make sure we have this new finalized block somewhere in our unfinalized state; if not
            // we cannot necessarily handle this event.
            if Some(&block.finalized.number)
                != write_state.blocks_by_hash.get(&block.finalized.hash)
            {
                // This can only fail if there has been a reorg.
                return write_state.reorg(stream).await;
            }
            write_state.finalize(block.finalized.number).await?;

            // Drop the write lock while we proceed to process the new block like normal and compute
            // the new state snapshots, as this might be slow.
            state = RwLockWriteGuard::downgrade_to_upgradable(write_state);
        }

        // Check that this block extends the last block; if not there's been a reorg.
        let last = &state.blocks[state.blocks.len() - 1];
        if block.block.number != last.block.number + 1 || block.block.parent != last.block.hash {
            let mut state = RwLockUpgradableReadGuard::upgrade(state).await;
            return state.reorg(stream).await;
        }

        // Convert the input event into new updates to our state.
        let new_block = last.next(block);

        // Update state. As soon as we take this write lock, we are updating the state object in
        // place. We must not fail after this point, or we may drop the lock while the state is in a
        // partially modified state. All the validation performed up to this point should be
        // sufficient to ensure that we will not fail after this.
        let mut state = RwLockUpgradableReadGuard::upgrade(state).await;
        state.blocks.push(new_block);
        state
            .blocks_by_hash
            .insert(block.block.hash, block.block.number);

        Ok(())
    }

    async fn reorg(&mut self, _stream: &mut impl ResettableStream) -> Result<()> {
        tracing::warn!("reorg detected");
        todo!()
    }

    async fn finalize(&mut self, finalized: u64) -> Result<()> {
        tracing::info!(?finalized, "new finalized block");

        // Collect events up to the new finalized block, to write to persistent storage.
        let node_set_diff = self
            .blocks
            .iter()
            // Skip the first block, which is already finalized.
            .skip(1)
            // Stop at the new finalized block.
            .take_while(|block| block.block.number <= finalized)
            .flat_map(|block| block.node_set_update.as_ref().unwrap().iter());
        let wallets_diff = self
            .blocks
            .iter()
            // Skip the first block, which is already finalized.
            .skip(1)
            // Stop at the new finalized block.
            .take_while(|block| block.block.number <= finalized)
            .flat_map(|block| {
                block
                    .wallets_update
                    .as_ref()
                    .unwrap()
                    .iter()
                    .flat_map(|(addr, diff)| diff.iter().map(|diff| (*addr, diff)))
            });

        let finalized_info = self.block(finalized)?;
        self.storage
            .apply_events(
                finalized_info.block,
                finalized_info.timestamp,
                node_set_diff,
                wallets_diff,
            )
            .await?;
        self.garbage_collect(finalized);
        Ok(())
    }

    /// Garbage collect in-memory blocks.
    ///
    /// Brings thee new `finalized` block to the front of the `blocks` list, deleting all blocks
    /// before it.
    fn garbage_collect(&mut self, finalized: u64) {
        // Bring the new finalized block to the front of the list, shifting all now-old blocks to
        // the end.
        let offset = (finalized - self.blocks[0].block.number) as usize;
        self.blocks.rotate_left(offset);
        // Remove the old blocks, and also their entries in `blocks_by_hash`.
        for _ in 0..offset {
            let block = self.blocks.pop().unwrap();
            self.blocks_by_hash.remove(&block.block.hash);
        }
    }
}

/// Snapshots and updates for each L1 block.
#[derive(Clone, Debug)]
struct BlockData {
    /// The L1 block.
    block: L1BlockId,

    /// The L1 block timestamp.
    timestamp: Timestamp,

    /// The full node set as of this L1 block.
    node_set: FullNodeSetSnapshot,

    /// The change to the full node set between the previous L1 block and this one.
    ///
    /// This may be [`None`] when the previous L1 block has been garbage collected.
    node_set_update: Option<Vec<FullNodeSetDiff>>,

    /// The state of each wallet as of this L1 block.
    wallets: WalletsSnapshot,

    /// The changes to each wallet between the previous L1 block and this one.
    ///
    /// Unlike wallet snapshots, we don't benefit from using an immutable map here, as updates are
    /// always unique to a block, not partially shared with previous blocks. However, the good news
    /// is that only a few if any wallets will have non-trivial updates in any given block, so the
    /// size of this set should be small in practice. We use a [`BTreeMap`] to further compress the
    /// size of this set, as B-Trees should generally have a better memory footprint than hash maps.
    ///
    /// If a wallet was not updated in a given block, it will not have an entry here, but a trivial
    /// [`WalletUpdate`] can always be generated for any account by combining the [`L1BlockInfo`]
    /// with an empty [`WalletDiff`] list.
    ///
    /// This may be [`None`] when the previous L1 block has been garbage collected.
    wallets_update: Option<BTreeMap<Address, Vec<WalletDiff>>>,
}

/// A snapshot of the latest state of every wallet.
///
/// Note that while this set can, in concept, become very large, the use of immutable data
/// structures means that the memory for all entries is shared with the previous block's
/// snapshot, excepting only those wallets whose state has actually changed in this L1 block.
/// This will be a small number of wallets (often 0!) in practice.
pub type WalletsSnapshot = im::HashMap<Address, WalletSnapshot>;

impl BlockData {
    fn block_info(&self) -> L1BlockInfo {
        L1BlockInfo {
            number: self.block.number,
            hash: self.block.hash,
            timestamp: self.timestamp,
        }
    }

    fn next(&self, input: &BlockInput) -> Self {
        #![allow(unused_mut)]

        // Cloning entire state in order to apply updates and get the new state. This is cheap
        // thanks to the magic of immutable data structures.
        let mut node_set = self.node_set.clone();
        let mut wallets = self.wallets.clone();

        // Keep track of changes we apply.
        let mut node_set_update = vec![];
        let mut wallets_update = BTreeMap::default();

        for _event in &input.events {
            // TODO convert contract events into either node set or wallet updates, and apply
            // updates to state snapshots.
        }

        Self {
            block: input.block,
            timestamp: input.timestamp,
            node_set,
            node_set_update: Some(node_set_update),
            wallets,
            wallets_update: Some(wallets_update),
        }
    }
}

/// The minimal data we need in order to ingest a new L1 block.
#[derive(Debug)]
pub struct BlockInput {
    /// The L1 block.
    pub block: L1BlockId,

    /// The latest finalized block, as of the time of this block being produced as the head.
    pub finalized: L1BlockId,

    /// The timestamp of this L1 block.
    pub timestamp: Timestamp,

    /// Relevant events emitted by this block.
    pub events: Vec<L1Event>,
}

/// The set of L1 events that we care about.
#[derive(Clone, derive_more::Debug)]
pub enum L1Event {
    /// An event emitted by the reward claim contract.
    Reward(Arc<RewardClaimEvents>),

    /// An event emitted by the stake table contract.
    #[debug("StakeTableV2Events")]
    StakeTable(Arc<StakeTableV2Events>),
}

/// A stream of information incoming from the L1.
///
/// This stream is special in that it has an interface for resetting back to the finalized block.
/// This is used to handle reorgs.
///
/// This interface is also unique in that it outputs just the information that the L1 input source
/// needs, namely minimal information about the L1 block itself, plus the parsed event logs that we
/// care about.
///
/// Note that both the stream interface and the [`reset`](Self::reset) function are infallible. It
/// is expected that the underlying implementation has its own way of retrying and/or reconnecting
/// when errors occur, so that, in theory, the interface we consume is that of a never-ending stream
/// of L1 blocks.
pub trait ResettableStream: Unpin + Stream<Item = BlockInput> {
    fn reset(&mut self, number: u64) -> impl Send + Future<Output = ()>;
}

/// The information which must be stored in persistent storage.
#[derive(Clone, Debug)]
pub struct PersistentSnapshot {
    pub block: L1BlockId,
    pub timestamp: Timestamp,
    pub node_set: FullNodeSetSnapshot,
    pub wallets: WalletsSnapshot,
}

/// Persistent storage for the L1 data.
pub trait L1Persistence {
    /// Fetch the latest persisted snapshots.
    fn finalized_snapshots(&self) -> impl Send + Future<Output = Result<PersistentSnapshot>>;

    /// Apply changes to persistent storage up to the specified L1 block.
    fn apply_events<'a>(
        &self,
        block: L1BlockId,
        timestamp: Timestamp,
        node_set_diff: impl IntoIterator<Item = &'a FullNodeSetDiff> + Send,
        wallets_diff: impl IntoIterator<Item = (Address, &'a WalletDiff)> + Send,
    ) -> impl Send + Future<Output = Result<()>>;
}
