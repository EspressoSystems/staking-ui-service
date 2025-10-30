#![cfg(test)]

use std::{
    collections::HashSet,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use alloy::{
    primitives::{FixedBytes, keccak256},
    sol_types::SolValue,
};
use async_lock::RwLockReadGuard;
use espresso_types::v0::v0_3::StakeTableEvent;
use hotshot_contract_adapter::{
    sol_types::{
        G1PointSol,
        StakeTableV2::{ValidatorExit, ValidatorRegistered, ValidatorRegisteredV2},
    },
    stake_table::StateSignatureSol,
};
use hotshot_types::{
    light_client::{StateVerKey, hash_bytes_to_field},
    traits::signature_key::{SignatureKey, StateSignatureKey},
};
use jf_signature::{SignatureScheme, schnorr::SchnorrSignatureScheme};
use rand::{Rng, RngCore, SeedableRng, rngs::StdRng, seq::IteratorRandom};
use tagged_base64::TaggedBase64;
use tide_disco::{Error as _, StatusCode};
use tokio::{task::spawn, time::sleep};

use crate::types::common::{NodeSetEntry, Ratio};

use super::*;

/// Easy-setup storage that just uses memory.
#[derive(Clone, Debug, Default)]
pub(crate) struct MemoryStorage {
    snapshot: Arc<RwLock<Option<Snapshot>>>,
}

impl L1Persistence for MemoryStorage {
    async fn finalized_snapshot(&self) -> Result<Option<Snapshot>> {
        Ok(self.snapshot.read().await.clone())
    }

    async fn save_genesis(&self, snapshot: Snapshot) -> Result<()> {
        *self.snapshot.write().await = Some(snapshot);
        Ok(())
    }

    async fn apply_events(
        &self,
        block: L1BlockSnapshot,
        node_set_diff: impl IntoIterator<Item = FullNodeSetDiff> + Send,
        wallets_diff: impl IntoIterator<Item = (Address, WalletDiff)> + Send,
    ) -> Result<()> {
        let mut lock = self.snapshot.write().await;
        let snapshot = lock.get_or_insert(Snapshot::empty(block_snapshot(0)));

        for diff in node_set_diff {
            snapshot.node_set.apply(&diff);
        }
        for (address, diff) in wallets_diff {
            snapshot.wallets.apply(address, &diff);
        }
        snapshot.block = block;
        Ok(())
    }
}

/// Storage that always fails.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct FailStorage;

impl L1Persistence for FailStorage {
    async fn finalized_snapshot(&self) -> Result<Option<Snapshot>> {
        Err(Error::catch_all(
            StatusCode::INTERNAL_SERVER_ERROR,
            "FailStorage".into(),
        ))
    }

    async fn save_genesis(&self, _snapshot: Snapshot) -> Result<()> {
        Err(Error::catch_all(
            StatusCode::INTERNAL_SERVER_ERROR,
            "FailStorage".into(),
        ))
    }

    async fn apply_events(
        &self,
        _block: L1BlockSnapshot,
        _node_set_diff: impl IntoIterator<Item = FullNodeSetDiff> + Send,
        _wallets_diff: impl IntoIterator<Item = (Address, WalletDiff)> + Send,
    ) -> Result<()> {
        Err(Error::catch_all(
            StatusCode::INTERNAL_SERVER_ERROR,
            "FailStorage".into(),
        ))
    }
}

/// Resettable stream that yields a predefined list of inputs.
#[derive(Clone, Debug)]
pub(crate) struct VecStream {
    inputs: Vec<BlockInput>,
    reorg: Option<Vec<BlockInput>>,
    pos: usize,
    panic_at_end: bool,
}

impl Default for VecStream {
    fn default() -> Self {
        Self {
            inputs: vec![],
            reorg: None,
            pos: 0,
            panic_at_end: true,
        }
    }
}

impl VecStream {
    /// Emulate an infinite stream.
    ///
    /// The resulting stream will block indefinitely when it reaches the end of its predefined
    /// input sequence.
    pub(crate) fn infinite() -> Self {
        Self {
            panic_at_end: false,
            ..Default::default()
        }
    }

    /// Append a new L1 block input to be yielded by the stream.
    pub(crate) fn push(&mut self, input: BlockInput) {
        self.inputs.push(input);
    }

    /// Provide an alternative sequence of inputs to yield after the stream is reset.
    pub(crate) fn with_reorg(mut self, inputs: Vec<BlockInput>) -> Self {
        self.reorg = Some(inputs);
        self
    }
}

impl Stream for VecStream {
    type Item = BlockInput;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.pos >= self.inputs.len() {
            if self.panic_at_end {
                // Most tests expect to never hit the end of the predefined input sequence. If
                // we do, panic and fail the test.
                panic!("reached end of predefined input stream");
            } else {
                // In some cases, we want to emulate the realistic behavior of an L1 stream,
                // which is blocking when no more blocks are readily available.
                tracing::warn!("reached end of predefined input stream, blocking indefinitely");
                return Poll::Pending;
            }
        }

        let poll = Poll::Ready(Some(self.inputs[self.pos].clone()));
        self.pos += 1;
        poll
    }
}

impl ResettableStream for VecStream {
    async fn reset(&mut self, number: u64) {
        tracing::info!(number, "reset");
        self.pos = self
            .inputs
            .iter()
            .position(|input| input.block.number == number + 1)
            .unwrap_or_else(|| panic!("cannot reset to unknown block height {number}"));
        if let Some(reorg) = self.reorg.take() {
            self.inputs = reorg;
        }
    }
}

/// Generate a block ID for testing.
pub(crate) fn block_id(number: u64) -> L1BlockId {
    let parent = keccak256(number.saturating_sub(1).to_le_bytes());
    let hash = keccak256(number.to_le_bytes());
    L1BlockId {
        number,
        hash,
        parent,
    }
}

/// Generate a block snapshot for testing.
pub(crate) fn block_snapshot(number: u64) -> L1BlockSnapshot {
    L1BlockSnapshot {
        id: block_id(number),
        timestamp: 12 * number,
        exit_escrow_period: 3600,
    }
}

/// Generate an arbitrary node for testing.
pub(crate) fn make_node(i: usize) -> NodeSetEntry {
    let address = Address::random();
    let staking_key = TaggedBase64::new("KEY", &i.to_le_bytes()).unwrap();
    NodeSetEntry {
        address,
        staking_key,
        stake: i.try_into().unwrap(),
        commission: Ratio::new(5, 100),
    }
}

/// Generate random L1 events for testing.
///
/// The generation process is stateful, which makes it possible to generate random events such that
/// every generated event is "valid" given the events that came before it (e.g. no duplicate
/// registrations, no delegations to an unregistered validator, etc.).
#[derive(Debug)]
pub(crate) struct EventGenerator {
    nodes: HashSet<Address>,
    rng: StdRng,
}

impl Default for EventGenerator {
    fn default() -> Self {
        Self {
            nodes: Default::default(),
            rng: StdRng::from_seed(Default::default()),
        }
    }
}

impl Iterator for EventGenerator {
    type Item = L1Event;

    fn next(&mut self) -> Option<Self::Item> {
        // Generate a random event until we get one that is possible given the current state.
        loop {
            // Assign each type of event a numeric code.
            const REGISTER: usize = 0;
            const REGISTER_V2: usize = 1;
            const DEREGISTER: usize = 2;
            const MAX_EVENT_TYPE: usize = 3;

            // Generate the code for a random event type.
            let event_type = self.rng.gen_range(0..MAX_EVENT_TYPE);

            let event = match event_type {
                // Register, RegisterV2
                t @ (REGISTER | REGISTER_V2) => {
                    let mut address_bytes = FixedBytes::<32>::default();
                    self.rng.fill_bytes(address_bytes.as_mut_slice());
                    let account = Address::from_word(address_bytes);

                    // Insert the node so we can reference it in later events (like delegations).
                    // This insert should always return `true` because with a random address, it is
                    // vanishingly unlikely we have generated this same address before.
                    assert!(self.nodes.insert(account));

                    let index = self.rng.next_u64();
                    let (bls_vk, bls_sk) =
                        PubKey::generated_from_seed_indexed(Default::default(), index);
                    let (schnorr_vk, schnorr_sk) =
                        StateVerKey::generated_from_seed_indexed(Default::default(), index);

                    let commission = self.rng.gen_range(0..COMMISSION_BASIS_POINTS);

                    if t == REGISTER {
                        StakeTableEvent::Register(ValidatorRegistered {
                            account,
                            blsVk: bls_vk.into(),
                            schnorrVk: schnorr_vk.into(),
                            commission,
                        })
                        .into()
                    } else {
                        let auth_msg = account.abi_encode();
                        let bls_sig = PubKey::sign(&bls_sk, &auth_msg).unwrap();
                        let schnorr_sig = SchnorrSignatureScheme::sign(
                            &(),
                            &schnorr_sk,
                            [hash_bytes_to_field(&auth_msg).unwrap()],
                            &mut self.rng,
                        )
                        .unwrap();

                        StakeTableEvent::RegisterV2(ValidatorRegisteredV2 {
                            account,
                            blsVK: bls_vk.into(),
                            schnorrVK: schnorr_vk.into(),
                            commission,
                            blsSig: G1PointSol::from(bls_sig).into(),
                            schnorrSig: StateSignatureSol::from(schnorr_sig).into(),
                        })
                        .into()
                    }
                }

                // Deregister
                DEREGISTER => {
                    // Choose a random node to deregister.
                    let Some(&node) = self.nodes.iter().choose(&mut self.rng) else {
                        // If there are none try again.
                        continue;
                    };
                    self.nodes.remove(&node);
                    StakeTableEvent::Deregister(ValidatorExit { validator: node }).into()
                }

                _ => unreachable!(),
            };
            return Some(event);
        }
    }
}

/// Generate random L1 inputs for testing.
///
/// The generation process is stateful, which makes it possible to generate random events in a
/// sequence of L1 blocks such that every generated event is "valid" given the events that came
/// before it (e.g. no duplicate registrations, no delegations to an unregistered validator, etc.).
#[derive(Debug)]
pub(crate) struct InputGenerator {
    events: EventGenerator,
    next_block: u64,
    rng: StdRng,
}

impl Default for InputGenerator {
    fn default() -> Self {
        Self {
            events: Default::default(),
            next_block: 0,
            rng: StdRng::from_seed(Default::default()),
        }
    }
}

impl Iterator for InputGenerator {
    type Item = BlockInput;

    fn next(&mut self) -> Option<Self::Item> {
        // Select a realistic random number of events to include in this block.
        let n_events = self.rng.gen_range(0..5);

        // Generate block.
        let mut input = BlockInput::empty(self.next_block);
        for _ in 0..n_events {
            input.events.push(self.events.next()?);
        }
        self.next_block += 1;
        Some(input)
    }
}

impl BlockInput {
    /// Create a [`BlockInput`] with no events, just L1 block information.
    pub(crate) fn empty(number: u64) -> BlockInput {
        let block = block_snapshot(number);
        Self {
            block: block.id(),
            timestamp: block.timestamp(),
            finalized: block_id(0),
            events: vec![],
        }
    }
}

impl<S: Default> super::State<S> {
    pub(crate) fn with_l1_block_range(start: u64, end: u64) -> Self {
        let blocks = (start..end).map(BlockData::empty).collect::<Vec<_>>();
        let blocks_by_hash = blocks
            .iter()
            .map(|block| (block.block().hash(), block.block().number()))
            .collect();
        Self {
            blocks,
            blocks_by_hash,
            storage: Default::default(),
        }
    }
}

impl BlockData {
    /// Generate a test L1 block with no staking-related data.
    pub(super) fn empty(number: u64) -> Self {
        Self {
            state: Snapshot::empty(block_snapshot(number)),
            node_set_update: Some(Default::default()),
            wallets_update: Some(Default::default()),
        }
    }
}

/// Process events from the given stream until the predicate is satisfied.
///
/// Returns a lock on the state, frozen after the first observation where the predicate was
/// satisfied.
pub(crate) async fn subscribe_until<S>(
    state: &'_ Arc<RwLock<State<S>>>,
    stream: impl ResettableStream + Send + 'static,
    p: impl Fn(&State<S>) -> bool,
) -> RwLockReadGuard<'_, State<S>>
where
    S: L1Persistence + Sync + 'static,
{
    let task = spawn(State::subscribe(state.clone(), stream));

    // Wait for the predicate.
    loop {
        sleep(Duration::from_millis(100)).await;
        let state = state.read().await;
        if p(&state) {
            task.abort();
            let _ = task.await;
            break state;
        }
    }
}
