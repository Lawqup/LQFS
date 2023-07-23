use std::{borrow::Cow, fs, path::Path};

use crate::prelude::*;

use persy::{ByteVec, IndexType, Persy, Transaction, ValueMode};
use raft::{prelude::*, Storage};

use crate::prelude::Result;

pub struct NodeStore {
    persy: Persy,
}

const ENTRIES_KEY: &str = "snapshot";
const SNAPSHOT_KEY: &str = "snapshot";
const LAST_INDEX_KEY: &str = "last_index";
const HARD_STATE_KEY: &str = "hard_state";
const CONF_STATE_KEY: &str = "conf_state";

impl NodeStore {
    pub fn create(id: u64) -> Result<Self> {
        let path = format!("store/raft-{id}.mdb");

        Persy::create(&path)?;
        let persy: Persy = Persy::open(path, persy::Config::new())?;
        let mut tx = persy.begin()?;

        let store = Self { persy };

        tx.create_index::<u64, ByteVec>(ENTRIES_KEY, ValueMode::Exclusive)?;

        tx.create_segment(SNAPSHOT_KEY)?;
        tx.create_segment(HARD_STATE_KEY)?;
        tx.create_segment(CONF_STATE_KEY)?;
        tx.create_segment(LAST_INDEX_KEY)?;

        store.set_hard_state(&mut tx, &HardState::new())?;
        store.set_conf_state(&mut tx, &ConfState::new())?;
        store.append_entries(&mut tx, &[Entry::default()])?;

        Ok(store)
    }

    pub fn set_hard_state(&self, tx: &mut Transaction, hard_state: &HardState) -> Result<()> {
        todo!()
    }

    pub fn set_conf_state(&self, tx: &mut Transaction, conf_state: &ConfState) -> Result<()> {
        todo!()
    }

    pub fn append_entries(&self, tx: &mut Transaction, entries: &[Entry]) -> Result<()> {
        todo!()
    }
}

impl Storage for NodeStore {
    fn initial_state(&self) -> raft::Result<raft::RaftState> {
        todo!()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: raft::GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        todo!()
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        todo!()
    }

    fn first_index(&self) -> raft::Result<u64> {
        todo!()
    }

    fn last_index(&self) -> raft::Result<u64> {
        todo!()
    }

    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        todo!()
    }
}
