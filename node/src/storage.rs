use std::sync::Arc;

use crate::prelude::*;

use persy::{ByteVec, Persy, Transaction, TxIndexIter, ValueMode};
use protobuf::Message;
use raft::{prelude::*, Storage};

use crate::prelude::Result;

pub struct NodeStorageCore {
    persy: Persy,
}

const ENTRIES_INDEX: &str = "snapshot";
const METADATA_INDEX: &str = "metadata";

const SNAPSHOT_KEY: &str = "snapshot";
const LAST_INDEX_KEY: &str = "last_index";
const HARD_STATE_KEY: &str = "hard_state";
const CONF_STATE_KEY: &str = "conf_state";

impl NodeStorageCore {
    pub fn create(id: u64) -> Result<Self> {
        let path = format!("store/raft-{id}.mdb");

        println!("CREATING {path}");
        Persy::create(&path)?;
        let persy: Persy = Persy::open(path, persy::Config::new())?;
        let mut tx = persy.begin()?;

        let store = Self { persy };

        tx.create_index::<u64, ByteVec>(ENTRIES_INDEX, ValueMode::Exclusive)?;
        tx.create_index::<String, ByteVec>(METADATA_INDEX, ValueMode::Replace)?;

        store.set_hard_state(&mut tx, &HardState::default())?;
        store.set_conf_state(&mut tx, &ConfState::default())?;
        store.append_entries(&mut tx, &[Entry::default()])?;

        tx.prepare()?.commit()?;

        Ok(store)
    }

    pub fn set_hard_state(&self, tx: &mut Transaction, hard_state: &HardState) -> Result<()> {
        println!("SETTING HARDSTATE: {:?}", hard_state);
        tx.put::<String, ByteVec>(
            METADATA_INDEX,
            HARD_STATE_KEY.to_string(),
            hard_state.write_to_bytes()?.into(),
        )?;

        Ok(())
    }

    pub fn set_conf_state(&self, tx: &mut Transaction, conf_state: &ConfState) -> Result<()> {
        println!("SETTING CONFSTATE: {:?}", conf_state);
        tx.put::<String, ByteVec>(
            METADATA_INDEX,
            CONF_STATE_KEY.to_string(),
            conf_state.write_to_bytes()?.into(),
        )?;

        Ok(())
    }

    pub fn set_snapshot(&self, tx: &mut Transaction, snapshot: &Snapshot) -> Result<()> {
        tx.put::<String, ByteVec>(
            METADATA_INDEX,
            SNAPSHOT_KEY.to_string(),
            snapshot.write_to_bytes()?.into(),
        )?;

        Ok(())
    }

    pub fn set_last_index(&self, tx: &mut Transaction, last_index: u64) -> Result<()> {
        tx.put::<String, ByteVec>(
            METADATA_INDEX,
            LAST_INDEX_KEY.to_string(),
            last_index.to_le_bytes().to_vec().into(),
        )?;

        Ok(())
    }

    pub fn append_entries(&self, tx: &mut Transaction, entries: &[Entry]) -> Result<()> {
        let mut last_index = self.get_last_index(tx)?;

        println!("APPENDING ENTRIES");
        for entry in entries {
            let index = entry.index;
            println!("APPENDING ENTRY to index {}", index);
            last_index = std::cmp::max(last_index, index);
            tx.put::<u64, ByteVec>(ENTRIES_INDEX, index, entry.write_to_bytes()?.into())?;
        }
        Ok(())
    }

    pub fn get_hard_state(&self, tx: &mut Transaction) -> Result<HardState> {
        let data = &tx
            .get::<String, ByteVec>(METADATA_INDEX, &HARD_STATE_KEY.into())?
            .nth(0)
            .unwrap();

        Ok(HardState::parse_from_bytes(data)?)
    }

    pub fn get_conf_state(&self, tx: &mut Transaction) -> Result<ConfState> {
        let data = &tx
            .get::<String, ByteVec>(METADATA_INDEX, &CONF_STATE_KEY.into())?
            .nth(0)
            .unwrap();

        Ok(ConfState::parse_from_bytes(data)?)
    }

    pub fn get_snapshot(&self, tx: &mut Transaction) -> Result<Snapshot> {
        let data = &tx
            .get::<String, ByteVec>(METADATA_INDEX, &SNAPSHOT_KEY.into())?
            .nth(0)
            .unwrap();

        Ok(Snapshot::parse_from_bytes(data)?)
    }

    pub fn get_last_index(&self, tx: &mut Transaction) -> Result<u64> {
        let mut data = tx.get::<String, ByteVec>(METADATA_INDEX, &LAST_INDEX_KEY.into())?;

        if let Some(data) = data.nth(0) {
            Ok(u64::from_le_bytes(
                data[..].try_into().map_err(|_| Error::ConverstionError)?,
            ))
        } else {
            Ok(0)
        }
    }

    pub fn get_first_index(&self, tx: &mut Transaction) -> Result<u64> {
        Ok(self.get_entry(0, tx)?.index + 1)
    }

    pub fn get_entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        tx: &mut Transaction,
    ) -> Result<Vec<Entry>> {
        assert!(!self.get_last_index(tx)? + 1 >= high);

        let iter: TxIndexIter<u64, ByteVec> = tx.range(ENTRIES_INDEX, low..high)?;
        let mut total_bytes = 0;
        let max_size = max_size.into();

        Ok(iter
            .take_while(|(_, e)| match max_size {
                Some(max_size) => {
                    total_bytes += e.len() as u64;
                    total_bytes <= max_size
                }
                None => true,
            })
            .map(|(_, mut e)| {
                Entry::parse_from_bytes(&e.nth(0).unwrap())
                    .expect("Entry bytes should not be malformed.")
            })
            .collect())
    }

    pub fn get_entry(&self, index: u64, tx: &mut Transaction) -> Result<Entry> {
        let data = tx
            .get::<u64, ByteVec>(ENTRIES_INDEX, &index)?
            .nth(0)
            .unwrap();

        Ok(Entry::parse_from_bytes(&data)?)
    }
}

#[derive(Clone)]
pub struct NodeStorage(Arc<NodeStorageCore>);

impl NodeStorage {
    pub fn create(id: u64) -> Result<Self> {
        let core = NodeStorageCore::create(id)?;
        Ok(Self(Arc::new(core)))
    }
}

impl Storage for NodeStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let store = &self.0;
        let mut tx = store
            .persy
            .begin()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?;

        let state = RaftState {
            hard_state: store
                .get_hard_state(&mut tx)
                .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?,
            conf_state: store
                .get_conf_state(&mut tx)
                .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?,
        };

        tx.prepare()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?
            .commit()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?;

        Ok(state)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: raft::GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let store = &self.0;
        let mut tx = store
            .persy
            .begin()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?;

        let res = store
            .get_entries(low, high, max_size, &mut tx)
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())));

        tx.prepare()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?
            .commit()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?;

        res
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let store = &self.0;
        let mut tx = store
            .persy
            .begin()
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;

        let first_index = store
            .get_first_index(&mut tx)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
        let last_index = store
            .get_last_index(&mut tx)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
        let hs = store
            .get_hard_state(&mut tx)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;

        let res = if idx == hs.commit {
            Ok(hs.term)
        } else if idx < first_index - 1 {
            Err(raft::Error::Store(raft::StorageError::Compacted))
        } else if idx > last_index {
            Err(raft::Error::Store(raft::StorageError::Unavailable))
        } else {
            store
                .get_entry(idx, &mut tx)
                .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))
                .map(|e| e.term)
        };

        tx.prepare()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?
            .commit()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?;

        res
    }

    fn first_index(&self) -> raft::Result<u64> {
        let store = &self.0;
        let mut tx = store
            .persy
            .begin()
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;

        let first_index = store
            .get_first_index(&mut tx)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable));

        tx.prepare()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?
            .commit()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?;

        first_index
    }

    fn last_index(&self) -> raft::Result<u64> {
        let store = &self.0;
        let mut tx = store
            .persy
            .begin()
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;

        let last_index = store
            .get_last_index(&mut tx)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable));

        tx.prepare()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?
            .commit()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?;

        last_index
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let store = &self.0;
        let mut tx = store
            .persy
            .begin()
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;

        let snap = store
            .get_snapshot(&mut tx)
            .map_err(|_| raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable));

        tx.prepare()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?
            .commit()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?;

        snap
    }
}

pub trait LogStore: Storage {
    fn append(&self, entries: &[Entry]) -> Result<()>;
    fn set_hard_state(&self, hard_state: &HardState) -> Result<()>;
    fn get_hard_state(&self) -> Result<HardState>;
    fn set_conf_state(&self, conf_state: &ConfState) -> Result<()>;
    fn create_snapshot(&self, data: Vec<u8>) -> Result<()>;
    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<()>;
    fn compact(&self, index: u64) -> Result<()>;
}

impl LogStore for NodeStorage {
    fn append(&self, entries: &[Entry]) -> Result<()> {
        let store = &self.0;
        let mut tx = store.persy.begin()?;

        store.append_entries(&mut tx, entries)?;

        tx.prepare()?.commit()?;
        Ok(())
    }

    fn set_hard_state(&self, hard_state: &HardState) -> Result<()> {
        let store = &self.0;
        let mut tx = store.persy.begin()?;

        store.set_hard_state(&mut tx, hard_state)?;

        tx.prepare()?.commit()?;
        Ok(())
    }

    fn set_conf_state(&self, conf_state: &ConfState) -> Result<()> {
        let store = &self.0;
        let mut tx = store.persy.begin()?;

        store.set_conf_state(&mut tx, conf_state)?;

        tx.prepare()?.commit()?;
        Ok(())
    }

    fn create_snapshot(&self, data: Vec<u8>) -> Result<()> {
        let store = &self.0;
        let mut tx = store.persy.begin()?;

        let hard_state = store.get_hard_state(&mut tx)?;
        let conf_state = store.get_conf_state(&mut tx)?;

        let mut snapshot = Snapshot::default();
        snapshot.set_data(data.into());

        let metadata = snapshot.mut_metadata();
        metadata.set_conf_state(conf_state);
        metadata.set_index(hard_state.commit);
        metadata.set_term(hard_state.term);

        store.set_snapshot(&mut tx, &snapshot)?;

        tx.prepare()?.commit()?;
        Ok(())
    }

    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let store = &self.0;
        let mut tx = store.persy.begin()?;

        let metadata = snapshot.get_metadata();
        let conf_state = metadata.get_conf_state();
        let mut hard_state = store.get_hard_state(&mut tx)?;

        hard_state.set_term(metadata.term);
        hard_state.set_commit(metadata.index);

        store.set_hard_state(&mut tx, &hard_state)?;
        store.set_conf_state(&mut tx, &conf_state)?;
        store.set_last_index(&mut tx, metadata.index)?;

        tx.prepare()?.commit()?;
        Ok(())
    }

    fn compact(&self, index: u64) -> Result<()> {
        let store = &self.0;
        let mut tx = store.persy.begin()?;

        let last_index = store.get_last_index(&mut tx)?;
        assert!(last_index > index + 1);
        for i in 0..index {
            tx.remove::<u64, ByteVec>(ENTRIES_INDEX, i, None)?;
        }

        tx.prepare()?.commit()?;
        Ok(())
    }

    fn get_hard_state(&self) -> Result<HardState> {
        let store = &self.0;
        let mut tx = store.persy.begin()?;

        let hard_state = store.get_hard_state(&mut tx)?;

        tx.prepare()?.commit()?;
        Ok(hard_state)
    }
}
