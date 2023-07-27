use std::{cmp::Ordering, sync::Arc};

use crate::prelude::*;

use persy::{ByteVec, Persy, Transaction, TxIndexIter, ValueMode};
use protobuf::Message;
use raft::{prelude::*, Storage};

use protobuf::Message as PbMessage;

use crate::prelude::Result;

pub struct NodeStorageCore {
    persy: Persy,
}

const ENTRIES_INDEX: &str = "snapshot";
const METADATA_INDEX: &str = "metadata";

const SNAPSHOT_METADATA_KEY: &str = "snapshot";
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
        store.set_snapshot_metadata(&mut tx, &SnapshotMetadata::default())?;

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

    pub fn set_snapshot_metadata(
        &self,
        tx: &mut Transaction,
        metadata: &SnapshotMetadata,
    ) -> Result<()> {
        tx.put::<String, ByteVec>(
            METADATA_INDEX,
            SNAPSHOT_METADATA_KEY.to_string(),
            metadata.write_to_bytes()?.into(),
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
        self.set_last_index(tx, last_index)
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

    pub fn get_snapshot_metadata(&self, tx: &mut Transaction) -> Result<SnapshotMetadata> {
        let data = &tx
            .get::<String, ByteVec>(METADATA_INDEX, &SNAPSHOT_METADATA_KEY.into())?
            .nth(0)
            .unwrap();

        Ok(SnapshotMetadata::parse_from_bytes(data)?)
    }

    pub fn get_snapshot(&self, tx: &mut Transaction) -> Result<Snapshot> {
        let mut snapshot = Snapshot::default();

        let meta = snapshot.mut_metadata();
        let self_meta = self.get_snapshot_metadata(tx)?;
        meta.index = self.get_hard_state(tx)?.commit;
        meta.term = match meta.index.cmp(&self_meta.index) {
            Ordering::Less => {
                panic!(
                    "commit {} < snapshot metadata index {}",
                    meta.index, self_meta.index
                )
            }
            Ordering::Equal => self_meta.term,
            Ordering::Greater => self.get_entry(meta.index, tx)?.term,
        };

        let data = &tx
            .get::<String, ByteVec>(METADATA_INDEX, &SNAPSHOT_METADATA_KEY.into())?
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
            Ok(self.get_snapshot_metadata(tx)?.index)
        }
    }

    pub fn get_first_index(&self, tx: &mut Transaction) -> Result<u64> {
        let mut iter: TxIndexIter<u64, ByteVec> = tx.range(ENTRIES_INDEX, ..)?;

        if let Some(mut e) = iter.nth(0) {
            let e = Entry::parse_from_bytes(&e.1.nth(0).unwrap())
                .expect("Entry bytes should not be malformed.");

            Ok(e.index)
        } else {
            Ok(self.get_snapshot_metadata(tx)?.index + 1)
        }
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

        let mut res = Vec::new();

        for (i, (_, mut e)) in iter.enumerate() {
            let entry = Entry::parse_from_bytes(&e.nth(0).unwrap())
                .expect("Entry bytes should not be malformed.");

            total_bytes += entry.compute_size() as u64;

            if max_size.is_some_and(|max_size| total_bytes > max_size) && i != 0 {
                break;
            }

            res.push(entry);
        }

        Ok(res)
    }

    pub fn get_entry(&self, index: u64, tx: &mut Transaction) -> Result<Entry> {
        let data = tx
            .get::<u64, ByteVec>(ENTRIES_INDEX, &index)?
            .nth(0)
            .ok_or(raft::Error::Store(raft::StorageError::Unavailable))?;

        Ok(Entry::parse_from_bytes(&data)?)
    }

    #[cfg(test)]
    fn set_entries(&self, entries: &[Entry]) -> Result<()> {
        let mut tx = self.persy.begin()?;
        tx.drop_index(ENTRIES_INDEX)?;
        tx.create_index::<u64, ByteVec>(ENTRIES_INDEX, ValueMode::Exclusive)?;

        // let mut to_add = [Entry::default()].to_vec();
        // to_add.append(&mut entries.to_vec());

        // self.append_entries(&mut tx, &to_add)?;

        self.append_entries(&mut tx, entries)?;

        tx.prepare()?.commit()?;
        Ok(())
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

        if low
            < store
                .get_first_index(&mut tx)
                .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?
        {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        let res = store
            .get_entries(low, high, max_size, &mut tx)
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())));

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

        println!("GOT STUFF first: {first_index} last: {last_index} hs: {hs:?}");

        let res = if idx == hs.commit {
            Ok(hs.term)
        } else if idx < first_index {
            Err(raft::Error::Store(raft::StorageError::Compacted))
        } else if idx > last_index {
            Err(raft::Error::Store(raft::StorageError::Unavailable))
        } else {
            store
                .get_entry(idx, &mut tx)
                .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))
                .map(|e| e.term)
        };

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

        store.set_snapshot_metadata(&mut tx, &metadata)?;

        tx.prepare()?.commit()?;
        Ok(())
    }

    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let store = &self.0;
        let mut tx = store.persy.begin()?;

        let metadata = snapshot.get_metadata();

        println!(
            "FIRST {} META {}",
            self.0.get_first_index(&mut tx)?,
            metadata.index
        );
        if self.0.get_first_index(&mut tx)? > metadata.index {
            Err(raft::Error::Store(raft::StorageError::SnapshotOutOfDate))?;
        }

        let conf_state = metadata.get_conf_state();
        let mut hard_state = store.get_hard_state(&mut tx)?;

        hard_state.set_term(metadata.term);
        hard_state.set_commit(metadata.index);

        store.set_hard_state(&mut tx, &hard_state)?;
        store.set_conf_state(&mut tx, &conf_state)?;
        store.set_last_index(&mut tx, metadata.index)?;
        store.set_snapshot_metadata(&mut tx, metadata)?;

        tx.prepare()?.commit()?;
        Ok(())
    }

    fn compact(&self, index: u64) -> Result<()> {
        let store = &self.0;
        let mut tx = store.persy.begin()?;

        let last_index = store.get_last_index(&mut tx)?;
        assert!(last_index >= index);

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

#[cfg(test)]
mod test {
    use protobuf::Message as PbMessage;
    use raft::prelude::*;
    use raft::Error as RaftError;
    use raft::GetEntriesContext;
    use raft::StorageError;

    use std::panic;
    use std::panic::AssertUnwindSafe;
    use std::{env, fs};

    use super::LogStore;
    use super::NodeStorage;

    macro_rules! in_temp_dir {
        ($block:block) => {
            let tmpdir = tempfile::tempdir().unwrap();
            env::set_current_dir(&tmpdir).unwrap();
            fs::create_dir("store").unwrap();

            $block;
        };
    }

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::default();
        e.term = term;
        e.index = index;
        e
    }

    fn size_of<T: PbMessage>(m: &T) -> u32 {
        m.compute_size()
    }

    fn new_snapshot(index: u64, term: u64, voters: Vec<u64>) -> Snapshot {
        let mut s = Snapshot::default();
        s.mut_metadata().index = index;
        s.mut_metadata().term = term;
        s.mut_metadata().mut_conf_state().voters = voters;
        s
    }

    #[test]
    fn test_storage_term() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (2, Err(RaftError::Store(StorageError::Compacted))),
            (3, Ok(3)),
            (4, Ok(4)),
            (5, Ok(5)),
            (6, Err(RaftError::Store(StorageError::Unavailable))),
        ];

        for (i, (idx, wterm)) in tests.drain(..).enumerate() {
            in_temp_dir!({
                let storage = NodeStorage::create(1).unwrap();
                storage.0.set_entries(&ents).unwrap();

                let t = storage.term(idx);
                if t != wterm {
                    panic!("#{}: expect res {:?}, got {:?}", i, wterm, t);
                }
            });
        }
    }

    #[test]
    fn test_storage_entries() {
        let ents = vec![
            new_entry(3, 3),
            new_entry(4, 4),
            new_entry(5, 5),
            new_entry(6, 6),
        ];
        let max_u64 = u64::max_value();
        let mut tests = vec![
            (
                2,
                6,
                max_u64,
                Err(RaftError::Store(StorageError::Compacted)),
            ),
            (3, 4, max_u64, Ok(vec![new_entry(3, 3)])),
            (4, 5, max_u64, Ok(vec![new_entry(4, 4)])),
            (4, 6, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            (
                4,
                7,
                max_u64,
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
            // even if maxsize is zero, the first entry should be returned
            (4, 7, 0, Ok(vec![new_entry(4, 4)])),
            // limit to 2
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2])),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) / 2),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) - 1),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            // all
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3])),
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
        ];
        for (i, (lo, hi, maxsize, wentries)) in tests.drain(..).enumerate() {
            in_temp_dir!({
                let storage = NodeStorage::create(1).unwrap();
                storage.0.set_entries(&ents).unwrap();

                let e = storage.entries(lo, hi, maxsize, GetEntriesContext::empty(false));
                if e != wentries {
                    panic!("#{}: expect entries {:?}, got {:?}", i, wentries, e);
                }
            });
        }
    }

    #[test]
    fn test_storage_last_index() {
        in_temp_dir!({
            let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
            let storage = NodeStorage::create(1).unwrap();
            storage.0.set_entries(&ents);

            let wresult = Ok(5);
            let result = storage.last_index();
            if result != wresult {
                panic!("want {:?}, got {:?}", wresult, result);
            }

            storage.append(&[new_entry(6, 5)]).unwrap();
            let wresult = Ok(6);
            let result = storage.last_index();
            if result != wresult {
                panic!("want {:?}, got {:?}", wresult, result);
            }
        });
    }

    #[test]
    fn test_storage_first_index() {
        in_temp_dir!({
            let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
            let storage = NodeStorage::create(1).unwrap();
            storage.0.set_entries(&ents);

            assert_eq!(storage.first_index(), Ok(3));
            storage.compact(4).unwrap();
            assert_eq!(storage.first_index(), Ok(4));
        });
    }

    #[test]
    fn test_storage_compact() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![(2, 3, 3, 3), (3, 3, 3, 3), (4, 4, 4, 2), (5, 5, 5, 1)];
        for (i, (idx, windex, wterm, wlen)) in tests.drain(..).enumerate() {
            in_temp_dir!({
                let storage = NodeStorage::create(1).unwrap();
                storage.0.set_entries(&ents);

                storage.compact(idx).unwrap();
                let index = storage.first_index().unwrap();
                if index != windex {
                    panic!("#{}: want {}, index {}", i, windex, index);
                }
                let term = if let Ok(v) =
                    storage.entries(index, index + 1, 1, GetEntriesContext::empty(false))
                {
                    v.first().map_or(0, |e| e.term)
                } else {
                    0
                };
                if term != wterm {
                    panic!("#{}: want {}, term {}", i, wterm, term);
                }
                let last = storage.last_index().unwrap();
                let len = storage
                    .entries(index, last + 1, 100, GetEntriesContext::empty(false))
                    .unwrap()
                    .len();
                if len != wlen {
                    panic!("#{}: want {}, term {}", i, wlen, len);
                }
            });
        }
    }

    #[test]
    fn test_storage_append() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (
                vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
                Some(vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
                Some(vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)]),
            ),
            (
                vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ],
                Some(vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ]),
            ),
            // overwrite compacted raft logs is not allowed
            (
                vec![new_entry(2, 3), new_entry(3, 3), new_entry(4, 5)],
                None,
            ),
            // truncate the existing entries and append
            (
                vec![new_entry(4, 5)],
                Some(vec![new_entry(3, 3), new_entry(4, 5)]),
            ),
            // direct append
            (
                vec![new_entry(6, 6)],
                Some(vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 6),
                ]),
            ),
        ];
        for (i, (entries, wentries)) in tests.drain(..).enumerate() {
            in_temp_dir!({
                let storage = NodeStorage::create(1).unwrap();
                storage.0.set_entries(&ents);
                let res = panic::catch_unwind(AssertUnwindSafe(|| storage.append(&entries)));
                if let Some(wentries) = wentries {
                    let _ = res.unwrap();
                    let e = &storage
                        .entries(
                            0,
                            storage.last_index().unwrap() + 1,
                            None,
                            GetEntriesContext::empty(false),
                        )
                        .unwrap();

                    if *e != wentries {
                        panic!("#{}: want {:?}, entries {:?}", i, wentries, e);
                    }
                } else {
                    res.unwrap_err();
                }
            });
        }
    }

    #[test]
    fn test_storage_apply_snapshot() {
        let nodes = vec![1, 2, 3];
        in_temp_dir!({
            let storage = NodeStorage::create(1).unwrap();

            // Apply snapshot successfully
            let snap = new_snapshot(4, 4, nodes.clone());
            storage.apply_snapshot(snap).unwrap();

            // Apply snapshot fails due to StorageError::SnapshotOutOfDate
            let snap = new_snapshot(3, 3, nodes);
            storage.apply_snapshot(snap).unwrap_err();
        });
    }
}
