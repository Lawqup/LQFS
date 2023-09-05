use std::{cmp::Ordering, path::Path, sync::Arc};

use prost::Message as PRMessage;
use raft::{prelude::*, Storage};
use sled::{Db, Tree};

use crate::prelude::*;

pub struct NodeStorageCore {
    db: Db,
    entries_tree: Tree,
}

const ENTRIES_TREE: &str = "snapshot";
const METADATA_INDEX: &str = "metadata";

const SNAPSHOT_METADATA_KEY: &str = "snapshot";
const HARD_STATE_KEY: &str = "hard_state";
const CONF_STATE_KEY: &str = "conf_state";

impl NodeStorageCore {
    pub fn create(id: u64) -> Result<Self> {
        let path = format!("store/metadata-{id}/");

        println!("CREATING {path}");
        let db = sled::open(path)?;

        let entries_tree = db.open_tree(ENTRIES_TREE)?;

        let store = Self { db, entries_tree };
        store.set_hard_state(&HardState::default())?;
        store.set_conf_state(&ConfState::default())?;
        store.set_snapshot_metadata(&SnapshotMetadata::default())?;

        store.db.flush()?;

        Ok(store)
    }

    pub fn restore(id: u64) -> Result<Self> {
        let path = format!("store/metadata-{id}/");

        println!("PATH: {}", path);
        if !Path::new(&path).exists() {
            return Err(Error::InitError);
        }

        let db = sled::open(path.clone())?;

        let entries_tree = db.open_tree(ENTRIES_TREE)?;

        Ok(Self { db, entries_tree })
    }

    pub fn set_hard_state(&self, hard_state: &HardState) -> Result<()> {
        self.db.insert(HARD_STATE_KEY, hard_state.encode_to_vec())?;

        Ok(())
    }

    pub fn set_conf_state(&self, conf_state: &ConfState) -> Result<()> {
        self.db.insert(CONF_STATE_KEY, conf_state.encode_to_vec())?;

        Ok(())
    }

    pub fn set_snapshot_metadata(&self, metadata: &SnapshotMetadata) -> Result<()> {
        self.db
            .insert(SNAPSHOT_METADATA_KEY, metadata.encode_to_vec())?;

        Ok(())
    }

    pub fn append_entries(&self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let first_index = self.get_first_index()?;
        let last_index = self.get_last_index()?;

        if first_index > entries[0].index {
            panic!(
                "overwrite compacted raft logs, compacted: {}, append: {}",
                first_index - 1,
                entries[0].index,
            );
        }

        if last_index + 1 < entries[0].index {
            panic!(
                "raft logs should be continuous, last index: {}, new appended: {}",
                last_index, entries[0].index,
            );
        }

        for index in entries[0].index..=last_index {
            self.entries_tree.remove(index.to_be_bytes())?;
        }

        for entry in entries {
            let index = entry.index;
            self.entries_tree
                .insert(index.to_be_bytes(), entry.encode_to_vec())?;
        }

        Ok(())
    }

    pub fn get_hard_state(&self) -> Result<HardState> {
        Ok(HardState::decode(
            self.db
                .get(HARD_STATE_KEY)?
                .expect("Hard state didn't exist")
                .to_vec()
                .as_slice(),
        )?)
    }

    pub fn get_conf_state(&self) -> Result<ConfState> {
        Ok(ConfState::decode(
            self.db
                .get(CONF_STATE_KEY)?
                .expect("Conf state didn't exist")
                .to_vec()
                .as_slice(),
        )?)
    }

    pub fn get_snapshot_metadata(&self) -> Result<SnapshotMetadata> {
        Ok(SnapshotMetadata::decode(
            self.db
                .get(SNAPSHOT_METADATA_KEY)?
                .expect("Snapshot metadata did not exist")
                .to_vec()
                .as_slice(),
        )?)
    }

    pub fn get_snapshot(&self) -> Result<Snapshot> {
        let mut snapshot = Snapshot::default();

        let meta = snapshot.mut_metadata();
        let self_meta = self.get_snapshot_metadata()?;
        meta.index = self.get_hard_state()?.commit;
        meta.term = match meta.index.cmp(&self_meta.index) {
            Ordering::Less => {
                panic!(
                    "commit {} < snapshot metadata index {}",
                    meta.index, self_meta.index
                )
            }
            Ordering::Equal => self_meta.term,
            Ordering::Greater => self.get_entry(meta.index)?.term,
        };

        meta.set_conf_state(self.get_conf_state()?);
        Ok(snapshot)
    }

    pub fn get_last_index(&self) -> Result<u64> {
        if let Ok(Some(e)) = self.entries_tree.last() {
            let e = Entry::decode(e.1.to_vec().as_slice())
                .expect("Entry bytes should not be malformed.");

            Ok(e.index)
        } else {
            Ok(self.get_snapshot_metadata()?.index)
        }
    }

    pub fn get_first_index(&self) -> Result<u64> {
        if let Ok(Some(e)) = self.entries_tree.first() {
            let e = Entry::decode(e.1.to_vec().as_slice())
                .expect("Entry bytes should not be malformed.");

            Ok(e.index)
        } else {
            Ok(self.get_snapshot_metadata()?.index + 1)
        }
    }

    pub fn get_entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> Result<Vec<Entry>> {
        assert!(!self.get_last_index()? + 1 >= high);

        let mut total_bytes = 0;
        let max_size = max_size.into();

        let mut res = Vec::new();

        for (i, ent) in self
            .entries_tree
            .range(low.to_be_bytes()..high.to_be_bytes())
            .enumerate()
        {
            let entry = match ent {
                Ok((_, e)) => Entry::decode(e.to_vec().as_slice())
                    .expect("Entry bytes should not be malformed."),
                Err(e) => return Err(e.into()),
            };

            total_bytes += entry.encoded_len() as u64;

            if max_size.is_some_and(|max_size| total_bytes > max_size) && i != 0 {
                break;
            }

            res.push(entry);
        }

        Ok(res)
    }

    pub fn get_entry(&self, index: u64) -> Result<Entry> {
        Ok(Entry::decode(
            self.entries_tree
                .get(index.to_be_bytes())?
                .expect("Entry didn't exist")
                .to_vec()
                .as_slice(),
        )?)
    }

    #[cfg(test)]
    fn set_entries(&self, entries: &[Entry]) -> Result<()> {
        self.entries_tree.clear()?;

        for entry in entries {
            let index = entry.index;
            self.entries_tree
                .insert(index.to_be_bytes(), entry.encode_to_vec())?;
        }

        self.entries_tree.flush()?;
        self.db.flush()?;
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

    pub fn restore(id: u64) -> Result<Self> {
        let core = NodeStorageCore::restore(id)?;
        Ok(Self(Arc::new(core)))
    }
}

impl Storage for NodeStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let store = &self.0;

        let state = RaftState {
            hard_state: store
                .get_hard_state()
                .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?,
            conf_state: store
                .get_conf_state()
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

        if low
            < store
                .get_first_index()
                .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?
        {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        store
            .get_entries(low, high, max_size)
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let store = &self.0;

        let first_index = store
            .get_first_index()
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
        let last_index = store
            .get_last_index()
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
        let hs = store
            .get_hard_state()
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;

        if idx == hs.commit {
            Ok(hs.term)
        } else if idx < first_index {
            Err(raft::Error::Store(raft::StorageError::Compacted))
        } else if idx > last_index {
            Err(raft::Error::Store(raft::StorageError::Unavailable))
        } else {
            store
                .get_entry(idx)
                .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))
                .map(|e| e.term)
        }
    }

    fn first_index(&self) -> raft::Result<u64> {
        let store = &self.0;
        store
            .get_first_index()
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))
    }

    fn last_index(&self) -> raft::Result<u64> {
        let store = &self.0;
        store
            .get_last_index()
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let store = &self.0;
        let mut snap = store
            .get_snapshot()
            .map_err(|_| raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable))?;

        if snap.get_metadata().index < request_index {
            snap.mut_metadata().index = request_index;
        }

        Ok(snap)
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
        dbg!(entries);
        store.append_entries(entries)?;

        store.entries_tree.flush()?;
        Ok(())
    }

    fn set_hard_state(&self, hard_state: &HardState) -> Result<()> {
        let store = &self.0;

        store.set_hard_state(hard_state)?;

        store.db.flush()?;
        Ok(())
    }

    fn set_conf_state(&self, conf_state: &ConfState) -> Result<()> {
        let store = &self.0;

        store.set_conf_state(conf_state)?;

        store.db.flush()?;
        Ok(())
    }

    fn create_snapshot(&self, data: Vec<u8>) -> Result<()> {
        let store = &self.0;

        let hard_state = store.get_hard_state()?;
        let conf_state = store.get_conf_state()?;

        let mut snapshot = Snapshot::default();
        snapshot.set_data(data);

        let metadata = snapshot.mut_metadata();
        metadata.set_conf_state(conf_state);
        metadata.set_index(hard_state.commit);
        metadata.set_term(hard_state.term);

        store.set_snapshot_metadata(metadata)?;

        store.db.flush()?;
        Ok(())
    }

    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let store = &self.0;

        let metadata = snapshot.get_metadata();

        if self.0.get_first_index()? > metadata.index {
            Err(raft::Error::Store(raft::StorageError::SnapshotOutOfDate))?;
        }

        let conf_state = metadata.get_conf_state();
        let mut hard_state = store.get_hard_state()?;

        hard_state.set_term(metadata.term);
        hard_state.set_commit(metadata.index);

        store.set_hard_state(&hard_state)?;
        store.set_conf_state(conf_state)?;
        store.set_snapshot_metadata(metadata)?;

        store.db.flush()?;
        Ok(())
    }

    fn compact(&self, index: u64) -> Result<()> {
        let store = &self.0;

        assert!(index <= store.get_last_index()? + 1);

        for i in store.get_first_index()?..index {
            store.entries_tree.remove(i.to_be_bytes())?;
        }

        store.entries_tree.flush()?;
        Ok(())
    }

    fn get_hard_state(&self) -> Result<HardState> {
        let store = &self.0;

        let hard_state = store.get_hard_state()?;

        Ok(hard_state)
    }
}

#[cfg(test)]
mod test {
    use prost::Message as PRMessage;
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
            fs::create_dir("store/node-1").unwrap();

            $block;
        };
    }

    fn new_entry(index: u64, term: u64) -> Entry {
        Entry {
            term,
            index,
            ..Default::default()
        }
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
                (ents[1].encoded_len() + ents[2].encoded_len()) as u64,
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                (ents[1].encoded_len() + ents[2].encoded_len() + ents[3].encoded_len() / 2) as u64,
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                (ents[1].encoded_len() + ents[2].encoded_len() + ents[3].encoded_len() - 1) as u64,
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            // all
            (
                4,
                7,
                (ents[1].encoded_len() + ents[2].encoded_len() + ents[3].encoded_len()) as u64,
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
        ];
        for (i, (lo, hi, maxsize, wentries)) in tests.drain(..).enumerate() {
            in_temp_dir!({
                let storage = NodeStorage::create(1).unwrap();
                storage.0.set_entries(ents.as_slice()).unwrap();

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
            storage.0.set_entries(&ents).unwrap();

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
            storage.0.set_entries(&ents).unwrap();

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
                storage.0.set_entries(&ents).unwrap();

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
                storage.0.set_entries(&ents).unwrap();

                let res = panic::catch_unwind(AssertUnwindSafe(|| storage.append(&entries)));
                if let Some(wentries) = wentries {
                    let _ = res.unwrap();
                    let e = &storage
                        .0
                        .get_entries(0, storage.last_index().unwrap() + 1, None)
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
    fn test_storage_create_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let nodes = vec![1, 2, 3];
        let conf_state = ConfState {
            voters: nodes.clone(),
            ..Default::default()
        };

        let mut tests = vec![
            (4, Ok(new_snapshot(4, 4, nodes.clone())), 0),
            (5, Ok(new_snapshot(5, 5, nodes.clone())), 5),
            (5, Ok(new_snapshot(6, 5, nodes)), 6),
        ];
        for (i, (idx, wresult, windex)) in tests.drain(..).enumerate() {
            in_temp_dir!({
                let storage = NodeStorage::create(1).unwrap();
                storage.0.set_entries(&ents).unwrap();

                let mut hs = storage.get_hard_state().unwrap();
                hs.commit = idx;
                hs.term = idx;

                storage.set_hard_state(&hs).unwrap();
                storage.set_conf_state(&conf_state).unwrap();

                let result = storage.snapshot(windex, 0);
                if result != wresult {
                    panic!("#{}: want {:?}, got {:?}", i, wresult, result);
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
