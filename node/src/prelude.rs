use std::sync::Mutex;

use raft::prelude::ConfChange;
pub use slog::{debug, error, info, o, Drain, Logger};
use thiserror::Error as ThisError;
pub use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Proposal {
    pub id: Uuid,
    pub from: u64,
    pub conf_change: Option<ConfChange>,
    pub fragment: Option<Vec<u8>>,
}

impl Proposal {
    pub fn new_fragment(from: u64, fragment: Vec<u8>) -> Self {
        Self {
            id: Uuid::new_v4(),
            from,
            conf_change: None,
            fragment: Some(fragment),
        }
    }

    pub fn new_conf_change(from: u64, conf_change: ConfChange) -> Self {
        Self {
            id: Uuid::new_v4(),
            from,
            conf_change: Some(conf_change),
            fragment: None,
        }
    }

    pub fn is_fragment(&self) -> bool {
        self.fragment.is_some()
    }

    pub fn context_bytes(&self) -> Vec<u8> {
        let mut res = self.id.as_bytes().to_vec();
        res.extend_from_slice(&self.from.to_le_bytes());
        res
    }

    pub fn context_from_bytes(bytes: &[u8]) -> (Uuid, u64) {
        let id = Uuid::from_bytes(bytes[0..16].try_into().expect("Too few bytes for context"));
        let from = u64::from_le_bytes(bytes[16..24].try_into().expect("Too few bytes for context"));

        (id, from)
    }
}
pub fn build_default_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = Mutex::new(slog_term::FullFormat::new(decorator).build()).fuse();

    Logger::root(drain, o!())
}

pub fn build_debug_logger() -> Logger {
    let decorator = slog_term::PlainDecorator::new(slog_term::TestStdoutWriter);
    let drain = Mutex::new(slog_term::FullFormat::new(decorator).build()).fuse();

    Logger::root(drain, o!())
}

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("raft error: `{0}`")]
    Raft(#[from] raft::Error),
    #[error("io error: `{0}`")]
    Io(#[from] std::io::Error),
    #[error("database error: `{0}`")]
    Database(#[from] persy::PersyError),
    #[error("node init error")]
    InitError,
}

impl<T: Into<persy::PersyError>> From<persy::PE<T>> for Error {
    fn from(err: persy::PE<T>) -> Error {
        Error::Database(err.error().into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct W<T>(T);
