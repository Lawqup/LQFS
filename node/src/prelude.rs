use std::sync::Mutex;

pub use crate::messages::*;
pub use slog::{debug, error, info, o, Drain, Logger};
use thiserror::Error as ThisError;
pub use uuid::Uuid;

pub fn build_default_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = Mutex::new(
        slog_term::FullFormat::new(decorator)
            .build()
            .filter_level(slog::Level::Info),
    )
    .fuse();

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
    Database(#[from] sled::Error),
    #[error("decode error: `{0}`")]
    DecodeError(#[from] prost::DecodeError),
    #[error("encode error: `{0}`")]
    EncodeError(#[from] prost::EncodeError),
    #[error("unkown error: `{0}`")]
    Other(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("conversion error")]
    ConverstionError,
    #[error("node init error")]
    InitError,
    #[error("key error")]
    KeyError,
    #[error("entire file couldn't be retrieved")]
    FileRetrievalError,
}

pub type Result<T> = std::result::Result<T, Error>;
