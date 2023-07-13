use std::sync::{
    mpsc::{self, Receiver, Sender},
    Mutex,
};

pub use slog::*;

pub struct Proposal {
    pub id: u64,
    pub status_success: Sender<bool>,
}

impl Proposal {
    pub fn new(id: u64) -> (Self, Receiver<bool>) {
        let (status_success, rx) = mpsc::channel();

        (Self { id, status_success }, rx)
    }
}
pub fn build_default_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = Mutex::new(slog_term::FullFormat::new(decorator).build()).fuse();

    slog::Logger::root(drain, o!())
}

pub fn build_debug_logger() -> Logger {
    let decorator = slog_term::PlainDecorator::new(slog_term::TestStdoutWriter);
    let drain = Mutex::new(slog_term::FullFormat::new(decorator).build()).fuse();

    slog::Logger::root(drain, o!())
}
