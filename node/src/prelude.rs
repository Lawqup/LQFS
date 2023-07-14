use std::sync::Mutex;

pub use slog::*;

#[derive(bytemuck::Pod, bytemuck::Zeroable, Clone, Copy, Debug)]
#[repr(C)]
pub struct Proposal {
    pub id: u64,
    pub client_id: u64,
}

impl Proposal {
    pub fn new(id: u64, client_id: u64) -> Self {
        Self { id, client_id }
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
