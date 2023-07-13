//! Defines the node struct and provides its APIs
use lqfs_lib::Fragment;
use raft::{
    prelude::{Entry, EntryType, Message},
    storage::MemStorage,
    Config, RawNode,
};
use slog::*;

use std::{
    collections::HashMap,
    sync::mpsc::{channel, RecvTimeoutError},
    thread,
    time::{Duration, Instant},
};

type Callback = Box<dyn Fn() + Send>;
type Callbacks = HashMap<u8, Box<dyn Fn() + Send>>;

pub enum Msg {
    Proposal { id: u8, callback: Callback },
    Raft(Message),
}

pub struct Node {
    raft: RawNode<MemStorage>,
    cbs: Callbacks,
    logger: Logger,
}

impl Node {
    pub fn new() -> Self {
        // Create the configuration for the Raft node.
        let config = Config {
            // The unique ID for the Raft node.
            id: 1,
            // Election tick is for how long the follower may campaign again after
            // it doesn't receive any message from the leader.
            election_tick: 10,
            // Heartbeat tick is for how long the leader needs to send
            // a heartbeat to keep alive.
            heartbeat_tick: 3,
            // The max size limits the max size of each appended message. Mostly, 1 MB is enough.
            max_size_per_msg: 1024 * 1024 * 1024,
            // Max inflight msgs that the leader sends messages to follower without
            // receiving ACKs.
            max_inflight_msgs: 256,
            // The Raft applied index.
            // You need to save your applied index when you apply the committed Raft logs.
            applied: 0,
            ..Default::default()
        };

        config.validate().expect("Raft config should be valid");

        let storage = MemStorage::new_with_conf_state((vec![1], vec![]));

        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build();
        let drain = std::sync::Mutex::new(drain).fuse();

        let logger = slog::Logger::root(drain, o!());

        let raft = RawNode::new(&config, storage, &logger).unwrap();

        Self {
            raft,
            cbs: HashMap::new(),
            logger,
        }
    }

    pub fn recv_fragment(frag: Fragment) {
        todo!()
    }

    pub fn run(&mut self) {
        // We're using a channel, but this could be any stream of events.
        let (tx, rx) = channel();
        let timeout = Duration::from_millis(100);
        let mut remaining_timeout = timeout;

        let logger = self.logger.clone();
        thread::spawn(move || {
            for i in 0..10 {
                thread::sleep(Duration::from_millis(1500));
                info!(logger, "Proposing {i}!");
                // Send the `tx` somewhere else...

                let logger = logger.clone();
                tx.send(Msg::Proposal {
                    id: i,
                    callback: Box::new(move || info!(logger, "cb {i} called!")),
                })
                .unwrap();
            }
        });

        loop {
            let now = Instant::now();

            match rx.recv_timeout(remaining_timeout) {
                Ok(Msg::Proposal { id, callback }) => {
                    self.cbs.insert(id, callback);
                    let _ = self.raft.propose(vec![], vec![id]);
                }
                Ok(Msg::Raft(m)) => self.raft.step(m).unwrap(),
                Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => (),
            }

            let elapsed = now.elapsed();
            if elapsed >= remaining_timeout {
                remaining_timeout = timeout;
                // We drive Raft every 100ms.
                self.raft.tick();
                info!(self.logger, "tick");
            } else {
                remaining_timeout -= elapsed;
            }

            self.on_ready();
        }
    }

    fn on_ready(&mut self) {
        if !self.raft.has_ready() {
            return;
        }

        let mut ready = self.raft.ready();

        if !ready.messages().is_empty() {
            self.handle_messages(ready.take_messages());
        }

        if !ready.snapshot().is_empty() {
            // Apply most up to date snapshop from leader
            self.raft
                .mut_store()
                .wl()
                .apply_snapshot(ready.snapshot().clone())
                .unwrap();
        }

        self.handle_commited_entries(ready.take_committed_entries());

        if !ready.entries().is_empty() {
            self.raft.mut_store().wl().append(ready.entries()).unwrap();
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            self.raft.mut_store().wl().set_hardstate(hs.clone());
        }

        let mut light_rd = self.raft.advance(ready);

        self.handle_commited_entries(light_rd.take_committed_entries());
        self.handle_messages(light_rd.take_messages());

        self.raft.advance_apply();
    }

    fn handle_commited_entries(&mut self, entries: Vec<Entry>) {
        for entry in entries {
            if entry.data.is_empty() {
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    if let Some(cb) = self.cbs.remove(entry.data.first().unwrap()) {
                        cb();
                    }
                }
                EntryType::EntryConfChange => todo!(),
                EntryType::EntryConfChangeV2 => todo!(),
            }
        }
    }

    fn handle_messages(&self, msgs: Vec<Message>) {
        for msg in msgs {
            // TODO: send messages to other peers
            info!(self.logger, "{msg:?}");
        }
    }
}
