//! Defines the node struct and provides its APIs
use raft::{
    prelude::{Entry, EntryType, Message},
    storage::MemStorage,
    Config, RawNode, StateRole,
};

use std::{
    sync::{
        mpsc::{channel, RecvTimeoutError, TryRecvError},
        MutexGuard,
    },
    time::{Duration, Instant},
};

use crate::{
    network::{Network, NetworkController},
    prelude::*,
};

pub struct Node {
    raft: RawNode<MemStorage>,
    id: u64,
    network: Network,
    logger: Logger,
}

impl Node {
    pub fn new(id: u64, network: Network, logger: Logger) -> Self {
        // Create the configuration for the Raft node.
        let config = Config {
            // The unique ID for the Raft node.
            id,
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

        let raft = RawNode::new(&config, storage, &logger).unwrap();

        Self {
            raft,
            id,
            network,
            logger,
        }
    }

    pub fn run(&mut self) {
        // We're using a channel, but this could be any stream of events.
        let tick_cooldown = Duration::from_millis(100);

        let mut now = Instant::now();
        loop {
            // Keep recieving until there's nothing ready yet,
            // in which case stop recieving to drive the raft
            loop {
                match self.network.lock().unwrap().get_node_rx(self.id).try_recv() {
                    Ok(m) => self.raft.step(m).unwrap(),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }

            if self.raft.raft.state == StateRole::Leader {
                let network = self.network.lock().unwrap();
                for prop_id in network.proposals().keys() {
                    let _ = self.raft.propose(vec![], prop_id.to_le_bytes().to_vec());
                }
            }

            let elapsed = now.elapsed();
            if elapsed >= tick_cooldown {
                now = Instant::now();
                // We drive Raft every 100ms.
                self.raft.tick();
                info!(self.logger, "tick");
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
            self.network
                .lock()
                .unwrap()
                .send_messages(ready.take_messages());
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
        self.network
            .lock()
            .unwrap()
            .send_messages(light_rd.take_messages());

        self.raft.advance_apply();
    }

    fn handle_commited_entries(&mut self, entries: Vec<Entry>) {
        for entry in entries {
            if entry.data.is_empty() {
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    let prop_id = u64::from_le_bytes(
                        entry.data[0..8]
                            .try_into()
                            .expect("Entry data is invalid (incorrect number of bytes)"),
                    );
                    self.network.lock().unwrap().apply_proposal(prop_id);
                }
                EntryType::EntryConfChange => todo!(),
                EntryType::EntryConfChangeV2 => todo!(),
            }
        }
    }
}
