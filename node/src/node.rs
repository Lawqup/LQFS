//! Defines the node struct and provides its APIs
use raft::{
    prelude::{Entry, EntryType},
    storage::MemStorage,
    Config, RawNode, StateRole,
};

use std::{
    sync::mpsc::TryRecvError,
    time::{Duration, Instant},
};

use crate::{
    network::{ControlMsg, Network, NetworkMsg},
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
                    Ok(NetworkMsg::Raft(m)) => self.raft.step(m).unwrap(),
                    Ok(NetworkMsg::Control(ctl)) => match ctl {
                        ControlMsg::Shutdown => {
                            info!(self.logger, "Shutting down node {}", self.id);
                            return;
                        }
                    },
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }

            if self.raft.raft.state == StateRole::Leader {
                let network = self.network.lock().unwrap();
                for prop in network.proposals().values() {
                    Self::propose(&mut self.raft, prop);
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

    fn propose(raft: &mut RawNode<MemStorage>, prop: &Proposal) {
        let n_entries = raft.raft.raft_log.last_index();
        let _ = raft.propose(vec![], prop.id.to_le_bytes().to_vec());

        if n_entries == raft.raft.raft_log.last_index() {
            // Log didn't grow, so proposal failed
            prop.status_success.send(false).unwrap();
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
                .send_raft_messages(ready.take_messages());
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
            .send_raft_messages(light_rd.take_messages());

        self.raft.advance_apply();
    }

    fn handle_commited_entries(&mut self, entries: Vec<Entry>) {
        for entry in entries {
            if entry.data.is_empty() {
                continue;
            }

            let prop_id = u64::from_le_bytes(
                entry.data[0..8]
                    .try_into()
                    .expect("Entry data is invalid (incorrect number of bytes)"),
            );

            let prop = self.network.lock().unwrap().remove_proposal(prop_id);

            match entry.get_entry_type() {
                EntryType::EntryNormal => info!(self.logger, "Applied proposal {prop_id}"),
                EntryType::EntryConfChange => todo!(),
                EntryType::EntryConfChangeV2 => todo!(),
            }

            if self.raft.raft.state == StateRole::Leader {
                prop.status_success.send(true).unwrap();
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::{Arc, Mutex},
        thread,
    };

    use ntest::{test_case, timeout};

    use crate::{
        network::{ControlMsg, Network, NetworkController},
        prelude::*,
    };

    use super::Node;

    fn send_proposals(peers: &[u64], logger: Logger, network: Network) {
        let peers = peers.to_vec();
        thread::spawn(move || {
            let n_success = (0..100)
                .filter(|&i| {
                    let (prop, rx) = Proposal::new(i);
                    network.lock().unwrap().proposals().insert(i, prop);

                    let res = rx.recv().unwrap();
                    info!(
                        logger,
                        "Proposal {i} {}",
                        if res { "SUCCESS" } else { "FAILURE" }
                    );
                    res
                })
                .count();

            for id in peers {
                network
                    .lock()
                    .unwrap()
                    .send_control_message(id, ControlMsg::Shutdown);
            }
            assert_eq!(100, n_success);
            assert!(network.lock().unwrap().proposals().is_empty());
        });
    }

    #[test_case(1)]
    #[test_case(5)]
    #[timeout(2000)]
    fn simple_proposals(n_peers: u64) {
        let peers: Vec<u64> = (1..=n_peers).collect();
        let logger = build_debug_logger();
        let network = Arc::new(Mutex::new(NetworkController::new(&peers, logger.clone())));

        let mut handles = Vec::new();
        for &id in peers.iter() {
            let network = network.clone();
            let logger = logger.clone();
            handles.push(thread::spawn(move || {
                let mut node = Node::new(id, network, logger);
                node.run();
            }));
        }

        send_proposals(&peers, logger, network);

        for handle in handles {
            handle.join().expect("Handle could not be joined");
        }
    }
}
