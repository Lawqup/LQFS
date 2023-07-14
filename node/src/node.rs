//! Defines the node struct and provides its APIs
use raft::{prelude::*, storage::MemStorage, Config, RawNode, StateRole};

use protobuf::Message as PbMessage;
use std::{
    collections::HashMap,
    sync::mpsc::TryRecvError,
    time::{Duration, Instant},
};

use crate::{
    network::{Network, RequestMsg, Response, ResponseMsg, Signal},
    prelude::*,
};

pub struct Node {
    raft: RawNode<MemStorage>,
    id: u64,
    network: Network,
    proposals: HashMap<u64, Proposal>,
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
            proposals: HashMap::new(),
            logger,
        }
    }

    pub fn run(&mut self) {
        // We're using a channel, but this could be any stream of events.
        let tick_cooldown = Duration::from_millis(100);

        let mut now = Instant::now();
        let network_clone = self.network.clone();
        loop {
            // Keep recieving until there's nothing ready yet,
            // in which case stop recieving to drive the raft
            loop {
                match network_clone
                    .lock()
                    .unwrap()
                    .get_node_rx(self.id)
                    .try_recv()
                {
                    Ok(RequestMsg::Raft(m)) => self.raft.step(m).unwrap(),
                    Ok(RequestMsg::Propose(p)) => {
                        self.proposals.insert(p.id, p);
                    }
                    Ok(RequestMsg::Control(ctl)) => match ctl {
                        Signal::Shutdown => {
                            info!(self.logger, "Shutting down node {}", self.id);
                            return;
                        }
                    },
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }

            if self.raft.raft.state == StateRole::Leader {
                for prop in self.proposals.values() {
                    Self::propose(&mut self.raft, &self.network, &prop);
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

    fn propose(raft: &mut RawNode<MemStorage>, network: &Network, prop: &Proposal) {
        let n_entries = raft.raft.raft_log.last_index();
        let _ = raft.propose(vec![], bytemuck::bytes_of(prop).to_vec());

        if n_entries == raft.raft.raft_log.last_index() {
            // Log didn't grow, so proposal failed
            let resp = Response {
                proposal_id: prop.id,
                client_id: prop.client_id,
                msg: ResponseMsg::ProposalFailure,
            };
            network.lock().unwrap().respond_to_client(resp);
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
                .store()
                .wl()
                .apply_snapshot(ready.snapshot().clone())
                .unwrap();
        }

        self.handle_commited_entries(ready.take_committed_entries());

        if !ready.entries().is_empty() {
            self.raft.store().wl().append(ready.entries()).unwrap();
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            self.raft.store().wl().set_hardstate(hs.clone());
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

            let prop: &Proposal = bytemuck::from_bytes(&entry.data);

            match entry.get_entry_type() {
                EntryType::EntryNormal => info!(self.logger, "Applied proposal {}", prop.id),
                EntryType::EntryConfChange => {
                    let mut cc = ConfChange::default();
                    cc.merge_from_bytes(&entry.data).unwrap();
                    let cs = self.raft.apply_conf_change(&cc).unwrap();
                    self.raft.store().wl().set_conf_state(cs);
                }
                EntryType::EntryConfChangeV2 => todo!(),
            }

            self.proposals.remove(&prop.id);
            if self.raft.raft.state == StateRole::Leader {
                let resp = Response {
                    proposal_id: prop.id,
                    client_id: prop.client_id,
                    msg: ResponseMsg::ProposalSuccess,
                };
                self.network.lock().unwrap().respond_to_client(resp);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        panic, process,
        sync::{mpsc::Receiver, Arc, Mutex},
        thread::{self, JoinHandle},
        time::Duration,
    };

    use ntest::{test_case, timeout};

    use crate::{
        network::{Network, NetworkController, RequestMsg, Response, ResponseMsg, Signal},
        prelude::*,
    };

    use super::Node;

    const CLIENT_TIMEOUT: Duration = Duration::from_millis(5000);

    fn spawn_client(
        client_id: u64,
        n_proposals: usize,
        client_rx: Mutex<Receiver<Response>>,
        logger: Logger,
        network: Network,
    ) -> JoinHandle<()> {
        let proposals: HashMap<u64, Proposal> = (0..n_proposals)
            .map(|i| (i as u64, Proposal::new(i as u64, client_id)))
            .collect();

        let proposals = Arc::new(Mutex::new(proposals));

        let proposals_clone = proposals.clone();
        let logger_clone = logger.clone();
        thread::spawn(move || {
            while !proposals_clone.lock().unwrap().is_empty() {
                for prop in proposals_clone.lock().unwrap().values() {
                    info!(
                        logger_clone,
                        "Sending proposal ({}, {})", prop.id, prop.client_id
                    );
                    for tx in network.lock().unwrap().raft_senders.values() {
                        tx.send(RequestMsg::Propose(*prop)).unwrap();
                    }
                }
                thread::sleep(Duration::from_millis(200));
            }
        });

        thread::spawn(move || {
            while !proposals.lock().unwrap().is_empty() {
                let res = client_rx.lock().unwrap().recv_timeout(CLIENT_TIMEOUT);

                assert!(
                    res.is_ok(),
                    "Client {client_id} waited too long for response"
                );

                let res = res.unwrap();
                proposals.lock().unwrap().remove(&res.proposal_id);
                assert_eq!(res.client_id, client_id);
                info!(
                    logger,
                    "Proposal ({}, {}) {}",
                    res.proposal_id,
                    res.client_id,
                    match res.msg {
                        ResponseMsg::ProposalFailure => "FAILURE",
                        ResponseMsg::ProposalSuccess => "SUCCESS",
                    }
                );
            }
        })
    }

    #[test_case(1, 1, name = "single_node_1_client_simple")]
    #[test_case(1, 5, name = "single_node_5_client_simple")]
    #[test_case(5, 5, name = "multi_node_5_client_simple")]
    #[timeout(2000)]
    fn simple(n_peers: u64, n_clients: u64) {
        let peers: Vec<u64> = (1..=n_peers).collect();
        let clients: Vec<u64> = (1..=n_clients).collect();
        let logger = build_debug_logger();

        let (network, mut client_rxs) = NetworkController::new(&peers, &clients, logger.clone());
        let network = Arc::new(Mutex::new(network));

        let mut handles = Vec::new();
        for &id in peers.iter() {
            let network = network.clone();
            let logger = logger.clone();
            handles.push(thread::spawn(move || {
                let mut node = Node::new(id, network, logger.clone());
                node.run();
            }));
        }

        for client_id in clients {
            spawn_client(
                client_id,
                10,
                Mutex::new(client_rxs.remove(&client_id).unwrap()),
                logger.clone(),
                network.clone(),
            );
        }

        network
            .clone()
            .lock()
            .unwrap()
            .send_control_message(1, Signal::Shutdown);

        for id in peers {
            network
                .lock()
                .unwrap()
                .send_control_message(id, Signal::Shutdown);
        }

        for handle in handles {
            handle.join().expect("Handle could not be joined");
        }
    }

    #[test_case(1, 5, name = "single_node_5_client_node_failure")]
    #[test_case(5, 5, name = "multi_node_5_client_node_failure")]
    #[timeout(6000)]
    fn node_failure(n_peers: u64, n_clients: u64) {
        let peers: Vec<u64> = (1..=n_peers).collect();
        let clients: Vec<u64> = (1..=n_clients).collect();
        let logger = build_debug_logger();

        let (network, mut client_rxs) = NetworkController::new(&peers, &clients, logger.clone());
        let network = Arc::new(Mutex::new(network));

        let mut handles = Vec::new();
        for &id in peers.iter() {
            let network = network.clone();
            let logger = logger.clone();
            handles.push(thread::spawn(move || {
                let mut node = Node::new(id, network, logger.clone());
                node.run();
            }));
        }

        thread::sleep(Duration::from_millis(1000));

        let mut client_handles = Vec::new();
        for client_id in clients {
            client_handles.push(spawn_client(
                client_id,
                1,
                Mutex::new(client_rxs.remove(&client_id).unwrap()),
                logger.clone(),
                network.clone(),
            ));
        }

        thread::sleep(Duration::from_millis(200));
        network
            .lock()
            .unwrap()
            .send_control_message(1, Signal::Shutdown);

        for handle in client_handles {
            handle.join().expect("Handle could not be joined");
        }

        for id in peers {
            network
                .lock()
                .unwrap()
                .send_control_message(id, Signal::Shutdown);
        }

        for handle in handles {
            handle.join().expect("Handle could not be joined");
        }
    }
}
