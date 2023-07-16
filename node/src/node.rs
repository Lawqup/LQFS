//! Defines the node struct and provides its APIs
use raft::{prelude::*, storage::MemStorage, Config, RawNode, StateRole};

use protobuf::Message as PbMessage;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        mpsc::{self, Sender, TryRecvError},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

use crate::{
    network::{Network, RequestMsg, Response, ResponseMsg, Signal},
    prelude::*,
};

pub struct Node {
    raft: Option<RawNode<MemStorage>>,
    network: Network,
    id: u64,
    followers_to_add: VecDeque<u64>,
    logger: Logger,
}

impl Node {
    const TICK_COOLDOWN: Duration = Duration::from_millis(100);

    fn config() -> Config {
        Config {
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
        }
    }

    pub fn new_leader(id: u64, network: Network, logger: &Logger) -> Self {
        // Create the configuration for the Raft node.

        let mut config = Self::config();
        config.id = id;
        config.validate().expect("Raft config should be valid");

        let logger = logger.new(o!("tag" => format!("node_{id}")));
        let mut s = Snapshot::default();
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![1];

        let storage = MemStorage::new();
        storage.wl().apply_snapshot(s).unwrap();

        let mut raft = Some(RawNode::new(&config, storage, &logger).unwrap());

        let mut followers_to_add = VecDeque::new();

        for peer in network.lock().unwrap().peers() {
            if peer != id {
                followers_to_add.push_front(peer);
            }
        }

        raft.as_mut().unwrap().campaign().unwrap();
        Self {
            raft,
            network,
            followers_to_add,
            id,
            logger,
        }
    }
    /// Create an uninitialized raft incapable of starting without
    /// a message from a leader
    pub fn new_follower(id: u64, network: Network, logger: &Logger) -> Self {
        let logger = logger.new(o!("tag" => format!("node_{id}")));
        Self {
            raft: None,
            network,
            id,
            followers_to_add: VecDeque::new(),
            logger,
        }
    }

    pub fn init_from_message(&mut self, msg: &Message) -> Result<(), ()> {
        match msg.msg_type {
            MessageType::MsgRequestVote | MessageType::MsgRequestPreVote => {}
            MessageType::MsgHeartbeat if msg.commit == 0 => {}
            _ => return Err(()),
        }

        let mut config = Self::config();
        config.id = msg.to;
        let storage = MemStorage::new();
        self.raft = Some(RawNode::new(&config, storage, &self.logger).unwrap());
        return Ok(());
    }

    fn step(&mut self, msg: Message) {
        if self.raft.is_none() {
            match self.init_from_message(&msg) {
                Ok(()) => {}
                Err(()) => return,
            }
        }

        self.raft_mut().step(msg).unwrap();
    }

    pub fn run(&mut self) {
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
                    Ok(RequestMsg::Raft(m)) => {
                        self.step(m);
                    }
                    // TODO: how are followers going to get the conf change
                    // if they need to be leader to accept proposals?
                    Ok(RequestMsg::Propose(p)) => {
                        if self
                            .raft
                            .as_ref()
                            .is_some_and(|r| r.raft.state == StateRole::Leader)
                        {
                            info!(self.logger, "Recieved proposal from {}", p.from);
                            self.propose(&p);
                        }
                    }
                    Ok(RequestMsg::Control(ctl)) => match ctl {
                        Signal::Shutdown => {
                            info!(self.logger, "Shutting down");
                            return;
                        }
                    },
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }

            let raft = match &mut self.raft {
                Some(raft) => raft,
                None => continue, // Raft isn't initialized yet
            };

            let elapsed = now.elapsed();
            if elapsed >= Self::TICK_COOLDOWN {
                now = Instant::now();
                info!(self.logger, "tick",);
                raft.tick();

                if let Some(follower) = self.followers_to_add.get(0) {
                    let mut conf_change = ConfChange::default();
                    conf_change.node_id = *follower;
                    conf_change.set_change_type(ConfChangeType::AddNode);

                    let prop = Proposal::new_conf_change(self.id, conf_change);
                    self.network.lock().unwrap().raft_senders[&self.id]
                        .send(RequestMsg::Propose(prop))
                        .unwrap();
                }
            }

            self.on_ready();
        }
    }

    fn propose(&mut self, prop: &Proposal) {
        let index_before = self.raft_mut().raft.raft_log.last_index();
        let ctx = prop.context_bytes();
        if let Some(frag) = &prop.fragment {
            let _ = self.raft_mut().propose(ctx, frag.to_vec());
        } else if let Some(cc) = &prop.conf_change {
            let _ = self.raft_mut().propose_conf_change(ctx, cc.clone());
        }

        if index_before == self.raft_mut().raft.raft_log.last_index() {
            // Log didn't grow, so proposal failed
            if prop.is_fragment() {
                // Fragment proposals come from client, so respond over network
                let resp = Response {
                    proposal_id: prop.id,
                    to: prop.from,
                    from: self.id,
                    msg: ResponseMsg::ProposalFailure,
                };
                self.network.lock().unwrap().respond_to_client(resp);
            }
        }
    }

    fn on_ready(&mut self) {
        let raft = self.raft_mut();

        if !raft.has_ready() {
            return;
        }

        let mut ready = raft.ready();
        let store = raft.store().clone();

        if !ready.messages().is_empty() {
            self.network
                .lock()
                .unwrap()
                .send_raft_messages(ready.take_messages());
        }

        if *ready.snapshot() != Snapshot::default() {
            let s = ready.snapshot().clone();
            if let Err(e) = store.wl().apply_snapshot(s) {
                error!(
                    self.logger,
                    "Apply snapshot fail: {:?}, need to retry or panic", e
                );
                return;
            }
        }

        self.handle_commited_entries(ready.take_committed_entries());

        if !ready.entries().is_empty() {
            store.wl().append(ready.entries()).unwrap();
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            store.wl().set_hardstate(hs.clone());
        }

        if !ready.persisted_messages().is_empty() {
            self.network
                .lock()
                .unwrap()
                .send_raft_messages(ready.take_persisted_messages());
        }

        let mut light_rd = self.raft_mut().advance(ready);

        if let Some(commit) = light_rd.commit_index() {
            store.wl().mut_hard_state().set_commit(commit);
        }

        self.network
            .lock()
            .unwrap()
            .send_raft_messages(light_rd.take_messages());

        self.handle_commited_entries(light_rd.take_committed_entries());

        self.raft_mut().advance_apply();
    }

    fn raft_mut(&mut self) -> &mut RawNode<MemStorage> {
        self.raft.as_mut().unwrap()
    }

    fn raft(&self) -> &RawNode<MemStorage> {
        self.raft.as_ref().unwrap()
    }

    fn handle_commited_entries(&mut self, entries: Vec<Entry>) {
        for entry in entries {
            if entry.data.is_empty() {
                continue;
            }

            let (prop_id, prop_from) = Proposal::context_from_bytes(entry.get_context());
            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    let fragment = entry.get_data().to_vec();

                    info!(
                        self.logger,
                        "Applied fragment proposal from {prop_from} with data {:?}", fragment
                    );
                }
                EntryType::EntryConfChange => {
                    info!(self.logger, "Applied ConfChange proposal from {prop_from}");

                    let mut cc = ConfChange::default();
                    cc.merge_from_bytes(&entry.data).unwrap();
                    let cs = self.raft_mut().apply_conf_change(&cc).unwrap();
                    self.raft_mut().store().wl().set_conf_state(cs);
                }
                EntryType::EntryConfChangeV2 => todo!(),
            }

            if self.raft_mut().raft.state == StateRole::Leader {
                if entry.get_entry_type() == EntryType::EntryNormal {
                    let resp = Response {
                        proposal_id: prop_id,
                        to: prop_from,
                        from: self.id,
                        msg: ResponseMsg::ProposalSuccess,
                    };
                    self.network.lock().unwrap().respond_to_client(resp);
                } else {
                    self.followers_to_add.pop_front();
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        sync::{mpsc::Receiver, Arc, Mutex},
        thread::{self, JoinHandle},
        time::Duration,
    };

    use ntest::{test_case, timeout};
    use raft::StateRole;

    use crate::{
        network::{Network, NetworkController, RequestMsg, Response, ResponseMsg, Signal},
        prelude::*,
    };

    use super::Node;

    const CLIENT_TIMEOUT: Duration = Duration::from_millis(5000);

    fn init_nodes(
        peers: &[u64],
        logger: &Logger,
        network: &Network,
    ) -> (Vec<Arc<Mutex<Node>>>, Vec<JoinHandle<()>>) {
        let mut handles = Vec::new();
        let mut nodes = Vec::new();

        for &id in peers.iter().skip(1) {
            let network = network.clone();
            let logger = logger.clone();

            let node = Arc::new(Mutex::new(Node::new_follower(id, network, &logger)));

            let node_clone = node.clone();
            nodes.push(node);
            handles.push(thread::spawn(move || {
                info!(&logger, "Starting follower (id: {id})");
                node_clone.lock().unwrap().run();
            }));
        }

        let leader_id = peers[0];

        let logger_clone = logger.clone();
        let leader = Arc::new(Mutex::new(Node::new_leader(
            leader_id,
            network.clone(),
            &logger,
        )));

        let leader_clone = leader.clone();
        let leader_clone2 = leader.clone();
        nodes.push(leader);
        handles.push(thread::spawn(move || {
            info!(logger_clone, "Starting leader (id: {leader_id})");
            leader_clone.lock().unwrap().run();
        }));

        (nodes, handles)
    }

    fn spawn_client(
        client_id: u64,
        n_proposals: usize,
        client_rx: Mutex<Receiver<Response>>,
        logger: Logger,
        network: Network,
    ) -> JoinHandle<()> {
        let proposals: HashMap<Uuid, Proposal> = (0..n_proposals)
            .map(|i| {
                let prop = Proposal::new_fragment(client_id, i.to_le_bytes().to_vec());
                (prop.id, prop)
            })
            .collect();

        let proposals = Arc::new(Mutex::new(proposals));

        let proposals_clone = proposals.clone();
        let logger_clone = logger.clone();
        thread::spawn(move || {
            while !proposals_clone.lock().unwrap().is_empty() {
                for prop in proposals_clone.lock().unwrap().values() {
                    info!(logger_clone, "CLIENT proposal ({}, {})", prop.id, prop.from);
                    for tx in network.lock().unwrap().raft_senders.values() {
                        tx.send(RequestMsg::Propose(prop.clone())).unwrap();
                    }
                }
                thread::sleep(Duration::from_millis(500));
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
                assert_eq!(res.to, client_id);
                info!(
                    logger,
                    "Proposal ({}, {}) {}",
                    res.proposal_id,
                    res.to,
                    match res.msg {
                        ResponseMsg::ProposalFailure => "FAILURE",
                        ResponseMsg::ProposalSuccess => "SUCCESS",
                    }
                );
            }
        })
    }

    #[test_case(1)]
    #[test_case(3)]
    #[test_case(9)]
    #[timeout(4000)]
    fn leader_election(n_peers: u64) {
        let peers: Vec<u64> = (1..=n_peers).collect();
        let logger = build_debug_logger();

        let (network, _) = NetworkController::new(&peers, &[], logger.clone());
        let network = Arc::new(Mutex::new(network));

        let (nodes, node_handles) = init_nodes(&peers, &logger, &network);

        thread::sleep(Duration::from_millis(3000));

        for id in peers {
            network
                .lock()
                .unwrap()
                .send_control_message(id, Signal::Shutdown);
        }

        let mut n_leaders = 0;
        let mut n_followers = 0;
        for (handle, node) in node_handles.into_iter().zip(nodes.into_iter()) {
            handle.join().expect("Handle could not be joined");

            let node = node.lock().unwrap();
            if node.raft.as_ref().unwrap().raft.state == StateRole::Leader {
                n_leaders += 1;
            } else if node.raft.as_ref().unwrap().raft.state == StateRole::Follower {
                n_followers += 1;
            }
        }

        assert_eq!(n_leaders, 1);
        assert_eq!(n_followers, n_peers - 1);
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

        let (_, node_handles) = init_nodes(&peers, &logger, &network);

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

        for handle in client_handles {
            handle.join().expect("Handle could not be joined");
        }

        for id in peers {
            network
                .lock()
                .unwrap()
                .send_control_message(id, Signal::Shutdown);
        }

        for handle in node_handles {
            handle.join().expect("Handle could not be joined");
        }
    }

    #[test_case(3, 5, name = "three_node_5_client_node_failure")]
    #[test_case(9, 5, name = "nine_node_5_client_node_failure")]
    #[timeout(6000)]
    fn node_failure(n_peers: u64, n_clients: u64) {
        let peers: Vec<u64> = (1..=n_peers).collect();
        let clients: Vec<u64> = (1..=n_clients).collect();
        let logger = build_debug_logger();

        let (network, mut client_rxs) = NetworkController::new(&peers, &clients, logger.clone());
        let network = Arc::new(Mutex::new(network));

        let (_, node_handles) = init_nodes(&peers, &logger, &network);

        thread::sleep(Duration::from_millis(3000));

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

        for handle in node_handles {
            handle.join().expect("Handle could not be joined");
        }
    }
}
