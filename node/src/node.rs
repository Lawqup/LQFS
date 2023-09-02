//! Defines the node struct and provides its APIs
use prost::Message as PRMessage;
use raft::{prelude::*, Config, RawNode, StateRole};

use std::{
    collections::VecDeque,
    fs,
    path::Path,
    sync::mpsc::TryRecvError,
    time::{Duration, Instant},
};

use crate::{
    frag::{FSManager, Fragment},
    network::{Network, QueryMsg, RequestMsg, Response, ResponseMsg, Signal},
    prelude::*,
    storage::{LogStore, NodeStorage},
};

pub struct Node {
    pub(crate) raft: Option<RawNode<NodeStorage>>,
    fs: FSManager,
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

    pub fn try_restore(id: u64, network: Network, logger: &Logger) -> Result<Self> {
        let logger = logger.new(o!("tag" => format!("node_{id}")));

        info!(logger, "RESTORING");

        let path = &format!("store/node-{id}/");
        let path = Path::new(path);

        path.exists().then_some(()).ok_or(Error::InitError)?;
        let storage = NodeStorage::restore(id).expect("Could not restore node storage");

        let mut config = Self::config();
        config.id = id;
        config.applied = storage.first_index()?.saturating_sub(1);
        config.validate().expect("Raft config should be valid");

        let raft = Some(RawNode::new(&config, storage, &logger).unwrap());

        Ok(Self {
            raft,
            network,
            fs: FSManager::new(id),
            followers_to_add: VecDeque::new(),
            id,
            logger,
        })
    }

    pub fn new_leader(id: u64, network: Network, logger: &Logger) -> Self {
        let mut config = Self::config();
        config.id = id;
        config.validate().expect("Raft config should be valid");

        let logger = logger.new(o!("tag" => format!("node_{id}")));

        let mut s = Snapshot::default();
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![id];

        fs::create_dir(format!("store/node-{id}/")).expect("Could not create storage directory");
        let storage = NodeStorage::create(id).expect("Could not create node storage");
        storage.apply_snapshot(s).unwrap();

        let mut raft = Some(RawNode::new(&config, storage, &logger).unwrap());

        let mut followers_to_add = VecDeque::new();

        for peer in network.lock().unwrap().peers() {
            if peer != id {
                followers_to_add.push_front(peer);
            }
        }

        raft.as_mut().unwrap().raft.become_candidate();
        raft.as_mut().unwrap().raft.become_leader();
        Self {
            raft,
            fs: FSManager::new(id),
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
            fs: FSManager::new(id),
            network,
            id,
            followers_to_add: VecDeque::new(),
            logger,
        }
    }

    pub fn init_from_message(&mut self, msg: &Message) -> Result<()> {
        match MessageType::from_i32(msg.msg_type.into()).unwrap() {
            MessageType::MsgRequestVote | MessageType::MsgRequestPreVote => {}
            MessageType::MsgHeartbeat if msg.commit == 0 => {}
            _ => return Err(Error::InitError),
        }

        let mut config = Self::config();
        config.id = msg.to;

        fs::create_dir(format!("store/node-{}/", self.id))
            .expect("Could not create storage directory");
        let storage = NodeStorage::create(self.id)?;
        self.raft = Some(RawNode::new(&config, storage, &self.logger)?);
        Ok(())
    }

    fn step(&mut self, msg: Message) {
        if self.raft.is_none() {
            match self.init_from_message(&msg) {
                Ok(()) => {}
                Err(_) => return,
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
                let network = network_clone.lock().unwrap();
                match network.get_node_rx(self.id).try_recv() {
                    Ok(RequestMsg::Raft(m)) => {
                        info!(self.logger, "Recieved raft message {m:?}");
                        self.step(m);
                    }
                    Ok(RequestMsg::Propose(p)) => {
                        if self.is_leader() {
                            info!(self.logger, "Recieved proposal from {}", p.from);
                            self.propose(&p);
                        }
                    }
                    Ok(RequestMsg::Query(q)) => match q {
                        QueryMsg::IsInitialized { from } => {
                            network.respond_to_client(Response {
                                to: from,
                                from: self.id,
                                msg: ResponseMsg::Initialized(self.raft.is_some()),
                            });
                        }
                        QueryMsg::ReadFrags { from, file_name } => {
                            debug!(self.logger, "GOT READ FRAGS QUERY FROM {from}");
                            network.respond_to_client(Response {
                                to: from,
                                from: self.id,
                                msg: ResponseMsg::Frags(self.fs.get_frags(&file_name)),
                            });
                        }
                    },
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
                raft.tick();

                if let Some(follower) = self.followers_to_add.get(0) {
                    let conf_change = ConfChange {
                        node_id: *follower,
                        change_type: ConfChangeType::AddNode.into(),
                        ..Default::default()
                    };

                    let prop = Proposal::new_conf_change(self.id, conf_change);
                    self.network.lock().unwrap().raft_senders[&self.id]
                        .send(RequestMsg::Propose(prop))
                        .unwrap();
                }
            }

            self.on_ready();
        }
    }

    fn is_leader(&self) -> bool {
        self.raft
            .as_ref()
            .is_some_and(|r| r.raft.state == StateRole::Leader)
    }

    fn propose(&mut self, prop: &Proposal) {
        let index_before = self.raft_mut().raft.raft_log.last_index();
        let ctx = prop.context_bytes();
        if let Some(frag) = &prop.fragment {
            let bytes = frag.encode_to_vec();
            let _ = self.raft_mut().propose(ctx, bytes);
        } else if let Some(cc) = &prop.conf_change {
            let _ = self.raft_mut().propose_conf_change(ctx, cc.clone());
        }

        if index_before == self.raft_mut().raft.raft_log.last_index() {
            // Log didn't grow, so proposal failed
            if prop.is_fragment() {
                // Fragment proposals come from client, so respond over network
                let resp = Response {
                    to: prop.from,
                    from: self.id,
                    msg: ResponseMsg::Proposed {
                        proposal_id: prop.id,
                        success: false,
                    },
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

        if !ready.entries().is_empty() {
            store.append(ready.entries()).unwrap();
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            store
                .set_hard_state(hs)
                .expect("Could not update hard state");
        }

        if !ready.messages().is_empty() {
            self.network
                .lock()
                .unwrap()
                .send_raft_messages(ready.take_messages());
        }

        if !ready.snapshot().is_empty() {
            let s = ready.snapshot().clone();
            store.apply_snapshot(s).expect("Could not apply snapshot");
        }

        self.handle_commited_entries(ready.take_committed_entries());

        if !ready.persisted_messages().is_empty() {
            self.network
                .lock()
                .unwrap()
                .send_raft_messages(ready.take_persisted_messages());
        }

        let mut light_rd = self.raft_mut().advance(ready);

        if let Some(commit) = light_rd.commit_index() {
            let mut hs = store.get_hard_state().unwrap();
            hs.set_commit(commit);
            store
                .set_hard_state(&hs)
                .expect("Could not update hard state");
        }

        self.network
            .lock()
            .unwrap()
            .send_raft_messages(light_rd.take_messages());

        self.handle_commited_entries(light_rd.take_committed_entries());

        self.raft_mut().advance_apply();
    }

    pub(crate) fn raft_mut(&mut self) -> &mut RawNode<NodeStorage> {
        self.raft.as_mut().unwrap()
    }

    pub(crate) fn raft(&self) -> &RawNode<NodeStorage> {
        self.raft.as_ref().unwrap()
    }

    fn handle_commited_entries(&mut self, entries: Vec<Entry>) {
        let mut last_apply_index = 0;
        for entry in entries {
            last_apply_index = std::cmp::max(last_apply_index, entry.index);
            if entry.data.is_empty() {
                continue;
            }

            let (prop_id, prop_from) = Proposal::context_from_bytes(entry.get_context());
            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    let fragment = Fragment::decode(entry.get_data()).unwrap();

                    self.fs.apply(fragment).unwrap();
                    info!(self.logger, "Applied fragment proposal from {prop_from}");
                }
                EntryType::EntryConfChange => {
                    info!(self.logger, "Applied ConfChange proposal from {prop_from}");

                    let mut cc = ConfChange::default();
                    cc.merge(entry.data.as_slice()).unwrap();
                    let cs = self.raft_mut().apply_conf_change(&cc).unwrap();
                    self.raft_mut()
                        .store()
                        .set_conf_state(&cs)
                        .expect("Could not update conf state");
                }
                EntryType::EntryConfChangeV2 => todo!(),
            }

            if self.raft_mut().raft.state == StateRole::Leader {
                if entry.get_entry_type() == EntryType::EntryNormal {
                    let resp = Response {
                        to: prop_from,
                        from: self.id,
                        msg: ResponseMsg::Proposed {
                            proposal_id: prop_id,
                            success: true,
                        },
                    };
                    self.network.lock().unwrap().respond_to_client(resp);
                } else {
                    self.followers_to_add.pop_front();
                }
            }
        }

        self.raft_mut().store().compact(last_apply_index).unwrap();
    }
}
