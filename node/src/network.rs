//! Deals with the global state across nodes

use crate::prelude::*;
use raft::prelude::Message;

use std::{
    collections::HashMap,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc, Mutex, MutexGuard,
    },
    thread,
    time::Duration,
};

pub enum NetworkMsg {
    Control(ControlMsg),
    Raft(Message),
}

pub enum ControlMsg {
    Shutdown,
}

/// Manages all global state including which nodes are available
pub struct NetworkController {
    raft_senders: HashMap<u64, Sender<NetworkMsg>>,
    raft_recievers: HashMap<u64, Receiver<NetworkMsg>>,
    pub proposals: Arc<Mutex<HashMap<u64, Proposal>>>,
    logger: Logger,
}

pub type Network = Arc<Mutex<NetworkController>>;

impl NetworkController {
    pub fn new(peers: &[u64], logger: Logger) -> Self {
        let (raft_senders, raft_recievers) = peers
            .iter()
            .copied()
            .map(|id| {
                let (tx, rx) = mpsc::channel();
                ((id, tx), (id, rx))
            })
            .unzip();

        Self {
            raft_senders,
            raft_recievers,
            proposals: Arc::new(Mutex::new(HashMap::new())),
            logger,
        }
    }

    pub fn send_control_message(&self, to: u64, msg: ControlMsg) {
        if self.raft_senders[&to]
            .send(NetworkMsg::Control(msg))
            .is_err()
        {
            error!(self.logger, "Failed to send control message to {to}");
        }
    }

    pub fn send_raft_messages(&self, msgs: Vec<Message>) {
        for msg in msgs {
            let to = msg.to;
            let from = msg.from;
            if self.raft_senders[&to].send(NetworkMsg::Raft(msg)).is_err() {
                error!(
                    self.logger,
                    "Failed to send raft message from {from} to {to}"
                );
            }
        }
    }

    pub fn listen_for_proposals(&mut self) {
        let proposals = self.proposals.clone();
        let logger = self.logger.clone();
        thread::spawn(move || {
            // TODO set up an endpoint and listen to client

            for i in 0..10 {
                thread::sleep(Duration::from_millis(1500));
                info!(logger, "Proposing {i}!");

                let (prop, rx) = Proposal::new(i);
                proposals.lock().unwrap().insert(i, prop);

                info!(
                    logger,
                    "Proposal {i} {}",
                    if rx.recv().unwrap() { "SUCCESS" } else { "FAILURE" }
                );
            }
        });
    }

    pub fn proposals(&self) -> MutexGuard<HashMap<u64, Proposal>> {
        self.proposals.lock().unwrap()
    }

    pub fn remove_proposal(&self, prop_id: u64) -> Proposal {
        self.proposals()
            .remove(&prop_id)
            .expect("Applied proposal should exist")
    }

    pub fn get_node_rx(&self, node_id: u64) -> &Receiver<NetworkMsg> {
        &self.raft_recievers[&node_id]
    }
}
