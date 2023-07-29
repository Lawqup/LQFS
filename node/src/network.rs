//! Deals with the global state across nodes

use crate::prelude::*;
use raft::prelude::Message;
use uuid::Uuid;

use std::{
    collections::HashMap,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

/// RequestMsgs are all messages sent to nodes
pub enum RequestMsg {
    /// Signals that don't require consensus and don't return a response
    Control(Signal),
    /// Queries that don't require consensus and return a response
    Query(QueryMsg),
    /// Proposals that require consensus and return a response
    Propose(Proposal),
    /// Intra-node messages that carry out drive consensus
    Raft(Message),
}

pub enum QueryMsg {
    IsInitialized { from: u64 },
}

pub enum Signal {
    Shutdown,
}

#[derive(PartialEq)]
pub enum ResponseMsg {
    Proposed { proposal_id: Uuid, success: bool },
    Initialized(bool),
}

pub struct Response {
    pub to: u64,
    pub from: u64,
    pub msg: ResponseMsg,
}

/// Manages all global state including which nodes are available
pub struct NetworkController {
    pub raft_senders: HashMap<u64, Sender<RequestMsg>>,
    raft_recievers: HashMap<u64, Receiver<RequestMsg>>,
    client_responders: HashMap<u64, Sender<Response>>,
    logger: Logger,
}

pub type Network = Arc<Mutex<NetworkController>>;

impl NetworkController {
    pub fn new(
        peers: &[u64],
        clients: &[u64],
        logger: Logger,
    ) -> (Self, HashMap<u64, Receiver<Response>>) {
        let (raft_senders, raft_recievers) = peers
            .iter()
            .copied()
            .map(|id| {
                let (tx, rx) = mpsc::channel();
                ((id, tx), (id, rx))
            })
            .unzip();

        let (client_senders, client_recievers) = clients
            .iter()
            .copied()
            .map(|id| {
                let (tx, rx) = mpsc::channel();
                ((id, tx), (id, rx))
            })
            .unzip();

        (
            Self {
                raft_senders,
                raft_recievers,
                client_responders: client_senders,
                logger,
            },
            client_recievers,
        )
    }

    pub fn send_control_message(&self, to: u64, msg: Signal) {
        if self.raft_senders[&to]
            .send(RequestMsg::Control(msg))
            .is_err()
        {
            error!(self.logger, "Failed to send control message to {to}");
        }
    }

    pub fn send_query_message(&self, to: u64, msg: QueryMsg) {
        if self.raft_senders[&to].send(RequestMsg::Query(msg)).is_err() {
            error!(self.logger, "Failed to send query message to {to}");
        }
    }

    pub fn send_raft_messages(&self, msgs: Vec<Message>) {
        for msg in msgs {
            let to = msg.to;
            let from = msg.from;

            if self.raft_senders[&to].send(RequestMsg::Raft(msg)).is_err() {
                error!(
                    self.logger,
                    "Failed to send raft message from {from} to {to}"
                );
            }
        }
    }

    pub fn listen_for_proposals(&mut self) {
        let logger = self.logger.clone();
        let raft_senders = self.raft_senders.clone();
        let clients: Vec<u64> = self.client_responders.keys().copied().collect();
        thread::spawn(move || {
            // TODO set up an endpoint and listen to client

            for (i, c) in (0..10).zip(clients.into_iter().cycle()) {
                thread::sleep(Duration::from_millis(1500));

                info!(logger, "Proposing {i}!");
                let prop = &Proposal::new_fragment(c, vec![]);

                for tx in raft_senders.values() {
                    tx.send(RequestMsg::Propose(prop.clone())).unwrap();
                }
            }
        });
    }

    pub fn get_node_rx(&self, node_id: u64) -> &Receiver<RequestMsg> {
        &self.raft_recievers[&node_id]
    }

    pub fn respond_to_client(&self, response: Response) {
        self.client_responders[&response.to].send(response).unwrap();
    }

    pub fn add_client(&mut self, client_id: u64) -> Receiver<Response> {
        let (tx, rx) = mpsc::channel();
        self.client_responders.insert(client_id, tx);
        rx
    }

    pub fn peers(&self) -> Vec<u64> {
        self.raft_senders.keys().copied().collect()
    }
}
