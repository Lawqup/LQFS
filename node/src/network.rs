//! Deals with the global state across nodes

use crate::{
    frag::Fragment,
    prelude::*,
    service::{query_server, ReadFragsRequest, ReadFragsResponse},
};
use raft::prelude::Message;
use tonic::Status;
use uuid::Uuid;

use std::{
    collections::{hash_map, HashMap},
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    },
};

/// RequestMsgs are all messages sent to nodes
#[derive(Debug, Clone)]
pub enum RequestMsg {
    /// Signals that don't require consensus and don't return a response
    Control(Signal),
    /// Queries that don't require consensus and return a response
    Query(QueryMsg),
    /// Proposals that require consensus and return a response
    Propose(Proposal),
    /// Intra-node messages that drive consensus
    Raft(Message),
}

#[derive(Debug, Clone)]
pub enum QueryMsg {
    IsInitialized { from: u64 },
    ReadFrags { from: u64, file_name: String },
}

#[derive(Debug, Clone)]
pub enum Signal {
    Shutdown,
}

#[derive(Debug, Clone)]
pub enum ResponseMsg {
    Proposed { proposal_id: Uuid, success: bool },
    Initialized(bool),
    Frags(Vec<Fragment>),
}

#[derive(Debug, Clone)]
pub struct Response {
    pub to: u64,
    pub from: u64,
    pub msg: ResponseMsg,
}

/// Manages all global state including which nodes are available
struct NetworkController {
    raft_senders: HashMap<u64, Sender<RequestMsg>>,
    raft_recievers: HashMap<u64, Arc<Mutex<Receiver<RequestMsg>>>>,
    client_senders: HashMap<u64, Sender<Response>>,
    client_recievers: HashMap<u64, Arc<Mutex<Receiver<Response>>>>,
    logger: Logger,
}

impl NetworkController {
    fn new(peers: &[u64], clients: &[u64], logger: Logger) -> Self {
        let (raft_senders, raft_recievers) = peers
            .iter()
            .copied()
            .map(|id| {
                let (s, r) = channel();
                ((id, s), (id, Arc::new(Mutex::new(r))))
            })
            .unzip();

        let (client_senders, client_recievers) = clients
            .iter()
            .copied()
            .map(|id| {
                let (s, r) = channel();
                ((id, s), (id, Arc::new(Mutex::new(r))))
            })
            .unzip();

        Self {
            raft_senders,
            raft_recievers,
            client_senders,
            client_recievers,
            logger,
        }
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

    pub fn send_proposal_message(&self, to: u64, msg: Proposal) {
        if self.raft_senders[&to]
            .send(RequestMsg::Propose(msg))
            .is_err()
        {
            error!(self.logger, "Failed to send proposal message to {to}");
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

    pub fn respond_to_client(&self, response: Response) {
        self.client_senders[&response.to].send(response).unwrap();
    }

    pub fn add_client(&mut self, client_id: u64) {
        if let hash_map::Entry::Vacant(e) = self.client_recievers.entry(client_id) {
            let (s, r) = channel();
            self.client_senders.insert(client_id, s);
            e.insert(Arc::new(Mutex::new(r)));
        }
    }
}

#[derive(Clone)]
pub struct Network(Arc<Mutex<NetworkController>>);

impl Network {
    pub fn new(peers: &[u64], clients: &[u64], logger: Logger) -> Self {
        Network(Arc::new(Mutex::new(NetworkController::new(
            peers, clients, logger,
        ))))
    }

    pub fn send_control_message(&self, to: u64, msg: Signal) {
        self.0.lock().unwrap().send_control_message(to, msg);
    }

    pub fn send_query_message(&self, to: u64, msg: QueryMsg) {
        self.0.lock().unwrap().send_query_message(to, msg);
    }

    pub fn send_proposal_message(&self, to: u64, msg: Proposal) {
        self.0.lock().unwrap().send_proposal_message(to, msg);
    }

    pub fn send_raft_messages(&self, msgs: Vec<Message>) {
        self.0.lock().unwrap().send_raft_messages(msgs);
    }

    pub fn respond_to_client(&self, response: Response) {
        self.0.lock().unwrap().respond_to_client(response);
    }

    pub fn add_client(&self, client_id: u64) {
        self.0.lock().unwrap().add_client(client_id);
    }

    pub fn peers(&self) -> Vec<u64> {
        self.0
            .lock()
            .unwrap()
            .raft_senders
            .keys()
            .copied()
            .collect()
    }

    pub fn get_node_reciever(&self, node_id: u64) -> Arc<Mutex<Receiver<RequestMsg>>> {
        self.0.lock().unwrap().raft_recievers[&node_id].clone()
    }

    pub fn get_client_receiver(&self, client_id: u64) -> Arc<Mutex<Receiver<Response>>> {
        self.0.lock().unwrap().client_recievers[&client_id].clone()
    }
}

#[tonic::async_trait]
impl query_server::Query for Network {
    async fn read_frags(
        &self,
        request: tonic::Request<ReadFragsRequest>,
    ) -> std::result::Result<tonic::Response<ReadFragsResponse>, Status> {
        let inner = request.into_inner();
        let query = QueryMsg::ReadFrags {
            from: inner.from,
            file_name: inner.file_name,
        };

        for peer in self.peers() {
            self.send_query_message(peer, query.clone());
        }

        todo!()
    }
}
