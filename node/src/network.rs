//! Deals with the global state across nodes

use crate::{
    frag::Fragment,
    prelude::*,
    service::{query_server, ReadFragsRequest, ReadFragsResponse},
};
use raft::prelude::Message;
use tonic::Status;
use uuid::Uuid;

use std::{collections::HashMap, sync::Arc};

use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex,
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
pub struct NetworkController {
    pub raft_senders: HashMap<u64, Sender<RequestMsg>>,
    raft_recievers: HashMap<u64, Receiver<RequestMsg>>,
    client_senders: HashMap<u64, Sender<Response>>,
    client_recievers: HashMap<u64, Receiver<Response>>,
    logger: Logger,
}

impl NetworkController {
    pub fn new(peers: &[u64], clients: &[u64], logger: Logger) -> Self {
        let (raft_senders, raft_recievers) = peers
            .iter()
            .copied()
            .map(|id| {
                let (s, r) = channel(100);
                ((id, s), (id, r))
            })
            .unzip();

        let (client_senders, client_recievers) = clients
            .iter()
            .copied()
            .map(|id| {
                let (tx, rx) = channel(100);
                ((id, tx), (id, rx))
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

    pub async fn send_control_message(&self, to: u64, msg: Signal) {
        if self.raft_senders[&to]
            .send(RequestMsg::Control(msg))
            .await
            .is_err()
        {
            error!(self.logger, "Failed to send control message to {to}");
        }
    }

    pub async fn send_query_message(&self, to: u64, msg: QueryMsg) {
        if self.raft_senders[&to]
            .send(RequestMsg::Query(msg))
            .await
            .is_err()
        {
            error!(self.logger, "Failed to send query message to {to}");
        }
    }

    pub async fn send_raft_messages(&self, msgs: Vec<Message>) {
        for msg in msgs {
            let to = msg.to;
            let from = msg.from;

            if self.raft_senders[&to]
                .send(RequestMsg::Raft(msg))
                .await
                .is_err()
            {
                error!(
                    self.logger,
                    "Failed to send raft message from {from} to {to}"
                );
            }
        }
    }

    pub fn get_node_reciever(&mut self, node_id: u64) -> &mut Receiver<RequestMsg> {
        self.raft_recievers.get_mut(&node_id).unwrap()
    }

    pub async fn respond_to_client(&self, response: Response) {
        self.client_senders[&response.to]
            .send(response)
            .await
            .unwrap();
    }

    pub async fn add_client(&mut self, client_id: u64) {
        if !self.client_recievers.contains_key(&client_id) {
            let (s, r) = channel(100);
            self.client_senders.insert(client_id, s);
            self.client_recievers.insert(client_id, r);
        }
    }

    pub fn peers(&self) -> Vec<u64> {
        self.raft_senders.keys().copied().collect()
    }

    pub fn get_client_receiver(&mut self, client_id: u64) -> &mut Receiver<Response> {
        self.client_recievers.get_mut(&client_id).unwrap()
    }

    pub fn broadcast(&mut self, prop: Proposal) {}
}

pub type Network = Arc<Mutex<NetworkController>>;

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

        for peer in self.lock().await.peers() {
            self.lock()
                .await
                .send_query_message(peer, query.clone())
                .await;
        }
        todo!()
    }
}
