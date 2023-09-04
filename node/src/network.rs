//! Deals with the global state across nodes

use crate::{
    frag::Fragment,
    prelude::*,
    service::{query_server, ReadFragsRequest, ReadFragsResponse},
};
use raft::{prelude::Message, StateRole};
use tonic::Status;
use uuid::Uuid;

use std::{
    collections::HashMap,
    hint,
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    },
    time::{Duration, Instant},
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
    GetRaftState {
        id: Uuid,
        from: u64,
    },
    ReadFrags {
        id: Uuid,
        from: u64,
        file_name: String,
    },
}

#[derive(Debug, Clone)]
pub enum Signal {
    Shutdown,
}

#[derive(Debug, Clone)]
pub enum ResponseMsg {
    IsProposed(bool),
    /// The raft state. If the raft isn't initialized yet, is None.
    RaftState(Option<StateRole>),
    Frags(Vec<Fragment>),
}

#[derive(Debug, Clone)]
pub struct Response {
    pub id: Uuid,
    pub to: u64,
    pub from: u64,
    pub msg: ResponseMsg,
}

/// Manages all global state including which nodes are available
struct NetworkController {
    raft_senders: HashMap<u64, Sender<RequestMsg>>,
    raft_recievers: HashMap<u64, Arc<Mutex<Receiver<RequestMsg>>>>,
    responses: HashMap<Uuid, Response>,
    logger: Logger,
}

impl NetworkController {
    fn new(peers: &[u64], logger: Logger) -> Self {
        let (raft_senders, raft_recievers) = peers
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
            responses: HashMap::new(),
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

    pub fn respond_to_client(&mut self, response: Response) {
        self.responses.insert(response.id, response);
    }
}

#[derive(Clone)]
pub struct Network(Arc<Mutex<NetworkController>>);

impl Network {
    pub fn new(peers: &[u64], logger: Logger) -> Self {
        Network(Arc::new(Mutex::new(NetworkController::new(peers, logger))))
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

    pub fn wait_for_response(&self, response_id: Uuid) -> Response {
        while !self.0.lock().unwrap().responses.contains_key(&response_id) {
            hint::spin_loop();
        }

        self.0
            .lock()
            .unwrap()
            .responses
            .remove(&response_id)
            .unwrap()
    }

    pub fn wait_for_response_timeout(
        &self,
        response_id: Uuid,
        timeout: Duration,
    ) -> Option<Response> {
        let now = Instant::now();
        while now.elapsed() < timeout
            && !self.0.lock().unwrap().responses.contains_key(&response_id)
        {
            hint::spin_loop();
        }

        self.0.lock().unwrap().responses.remove(&response_id)
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
            id: Uuid::new_v4(),
            from: inner.from,
            file_name: inner.file_name,
        };

        for peer in self.peers() {
            self.send_query_message(peer, query.clone());
        }

        todo!();
    }
}
