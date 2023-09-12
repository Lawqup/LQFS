//! Deals with the global state across nodes

use crate::{prelude::*, services::file_store_server::FileStore};
use raft::prelude::{ConfChange, Message};
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
    /// Doesn't require consensus, from client
    GetRaftState(RaftStateRequest),
    /// Doesn't require consensus, from client
    ReadFileNames,
    /// Doesn't require consensus, from client
    ReadFrags(ReadFragsRequest),
    /// Doesn't require consensus, from client
    ShutdownNode(ShutdownNodeRequest),
    /// Messages that require consensus
    Propose(Proposal),
    /// Messages that drive consensus, from nodes
    DriveRaft(Message),
}

#[derive(Debug, Clone)]
pub enum Proposal {
    WriteFrag(Fragment),
    ConfChange(ConfChange),
}

#[derive(Debug, Clone)]
pub struct Request {
    pub id: Uuid,
    pub req: RequestMsg,
}

#[derive(Debug, Clone)]
pub enum ResponseMsg {
    /// The raft state. If the raft isn't initialized yet, is None.
    RaftState(RaftStateResponse),
    FileNames(ReadFileNamesResponse),
    Frags(ReadFragsResponse),
    WriteFragSuccess(WriteFragResponse),
}

#[derive(Debug, Clone)]
pub struct Response {
    pub id: Uuid,
    pub res: ResponseMsg,
}

/// Manages all global state including which nodes are available
struct NetworkController {
    raft_senders: HashMap<u64, Sender<Request>>,
    raft_recievers: HashMap<u64, Arc<Mutex<Receiver<Request>>>>,
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

    pub fn request_to_node(&self, to: u64, req: Request) {
        if self.raft_senders[&to].send(req).is_err() {
            error!(self.logger, "Failed to send request message to {to}");
        }
    }

    pub fn request_to_cluster(&self, req: Request) {
        for (to, sender) in self.raft_senders.iter() {
            if sender.send(req.clone()).is_err() {
                error!(self.logger, "Failed to send request message to {to}");
            }
        }
    }

    /// TODO: this should be node's job
    pub fn send_raft_messages(&self, msgs: Vec<Message>) {
        for msg in msgs {
            let to = msg.to;
            let from = msg.from;

            if self.raft_senders[&to]
                .send(Request {
                    id: Uuid::new_v4(),
                    req: RequestMsg::DriveRaft(msg),
                })
                .is_err()
            {
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

    pub fn request_to_node(&self, to: u64, req: Request) {
        self.0.lock().unwrap().request_to_node(to, req);
    }

    pub fn request_to_cluster(&self, req: Request) {
        self.0.lock().unwrap().request_to_cluster(req);
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

    pub fn get_node_reciever(&self, node_id: u64) -> Arc<Mutex<Receiver<Request>>> {
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

const SERVER_TIMEOUT: Duration = Duration::from_millis(5000);

#[tonic::async_trait]
impl FileStore for Network {
    async fn read_frags(
        &self,
        request: tonic::Request<ReadFragsRequest>,
    ) -> std::result::Result<tonic::Response<ReadFragsResponse>, Status> {
        let id = Uuid::new_v4();
        self.request_to_cluster(Request {
            id,
            req: RequestMsg::ReadFrags(request.into_inner()),
        });

        match self
            .wait_for_response_timeout(id, SERVER_TIMEOUT)
            .map(|r| r.res)
        {
            Some(ResponseMsg::Frags(r)) => Ok(tonic::Response::new(r)),
            None => Err(Status::deadline_exceeded(
                "Cluster timed out in executing request; try again later",
            )),
            _ => panic!("Incorrect response variant recieved"),
        }
    }

    async fn read_file_names(
        &self,
        _request: tonic::Request<ReadFileNamesRequest>,
    ) -> std::result::Result<tonic::Response<ReadFileNamesResponse>, Status> {
        let id = Uuid::new_v4();
        self.request_to_cluster(Request {
            id,
            req: RequestMsg::ReadFileNames,
        });

        match self
            .wait_for_response_timeout(id, SERVER_TIMEOUT)
            .map(|r| r.res)
        {
            Some(ResponseMsg::FileNames(r)) => Ok(tonic::Response::new(r)),
            None => Err(Status::deadline_exceeded(
                "Cluster timed out in executing request; try again later",
            )),
            _ => panic!("Incorrect response variant recieved"),
        }
    }

    async fn write_frag(
        &self,
        request: tonic::Request<Fragment>,
    ) -> std::result::Result<tonic::Response<WriteFragResponse>, Status> {
        let id = Uuid::new_v4();
        self.request_to_cluster(Request {
            id,
            req: RequestMsg::Propose(Proposal::WriteFrag(request.into_inner())),
        });

        match self
            .wait_for_response_timeout(id, SERVER_TIMEOUT)
            .map(|r| r.res)
        {
            Some(ResponseMsg::WriteFragSuccess(r)) => Ok(tonic::Response::new(r)),
            None => Err(Status::deadline_exceeded(
                "Cluster timed out in executing request; try again later",
            )),
            _ => panic!("Incorrect response variant recieved"),
        }
    }
}
