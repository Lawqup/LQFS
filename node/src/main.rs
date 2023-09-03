#![allow(dead_code)]

use std::{
    sync::{Arc, Mutex},
    thread,
};

use cluster::InitResult;
use network::{NetworkController, Signal};
use prelude::*;
use service::query_server::QueryServer;
use tonic::transport::Server;

mod cluster;
mod frag;
mod network;
mod node;
mod prelude;
mod storage;

#[allow(non_snake_case)]
pub mod service {
    tonic::include_proto!("service");
}

const N_PEERS: u64 = 3;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let peers: Vec<u64> = (1..=N_PEERS).collect();
    let logger = build_default_logger();

    let network = Arc::new(Mutex::new(NetworkController::new(
        &peers,
        &[],
        logger.clone(),
    )));

    let query_service = QueryServer::new(network);
    let server_handle = thread::spawn(move || async {
        let addr = "[::1]:50051".parse().unwrap();
        Server::builder()
            .add_service(query_service)
            .serve(addr)
            .await
            .unwrap();
    });

    let InitResult {
        network,
        node_handles,
        ..
    } = cluster::try_restore_cluster(&peers, &[], &logger)
        .unwrap_or_else(|_| cluster::init_cluster(&peers, &[], &logger));

    for id in peers {
        network
            .lock()
            .unwrap()
            .send_control_message(id, Signal::Shutdown);
    }

    for handle in node_handles {
        handle.join().expect("handle could not be joined");
    }

    server_handle.join().unwrap();
    Ok(())
}
