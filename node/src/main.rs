#![allow(dead_code)]

use std::thread;

use cluster::InitResult;
use network::{Network, Request, RequestMsg};
use prelude::*;
use tonic::transport::Server;

mod cluster;
mod frag;
mod network;
mod node;
mod prelude;
mod storage;

#[allow(non_snake_case)]
pub mod services {
    tonic::include_proto!("services");
}

pub mod messages {
    tonic::include_proto!("messages");
}

const N_PEERS: u64 = 3;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let peers: Vec<u64> = (1..=N_PEERS).collect();
    let logger = build_default_logger();

    let network = Network::new(&peers, logger.clone());

    // let query_service = QueryServer::new(network);
    // let _server_handle = thread::spawn(move || async {
    //     let addr = "[::1]:50051".parse().unwrap();
    //     Server::builder()
    //         .add_service(query_service)
    //         .serve(addr)
    //         .await
    //         .unwrap();
    // });

    let InitResult {
        network,
        node_handles,
        ..
    } = cluster::try_restore_cluster(&peers, &logger)
        .unwrap_or_else(|_| cluster::init_cluster(&peers, &logger));

    for peer in peers {
        network.request_to_node(
            peer,
            Request {
                id: Uuid::new_v4(),
                req: RequestMsg::ShutdownNode(ShutdownNodeRequest { to_node: peer }),
            },
        )
    }

    for handle in node_handles {
        handle.join().expect("handle could not be joined");
    }

    // server_handle.join().unwrap().await;
    Ok(())
}
