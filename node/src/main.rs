#![allow(dead_code)]

use std::{
    future::Future,
    thread::{self, JoinHandle},
    time::Duration,
};

use network::{Network, Request, RequestMsg};
use prelude::{Result, *};
use services::file_store_server::FileStoreServer;
use tonic::{codegen::http::HeaderName, transport::Server};
use tonic_web::GrpcWebLayer;
use tower_http::cors::{AllowOrigin, CorsLayer};

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

#[allow(non_snake_case)]
pub mod messages {
    tonic::include_proto!("messages");
}

const DEFAULT_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_EXPOSED_HEADERS: [&str; 3] =
    ["grpc-status", "grpc-message", "grpc-status-details-bin"];
const DEFAULT_ALLOW_HEADERS: [&str; 4] =
    ["x-grpc-web", "content-type", "x-user-agent", "grpc-timeout"];

fn build_and_start_server(network: Network) -> JoinHandle<impl Future<Output = Result<()>>> {
    let query_service = FileStoreServer::new(network);
    thread::spawn(move || async {
        let addr = "[::1]:50051".parse().unwrap();
        Server::builder()
            .accept_http1(true)
            .layer(
                CorsLayer::new()
                    .allow_origin(AllowOrigin::mirror_request())
                    .allow_credentials(true)
                    .max_age(DEFAULT_MAX_AGE)
                    .expose_headers(
                        DEFAULT_EXPOSED_HEADERS
                            .iter()
                            .cloned()
                            .map(HeaderName::from_static)
                            .collect::<Vec<HeaderName>>(),
                    )
                    .allow_headers(
                        DEFAULT_ALLOW_HEADERS
                            .iter()
                            .cloned()
                            .map(HeaderName::from_static)
                            .collect::<Vec<HeaderName>>(),
                    ),
            )
            .layer(GrpcWebLayer::new())
            .add_service(query_service)
            .serve(addr)
            .await?;
        Ok(())
    })
}

const N_PEERS: u64 = 3;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let peers: Vec<u64> = (1..=N_PEERS).collect();
    let logger = build_default_logger();

    let network = Network::new(&peers, logger.clone());

    let server_handle = build_and_start_server(network.clone());

    let node_handles = cluster::try_restore_cluster(&peers, &logger, &network)
        .unwrap_or_else(|_| cluster::init_cluster(&peers, &logger, &network));

    server_handle.join().unwrap().await?;

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
    Ok(())
}
