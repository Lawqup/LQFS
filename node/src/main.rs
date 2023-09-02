#![allow(dead_code)]

use cluster::InitResult;
use network::Signal;
use prelude::*;

mod cluster;
mod frag;
mod network;
mod node;
mod prelude;
mod storage;

const N_PEERS: u64 = 3;

fn main() {
    let peers: Vec<u64> = (1..=N_PEERS).collect();
    let logger = build_default_logger();

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
}
