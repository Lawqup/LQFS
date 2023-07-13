mod network;
mod node;
mod prelude;

use std::{
    sync::{Arc, Mutex},
    thread,
};

use network::NetworkController;
use node::Node;
use prelude::*;

fn main() {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build();
    let drain = std::sync::Mutex::new(drain).fuse();

    let logger = slog::Logger::root(drain, o!());

    let peers = [1, 2, 4];
    let network = Arc::new(Mutex::new(NetworkController::new(&peers, logger.clone())));

    let mut handles = Vec::new();
    for id in peers {
        let network = network.clone();
        let logger = logger.clone();
        handles.push(thread::spawn(move || {
            let mut node = Node::new(id, network, logger);
            node.run();
        }));
    }

    network.lock().unwrap().listen_for_proposals();

    for handle in handles {
        handle.join().expect("Handle could not be joined");
    }
}
