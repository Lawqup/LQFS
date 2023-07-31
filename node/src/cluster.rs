use std::{
    collections::{HashMap, HashSet},
    sync::{mpsc::Receiver, Arc, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

use slog::Logger;

use crate::{
    network::{QueryMsg, ResponseMsg},
    prelude::*,
};

use crate::{
    network::{Network, NetworkController, Response},
    node::Node,
};

pub struct InitResult {
    network: Network,
    client_rxs: HashMap<u64, Receiver<Response>>,
    node_handles: Vec<JoinHandle<Node>>,
}

fn init_cluster(peers: &[u64], clients: &[u64], logger: &Logger) -> InitResult {
    let (network, mut client_rxs) = NetworkController::new(peers, clients, logger.clone());
    let network = Arc::new(Mutex::new(network));

    let technician = clients.iter().max().copied().unwrap_or_default() + 1;
    let tech_rx = network.lock().unwrap().add_client(technician);

    let mut node_handles = Vec::new();
    for &id in peers.iter().skip(1) {
        let network = network.clone();
        let logger = logger.clone();
        node_handles.push(thread::spawn(move || {
            info!(&logger, "Starting follower (id: {id})");
            let mut node = Node::new_follower(id, network, &logger);
            node.run();
            node
        }));
    }

    let leader_id = peers[0];

    let logger_clone = logger.clone();
    let network_clone = network.clone();
    node_handles.push(thread::spawn(move || {
        info!(logger_clone, "Starting leader (id: {leader_id})");
        let mut leader = Node::new_leader(leader_id, network_clone, &logger_clone);
        leader.run();
        leader
    }));

    let mut still_uninit: HashSet<u64> = peers.iter().copied().collect();
    while !still_uninit.is_empty() {
        let mut to_remove = Vec::new();

        for node in still_uninit.iter().copied() {
            network
                .lock()
                .unwrap()
                .send_query_message(node, QueryMsg::IsInitialized { from: technician });
            match tech_rx.recv().unwrap().msg {
                ResponseMsg::Initialized(true) => {
                    to_remove.push(node);
                }
                ResponseMsg::Initialized(false) => (),
                _ => panic!("Incorrect response variant recieved"),
            }
        }

        for node in to_remove.iter() {
            still_uninit.remove(node);
        }
        to_remove.clear();

        thread::sleep(Duration::from_millis(200));
    }

    client_rxs.remove(&technician);
    InitResult {
        network,
        client_rxs,
        node_handles,
    }
}

// Restores the cluster from persistent storage.
// At least the leader's data must have been initialized.
pub fn restore_cluster(
    peers: &[u64],
    clients: &[u64],
    logger: &Logger,
    network: Network,
) -> Vec<JoinHandle<Node>> {
    let technician = clients.iter().max().copied().unwrap_or_default() + 1;
    let tech_rx = network.lock().unwrap().add_client(technician);

    let mut node_handles = Vec::new();
    for &id in peers.iter() {
        let network = network.clone();
        let logger = logger.clone();
        node_handles.push(thread::spawn(move || {
            let mut node = match Node::try_restore(id, network.clone(), &logger) {
                Ok(node) => node,
                Err(_) => Node::new_follower(id, network, &logger),
            };
            node.run();
            node
        }));
    }

    let mut still_uninit: HashSet<u64> = peers.iter().copied().collect();
    while !still_uninit.is_empty() {
        let mut to_remove = Vec::new();

        for node in still_uninit.iter().copied() {
            network
                .lock()
                .unwrap()
                .send_query_message(node, QueryMsg::IsInitialized { from: technician });
            match tech_rx.recv().unwrap().msg {
                ResponseMsg::Initialized(true) => {
                    to_remove.push(node);
                }
                ResponseMsg::Initialized(false) => (),
                _ => panic!("Incorrect response variant recieved"),
            }
        }

        for node in to_remove.iter() {
            still_uninit.remove(node);
        }
        to_remove.clear();

        thread::sleep(Duration::from_millis(200));
    }

    node_handles
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        env, fs,
        sync::{mpsc::Receiver, Arc, Mutex},
        thread::{self, JoinHandle},
        time::Duration,
    };

    use ntest::{test_case, timeout};
    use protobuf::Message;
    use raft::StateRole;

    use crate::{
        cluster::{init_cluster, restore_cluster, InitResult},
        fs::fragment::Fragment,
        network::{Network, RequestMsg, Response, ResponseMsg, Signal},
        prelude::*,
    };

    use crate::node::Node;

    const N_PROPOSALS: usize = 10;
    const DURATION_PER_PROPOSAL: Duration = Duration::from_millis(100);
    const CLIENT_TIMEOUT: Duration = Duration::from_millis(5000);

    macro_rules! in_temp_dir {
        ($block:block) => {
            let tmpdir = tempfile::tempdir().unwrap();
            env::set_current_dir(&tmpdir).unwrap();
            fs::create_dir("store").unwrap();

            $block;
        };
    }

    fn spawn_client(
        client_id: u64,
        n_proposals: usize,
        client_rx: Mutex<Receiver<Response>>,
        logger: Logger,
        network: Network,
    ) -> JoinHandle<()> {
        let proposals: HashMap<Uuid, Proposal> = (0..n_proposals)
            .map(|i| {
                let frag = Fragment {
                    file_idx: i as u64,
                    data: b"hello".to_vec(),
                    ..Default::default()
                };
                dbg!(&frag);
                let prop = Proposal::new_fragment(client_id, frag);
                (prop.id, prop)
            })
            .collect();

        let proposals = Arc::new(Mutex::new(proposals));

        let proposals_clone = proposals.clone();
        let logger_clone = logger.clone();
        thread::spawn(move || {
            while !proposals_clone.lock().unwrap().is_empty() {
                debug!(
                    logger_clone,
                    "{} PROPOSALS LEFT",
                    proposals_clone.lock().unwrap().len()
                );

                for prop in proposals_clone.lock().unwrap().values() {
                    info!(logger_clone, "CLIENT proposal ({}, {})", prop.id, prop.from);
                    for tx in network.lock().unwrap().raft_senders.values() {
                        tx.send(RequestMsg::Propose(prop.clone())).unwrap();
                    }
                    thread::sleep(DURATION_PER_PROPOSAL);
                }
                thread::sleep(Duration::from_millis(200));
            }
        });

        thread::spawn(move || {
            while !proposals.lock().unwrap().is_empty() {
                let res = client_rx.lock().unwrap().recv_timeout(CLIENT_TIMEOUT);

                assert!(
                    res.is_ok(),
                    "Client {client_id} waited too long for response"
                );

                let res = res.unwrap();
                match res.msg {
                    ResponseMsg::Proposed {
                        proposal_id,
                        success,
                    } => {
                        proposals
                            .lock()
                            .unwrap()
                            .remove(&proposal_id)
                            .expect("Proposal didn't exist in the queue.");

                        assert_eq!(res.to, client_id);
                        info!(
                            logger,
                            "Proposal ({}, {}) {}",
                            proposal_id,
                            res.to,
                            if success { "SUCCESS" } else { "FAILURE" }
                        );
                    }
                    _ => panic!("Incorrect response variant recieved"),
                }
            }
        })
    }

    fn spawn_clients(
        clients: &[u64],
        mut client_rxs: HashMap<u64, Receiver<Response>>,
        logger: &Logger,
        network: Network,
    ) -> Vec<JoinHandle<()>> {
        let mut client_handles = Vec::new();
        for &client_id in clients {
            client_handles.push(spawn_client(
                client_id,
                N_PROPOSALS,
                Mutex::new(client_rxs.remove(&client_id).unwrap()),
                logger.clone(),
                network.clone(),
            ));
        }

        client_handles
    }

    fn cleanup(
        client_handles: Vec<JoinHandle<()>>,
        node_handles: Vec<JoinHandle<Node>>,
        network: Network,
        peers: &[u64],
    ) {
        for handle in client_handles {
            handle.join().expect("handle could not be joined");
        }

        for id in peers {
            network
                .lock()
                .unwrap()
                .send_control_message(*id, Signal::Shutdown);
        }

        for handle in node_handles {
            handle.join().expect("handle could not be joined");
        }
    }

    #[test_case(1)]
    #[test_case(3)]
    #[test_case(9)]
    #[timeout(2000)]
    fn leader_election(n_peers: u64) {
        in_temp_dir!({
            let peers: Vec<u64> = (1..=n_peers).collect();
            let logger = build_debug_logger();

            let InitResult {
                network,
                node_handles,
                ..
            } = init_cluster(&peers, &[], &logger);

            for id in peers.clone() {
                network
                    .lock()
                    .unwrap()
                    .send_control_message(id, Signal::Shutdown);
            }

            let mut n_leaders = 0;
            let mut n_followers = 0;
            for handle in node_handles {
                let node = handle.join().expect("Handle could not be joined");

                assert!(node.raft.is_some());

                match node.raft().raft.state {
                    StateRole::Leader => n_leaders += 1,
                    StateRole::Follower => n_followers += 1,
                    _ => (),
                }
            }

            assert_eq!(1, n_leaders);
            assert_eq!(peers.len() - 1, n_followers);
        });
    }

    #[test_case(1, 1, name = "single_node_1_client_simple")]
    #[test_case(1, 5, name = "single_node_5_client_simple")]
    #[test_case(5, 5, name = "five_node_5_client_simple")]
    #[timeout(3000)]
    fn simple(n_peers: u64, n_clients: u64) {
        in_temp_dir!({
            let peers: Vec<u64> = (1..=n_peers).collect();
            let clients: Vec<u64> = (1..=n_clients).collect();
            let logger = build_debug_logger();

            let InitResult {
                network,
                node_handles,
                client_rxs,
                ..
            } = init_cluster(&peers, &clients, &logger);

            let client_handles = spawn_clients(&clients, client_rxs, &logger, network.clone());

            cleanup(client_handles, node_handles, network, &peers);
        });
    }

    #[test_case(3, 5, name = "three_node_5_client_node_failure")]
    #[test_case(5, 1, name = "five_node_1_client_node_failure")]
    #[timeout(6000)]
    fn node_failure(n_peers: u64, n_clients: u64) {
        in_temp_dir!({
            let peers: Vec<u64> = (1..=n_peers).collect();
            let clients: Vec<u64> = (1..=n_clients).collect();
            let logger = build_debug_logger();

            let InitResult {
                network,
                node_handles,
                client_rxs,
                ..
            } = init_cluster(&peers, &clients, &logger);

            let client_handles = spawn_clients(&clients, client_rxs, &logger, network.clone());

            network
                .lock()
                .unwrap()
                .send_control_message(1, Signal::Shutdown);

            cleanup(client_handles, node_handles, network, &peers);
        });
    }

    #[test_case(1, name = "single_node_reboot_all")]
    #[test_case(5, name = "five_node_reboot_all")]
    #[timeout(5000)]
    fn reboot_all(n_peers: u64) {
        in_temp_dir!({
            let peers: Vec<u64> = (1..=n_peers).collect();
            let clients: Vec<u64> = vec![1, 2, 3];
            let logger = build_debug_logger();

            let InitResult {
                network,
                node_handles,
                client_rxs,
                ..
            } = init_cluster(&peers, &clients, &logger);

            let client_handles = spawn_clients(&clients, client_rxs, &logger, network.clone());

            for id in peers.clone() {
                network
                    .lock()
                    .unwrap()
                    .send_control_message(id, Signal::Shutdown);
            }

            for handle in node_handles {
                handle.join().expect("handle could not be joined");
            }

            debug!(logger, "ALL NODES SHUTDOWN");

            let node_handles = restore_cluster(&peers, &clients, &logger, network.clone());

            cleanup(client_handles, node_handles, network, &peers);
        });
    }
}
