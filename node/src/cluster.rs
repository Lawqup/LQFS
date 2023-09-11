use std::{
    collections::{HashMap, HashSet},
    thread::{self, JoinHandle},
    time::Duration,
};

use slog::Logger;

use crate::{
    network::{Network, Request, RequestMsg, ResponseMsg},
    node::Node,
    prelude::*,
};

pub struct InitResult {
    pub network: Network,
    pub node_handles: Vec<JoinHandle<Node>>,
}

pub fn init_cluster(peers: &[u64], logger: &Logger) -> InitResult {
    let network = Network::new(peers, logger.clone());

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

    wait_until_cluster_initialized(peers, network.clone(), logger);

    InitResult {
        network,
        node_handles,
    }
}

fn wait_until_cluster_initialized(peers: &[u64], network: Network, logger: &Logger) {
    let queries: HashMap<u64, Request> = peers
        .iter()
        .copied()
        .map(|node| {
            (
                node,
                Request {
                    id: Uuid::new_v4(),
                    req: RequestMsg::GetRaftState(RaftStateRequest { to_node: node }),
                },
            )
        })
        .collect();

    let mut initialized = HashSet::new();
    let mut leader_elected = false;
    for (node, query) in queries.iter().cycle() {
        network.request_to_node(*node, query.clone());

        let resp = network.wait_for_response_timeout(query.id, Duration::from_millis(500));
        if resp.is_none() {
            continue;
        }

        match resp.unwrap().res {
            ResponseMsg::RaftState(RaftStateResponse { is_leader, .. }) if is_leader => {
                info!(logger, "Node {node} elected as leader");
                initialized.insert(node);
                leader_elected = true;
            }
            ResponseMsg::RaftState(RaftStateResponse { is_initialized, .. }) if is_initialized => {
                info!(logger, "Node {node} initialized");
                initialized.insert(node);
            }
            _ => {}
        }

        if initialized.len() == peers.len() && leader_elected {
            break;
        }

        thread::sleep(Duration::from_millis(300));
    }
}

/// Restores the cluster from persistent storage.
/// At least one node's data must have been initialized.
pub fn try_restore_cluster(peers: &[u64], logger: &Logger) -> Result<InitResult> {
    let network = Network::new(peers, logger.clone());

    Ok(InitResult {
        network: network.clone(),
        node_handles: try_restore_cluster_with_network(peers, logger, network)?,
    })
}

fn try_restore_cluster_with_network(
    peers: &[u64],
    logger: &Logger,
    network: Network,
) -> Result<Vec<JoinHandle<Node>>> {
    let mut can_recover = false;
    let mut nodes = Vec::new();
    for &id in peers.iter() {
        nodes.push(match Node::try_restore(id, network.clone(), logger) {
            Ok(node) => {
                can_recover = true;
                node
            }
            Err(_) => Node::new_follower(id, network.clone(), logger),
        });
    }

    if !can_recover {
        return Err(Error::InitError);
    }

    let mut node_handles = Vec::new();
    for mut node in nodes {
        node_handles.push(thread::spawn(move || {
            node.run();
            node
        }));
    }

    info!(logger, "CLUSTER WAITING");

    wait_until_cluster_initialized(peers, network.clone(), logger);

    info!(logger, "CLUSTER INITIALIZED");

    Ok(node_handles)
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        env, fs,
        sync::{atomic::AtomicBool, Arc, Mutex},
        thread::{self, JoinHandle},
        time::Duration,
    };

    use ntest::{test_case, timeout};
    use raft::StateRole;

    use crate::{
        cluster::{init_cluster, try_restore_cluster_with_network, InitResult},
        network::{Network, Proposal, Request, RequestMsg, ResponseMsg},
        prelude::*,
    };

    use crate::node::Node;

    const N_PROPOSALS: usize = 5;
    const DURATION_PER_PROPOSAL: Duration = Duration::from_millis(200);
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
        logger: Logger,
        network: Network,
    ) -> JoinHandle<()> {
        let proposals: Vec<Request> = (0..n_proposals)
            .map(|i| {
                let frag = Fragment {
                    file_name: client_id.to_string(),
                    frag_idx: i as u64,
                    total_frags: n_proposals as u64,
                    data: i.to_le_bytes().to_vec(),
                };

                Request {
                    id: Uuid::new_v4(),
                    req: RequestMsg::Propose(Proposal::WriteFrag(frag)),
                }
            })
            .collect();

        let proposals = Arc::new(Mutex::new(proposals));

        let proposals_clone = proposals.clone();
        let logger_clone = logger.clone();
        let network_clone = network.clone();
        thread::spawn(move || {
            while !proposals_clone.lock().unwrap().is_empty() {
                debug!(
                    logger_clone,
                    "{} PROPOSALS LEFT",
                    proposals_clone.lock().unwrap().len()
                );

                for prop in proposals_clone.lock().unwrap().iter() {
                    info!(logger_clone, "CLIENT proposal {}", prop.id);
                    network_clone.request_to_cluster(prop.clone());
                    thread::sleep(DURATION_PER_PROPOSAL);
                }
                thread::sleep(Duration::from_millis(200));
            }
        });

        thread::spawn(move || {
            while !proposals.lock().unwrap().is_empty() {
                let prop_id = proposals.lock().unwrap().last().unwrap().id;
                let res = network.wait_for_response_timeout(prop_id, CLIENT_TIMEOUT);

                assert!(
                    res.is_some(),
                    "Client {client_id} waited too long for response"
                );

                let res = res.unwrap();
                match res.res {
                    ResponseMsg::WriteFragSuccess(WriteFragResponse { success }) => {
                        let last = proposals.lock().unwrap().len() - 1;
                        proposals.lock().unwrap().remove(last);

                        assert_eq!(res.id, prop_id);
                        info!(
                            logger,
                            "Proposal {} {}",
                            prop_id,
                            if success { "SUCCESS" } else { "FAILURE" }
                        );
                    }
                    _ => panic!("Incorrect response variant recieved"),
                }
            }
        })
    }

    fn spawn_clients(clients: &[u64], logger: &Logger, network: Network) -> Vec<JoinHandle<()>> {
        let mut client_handles = Vec::new();
        for &client_id in clients {
            client_handles.push(spawn_client(
                client_id,
                N_PROPOSALS,
                logger.clone(),
                network.clone(),
            ));
        }

        client_handles
    }

    /// As a client, attempts to get and recombine an entire file
    /// by querying the whole cluster
    pub fn client_get_file(
        file_name: &str,
        client_id: u64,
        network: Network,
        logger: &Logger,
    ) -> Result<Vec<u8>> {
        let query = Request {
            id: Uuid::new_v4(),
            req: RequestMsg::ReadFrags(ReadFragsRequest {
                file_name: file_name.to_string(),
            }),
        };

        let query_clone = query.clone();
        let network_clone = network.clone();
        let got_response = Arc::new(AtomicBool::new(false));
        let got_response_clone = got_response.clone();
        thread::spawn(move || {
            while !got_response_clone.load(std::sync::atomic::Ordering::Relaxed) {
                network_clone.request_to_cluster(query_clone.clone());

                thread::sleep(DURATION_PER_PROPOSAL);
            }
        });

        let mut frags = HashMap::new();

        let res = network.wait_for_response_timeout(query.id, CLIENT_TIMEOUT);

        assert!(
            res.is_some(),
            "Client {client_id} waited too long for response"
        );

        got_response.store(true, std::sync::atomic::Ordering::Relaxed);
        let res = res.unwrap();
        match res.res {
            ResponseMsg::Frags(f) => {
                info!(logger, "Client {client_id} got fragment response");

                for frag in f.frags {
                    frags.insert(frag.frag_idx, frag);
                }
            }
            _ => panic!("Incorrect response variant recieved"),
        }

        if frags.is_empty() || frags.len() as u64 != frags[&0].total_frags {
            Err(Error::FileRetrievalError)
        } else {
            let mut frags: Vec<_> = frags.into_values().collect();
            frags.sort_by_key(|f| f.frag_idx);
            Ok(frags.iter().flat_map(|f| f.data.clone()).collect())
        }
    }

    fn cleanup(
        client_handles: Option<Vec<JoinHandle<()>>>,
        nodes: Option<(&[u64], Vec<JoinHandle<Node>>)>,
        network: Network,
    ) {
        if let Some(client_handles) = client_handles {
            for handle in client_handles {
                handle.join().expect("handle could not be joined");
            }
        }

        if let Some((peers, node_handles)) = nodes {
            for &peer in peers {
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
            } = init_cluster(&peers, &logger);

            for &peer in peers.iter() {
                network.request_to_node(
                    peer,
                    Request {
                        id: Uuid::new_v4(),
                        req: RequestMsg::ShutdownNode(ShutdownNodeRequest { to_node: peer }),
                    },
                )
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
    #[timeout(5000)]
    fn simple(n_peers: u64, n_clients: u64) {
        in_temp_dir!({
            let peers: Vec<u64> = (1..=n_peers).collect();
            let clients: Vec<u64> = (1..=n_clients).collect();
            let logger = build_debug_logger();

            let InitResult {
                network,
                node_handles,
                ..
            } = init_cluster(&peers, &logger);

            let client_handles = spawn_clients(&clients, &logger, network.clone());

            cleanup(Some(client_handles), None, network.clone());

            for c in clients {
                let file = client_get_file(&c.to_string(), c, network.clone(), &logger)
                    .expect("Could not get file");

                info!(logger, "Got file for client {c} with contents: {file:?}");

                assert_eq!(
                    file,
                    (0..N_PROPOSALS)
                        .flat_map(|i| i.to_le_bytes())
                        .collect::<Vec<_>>()
                )
            }

            cleanup(None, Some((&peers, node_handles)), network);
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
                ..
            } = init_cluster(&peers, &logger);

            let client_handles = spawn_clients(&clients, &logger, network.clone());

            network.request_to_node(
                1,
                Request {
                    id: Uuid::new_v4(),
                    req: RequestMsg::ShutdownNode(ShutdownNodeRequest { to_node: 1 }),
                },
            );

            cleanup(Some(client_handles), Some((&peers, node_handles)), network);
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
                ..
            } = init_cluster(&peers, &logger);

            let client_handles = spawn_clients(&clients, &logger, network.clone());
            thread::sleep(Duration::from_millis(100));

            cleanup(None, Some((&peers, node_handles)), network.clone());

            debug!(logger, "ALL NODES SHUTDOWN");

            let node_handles =
                try_restore_cluster_with_network(&peers, &logger, network.clone()).unwrap();

            cleanup(Some(client_handles), Some((&peers, node_handles)), network);
        });
    }
}
