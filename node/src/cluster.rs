use std::{
    collections::HashSet,
    thread::{self, JoinHandle},
    time::Duration,
};

use slog::Logger;

use crate::{
    network::{Network, QueryMsg, ResponseMsg},
    node::Node,
    prelude::*,
};

pub struct InitResult {
    pub network: Network,
    pub node_handles: Vec<JoinHandle<Node>>,
}

pub fn init_cluster(peers: &[u64], clients: &[u64], logger: &Logger) -> InitResult {
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

    wait_until_cluster_initialized(clients, peers, network.clone());

    InitResult {
        network,
        node_handles,
    }
}

fn wait_until_cluster_initialized(clients: &[u64], peers: &[u64], network: Network) {
    let technician = clients.iter().max().copied().unwrap_or_default() + 1;

    let mut still_uninit: HashSet<(u64, Uuid)> = peers
        .iter()
        .copied()
        .map(|node| (node, Uuid::new_v4()))
        .collect();

    while !still_uninit.is_empty() {
        let mut to_remove = Vec::new();

        for (node, query_id) in still_uninit.iter().copied() {
            network.send_query_message(
                node,
                QueryMsg::IsInitialized {
                    id: query_id,
                    from: technician,
                },
            );

            match network.wait_for_response(query_id).msg {
                ResponseMsg::IsInitialized(true) => {
                    to_remove.push((node, query_id));
                }
                ResponseMsg::IsInitialized(false) => (),
                _ => panic!("Incorrect response variant recieved"),
            }
        }

        for q in to_remove.iter() {
            still_uninit.remove(q);
        }
        to_remove.clear();

        thread::sleep(Duration::from_millis(200));
    }
}

/// Restores the cluster from persistent storage.
/// At least one node's data must have been initialized.
pub fn try_restore_cluster(peers: &[u64], clients: &[u64], logger: &Logger) -> Result<InitResult> {
    let network = Network::new(peers, logger.clone());

    Ok(InitResult {
        network: network.clone(),
        node_handles: try_restore_cluster_with_network(peers, clients, logger, network)?,
    })
}

fn try_restore_cluster_with_network(
    peers: &[u64],
    clients: &[u64],
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

    wait_until_cluster_initialized(clients, peers, network.clone());

    Ok(node_handles)
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        env, fs,
        sync::{Arc, Mutex},
        thread::{self, JoinHandle},
        time::Duration,
    };

    use ntest::{test_case, timeout};
    use raft::StateRole;

    use crate::{
        cluster::{init_cluster, try_restore_cluster_with_network, InitResult},
        frag::Fragment,
        network::{Network, QueryMsg, ResponseMsg, Signal},
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
        logger: Logger,
        network: Network,
    ) -> JoinHandle<()> {
        let proposals: Vec<Proposal> = (0..n_proposals)
            .map(|i| {
                let frag = Fragment {
                    name: client_id.to_string(),
                    file_idx: i as u64,
                    total_frags: n_proposals as u64,
                    data: i.to_le_bytes().to_vec(),
                };
                Proposal::new_fragment(client_id, frag)
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
                    info!(logger_clone, "CLIENT proposal ({}, {})", prop.id, prop.from);
                    for peer in network_clone.peers() {
                        network_clone.send_proposal_message(peer, prop.clone());
                    }
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
                match res.msg {
                    ResponseMsg::IsProposed(success) => {
                        let last = proposals.lock().unwrap().len() - 1;
                        proposals.lock().unwrap().remove(last);

                        assert_eq!(res.to, client_id);
                        assert_eq!(res.id, prop_id);
                        info!(
                            logger,
                            "Proposal ({}, {}) {}",
                            prop_id,
                            res.to,
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
        let queries: Vec<(u64, QueryMsg)> = network
            .peers()
            .into_iter()
            .map(|p| {
                let query = QueryMsg::ReadFrags {
                    id: Uuid::new_v4(),
                    from: client_id,
                    file_name: file_name.to_string(),
                };

                (p, query)
            })
            .collect();

        let queries = Arc::new(Mutex::new(queries));

        let queries_clone = queries.clone();
        let network_clone = network.clone();
        thread::spawn(move || {
            while !queries_clone.lock().unwrap().is_empty() {
                for (to, query) in queries_clone.lock().unwrap().iter() {
                    network_clone.send_query_message(*to, query.clone());

                    thread::sleep(DURATION_PER_PROPOSAL);
                }
                thread::sleep(Duration::from_millis(200));
            }
        });

        let mut frags = HashMap::new();

        while !queries.lock().unwrap().is_empty() {
            let query_id = match queries.lock().unwrap().last().unwrap() {
                (_, QueryMsg::ReadFrags { id, .. }) => *id,
                _ => panic!("Incorrect request variant recieved"),
            };

            let res = network.wait_for_response_timeout(query_id, CLIENT_TIMEOUT);

            assert!(
                res.is_some(),
                "Client {client_id} waited too long for response"
            );

            let res = res.unwrap();
            match res.msg {
                ResponseMsg::Frags(f) => {
                    info!(logger, "Client {client_id} got fragment response");

                    assert_eq!(res.to, client_id);
                    for frag in f {
                        frags.insert(frag.file_idx, frag);
                    }

                    let last = queries.lock().unwrap().len() - 1;
                    queries.lock().unwrap().remove(last);
                }
                _ => panic!("Incorrect response variant recieved"),
            }
        }

        if frags.is_empty() || frags.len() as u64 != frags[&0].total_frags {
            Err(Error::FileRetrievalError)
        } else {
            let mut frags: Vec<_> = frags.into_values().collect();
            frags.sort_by_key(|f| f.file_idx);
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
            for id in peers {
                network.send_control_message(*id, Signal::Shutdown);
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
            } = init_cluster(&peers, &[], &logger);

            for id in peers.clone() {
                network.send_control_message(id, Signal::Shutdown);
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
            } = init_cluster(&peers, &clients, &logger);

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
            } = init_cluster(&peers, &clients, &logger);

            let client_handles = spawn_clients(&clients, &logger, network.clone());

            network.send_control_message(1, Signal::Shutdown);

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
            } = init_cluster(&peers, &clients, &logger);

            let client_handles = spawn_clients(&clients, &logger, network.clone());
            thread::sleep(Duration::from_millis(100));

            cleanup(None, Some((&peers, node_handles)), network.clone());

            debug!(logger, "ALL NODES SHUTDOWN");

            let node_handles =
                try_restore_cluster_with_network(&peers, &clients, &logger, network.clone())
                    .unwrap();

            cleanup(Some(client_handles), Some((&peers, node_handles)), network);
        });
    }
}
