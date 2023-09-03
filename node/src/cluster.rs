use std::{
    collections::HashSet,
    sync::Arc,
    thread::{self},
    time::Duration,
};

use slog::Logger;

use tokio::{
    sync::Mutex,
    task::{self, JoinHandle},
};

use crate::{
    network::{QueryMsg, ResponseMsg},
    prelude::*,
};

use crate::{
    network::{Network, NetworkController},
    node::Node,
};

pub struct InitResult {
    pub network: Network,
    pub node_handles: Vec<JoinHandle<Node>>,
}

pub async fn init_cluster(peers: &[u64], clients: &[u64], logger: &Logger) -> InitResult {
    let network = NetworkController::new(peers, clients, logger.clone());
    let network = Arc::new(Mutex::new(network));

    let technician = clients.iter().max().copied().unwrap_or_default() + 1;
    network.lock().await.add_client(technician).await;

    let mut node_handles = Vec::new();
    for &id in peers.iter().skip(1) {
        let network = network.clone();
        let logger = logger.clone();
        node_handles.push(task::spawn(async move {
            info!(&logger, "Starting follower (id: {id})");
            let mut node = Node::new_follower(id, network, &logger);
            node.run().await;
            node
        }));
    }

    let leader_id = peers[0];

    let logger_clone = logger.clone();
    let network_clone = network.clone();
    node_handles.push(task::spawn(async move {
        info!(logger_clone, "Starting leader (id: {leader_id})");
        let mut leader = Node::new_leader(leader_id, network_clone, &logger_clone).await;
        leader.run().await;
        leader
    }));

    let mut still_uninit: HashSet<u64> = peers.iter().copied().collect();
    while !still_uninit.is_empty() {
        let mut to_remove = Vec::new();

        for node in still_uninit.iter().copied() {
            network
                .lock()
                .await
                .send_query_message(node, QueryMsg::IsInitialized { from: technician })
                .await;

            match network
                .lock()
                .await
                .get_client_receiver(technician)
                .recv()
                .await
                .unwrap()
                .msg
            {
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

    InitResult {
        network,
        node_handles,
    }
}

/// Restores the cluster from persistent storage.
/// At least one node's data must have been initialized.
pub async fn try_restore_cluster(
    peers: &[u64],
    clients: &[u64],
    logger: &Logger,
) -> Result<InitResult> {
    let network = NetworkController::new(peers, clients, logger.clone());
    let network = Arc::new(Mutex::new(network));

    Ok(InitResult {
        network: network.clone(),
        node_handles: try_restore_cluster_with_network(peers, clients, logger, network).await?,
    })
}

async fn try_restore_cluster_with_network(
    peers: &[u64],
    clients: &[u64],
    logger: &Logger,
    network: Network,
) -> Result<Vec<JoinHandle<Node>>> {
    let technician = clients.iter().max().copied().unwrap_or_default() + 1;
    network.lock().await.add_client(technician).await;

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
        node_handles.push(task::spawn(async move {
            node.run().await;
            node
        }));
    }

    let mut still_uninit: HashSet<u64> = peers.iter().copied().collect();
    while !still_uninit.is_empty() {
        let mut to_remove = Vec::new();

        for node in still_uninit.iter().copied() {
            network
                .lock()
                .await
                .send_query_message(node, QueryMsg::IsInitialized { from: technician })
                .await;

            match network
                .lock()
                .await
                .get_client_receiver(technician)
                .recv()
                .await
                .unwrap()
                .msg
            {
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

    Ok(node_handles)
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        env, fs,
        sync::Arc,
        thread,
        time::{Duration, Instant},
    };

    use tokio::{
        runtime::Runtime,
        task::{self, JoinHandle},
    };

    use ntest::{test_case, timeout};
    use raft::StateRole;

    use crate::{
        cluster::{init_cluster, try_restore_cluster_with_network, InitResult},
        frag::Fragment,
        network::{Network, QueryMsg, RequestMsg, ResponseMsg, Signal},
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

            Runtime::new().unwrap().block_on(async $block);
        };
    }

    fn spawn_client(
        client_id: u64,
        n_proposals: usize,
        logger: Logger,
        network: Network,
    ) -> JoinHandle<()> {
        let proposals: HashMap<Uuid, Proposal> = (0..n_proposals)
            .map(|i| {
                let frag = Fragment {
                    name: client_id.to_string(),
                    file_idx: i as u64,
                    total_frags: n_proposals as u64,
                    data: i.to_le_bytes().to_vec(),
                    ..Default::default()
                };
                let prop = Proposal::new_fragment(client_id, frag);
                (prop.id, prop)
            })
            .collect();

        let proposals = Arc::new(tokio::sync::Mutex::new(proposals));

        let proposals_clone = proposals.clone();
        let logger_clone = logger.clone();
        let network_clone = network.clone();
        task::spawn(async move {
            while !proposals_clone.lock().await.is_empty() {
                debug!(
                    logger_clone,
                    "{} PROPOSALS LEFT",
                    proposals_clone.lock().await.len()
                );

                for prop in proposals_clone.lock().await.values() {
                    info!(logger_clone, "CLIENT proposal ({}, {})", prop.id, prop.from);
                    for tx in network_clone.lock().await.raft_senders.values() {
                        tx.send(RequestMsg::Propose(prop.clone())).await.unwrap();
                    }
                    thread::sleep(DURATION_PER_PROPOSAL);
                }
                thread::sleep(Duration::from_millis(200));
            }
        });

        task::spawn(async move {
            while !proposals.lock().await.is_empty() {
                let now = Instant::now();
                let mut res = None;
                while now.elapsed() < CLIENT_TIMEOUT {
                    if let Ok(r) = network
                        .lock()
                        .await
                        .get_client_receiver(client_id)
                        .try_recv()
                    {
                        res = Some(r)
                    }
                }

                assert!(
                    res.is_some(),
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
                            .await
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
    pub async fn client_get_file(
        file_name: &str,
        client_id: u64,
        network: Network,
        logger: &Logger,
    ) -> Result<Vec<u8>> {
        let queries: HashMap<u64, QueryMsg> = network
            .lock()
            .await
            .peers()
            .into_iter()
            .map(|p| {
                let query = QueryMsg::ReadFrags {
                    from: client_id,
                    file_name: file_name.to_string(),
                };

                (p, query)
            })
            .collect();

        let queries = Arc::new(tokio::sync::Mutex::new(queries));

        let queries_clone = queries.clone();
        let network_clone = network.clone();
        task::spawn(async move {
            while !queries_clone.lock().await.is_empty() {
                for (to, query) in queries_clone.lock().await.iter() {
                    network_clone
                        .lock()
                        .await
                        .send_query_message(*to, query.clone());

                    thread::sleep(DURATION_PER_PROPOSAL);
                }
                thread::sleep(Duration::from_millis(200));
            }
        });

        let mut frags = HashMap::new();

        while !queries.lock().await.is_empty() {
            let now = Instant::now();
            let mut res = None;
            while now.elapsed() < CLIENT_TIMEOUT {
                if let Ok(r) = network
                    .lock()
                    .await
                    .get_client_receiver(client_id)
                    .try_recv()
                {
                    res = Some(r)
                }
            }

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

                    queries.lock().await.remove(&res.from);
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

    async fn cleanup(
        client_handles: Option<Vec<JoinHandle<()>>>,
        nodes: Option<(&[u64], Vec<JoinHandle<Node>>)>,
        network: Network,
    ) {
        if let Some(client_handles) = client_handles {
            for handle in client_handles {
                handle.await;
            }
        }

        if let Some((peers, node_handles)) = nodes {
            for id in peers {
                network
                    .lock()
                    .await
                    .send_control_message(*id, Signal::Shutdown)
                    .await;
            }

            for handle in node_handles {
                handle.await;
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
            } = init_cluster(&peers, &[], &logger).await;

            for id in peers.clone() {
                network
                    .lock()
                    .await
                    .send_control_message(id, Signal::Shutdown)
                    .await;
            }

            let mut n_leaders = 0;
            let mut n_followers = 0;
            for handle in node_handles {
                let node = handle.await.expect("Handle could not be joined");

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
            } = init_cluster(&peers, &clients, &logger).await;

            let client_handles = spawn_clients(&clients, &logger, network.clone());

            cleanup(Some(client_handles), None, network.clone());

            for c in clients {
                let file = client_get_file(&c.to_string(), c, network.clone(), &logger)
                    .await
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
            } = init_cluster(&peers, &clients, &logger).await;

            let client_handles = spawn_clients(&clients, &logger, network.clone());

            network
                .lock()
                .await
                .send_control_message(1, Signal::Shutdown);

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
            } = init_cluster(&peers, &clients, &logger).await;

            let client_handles = spawn_clients(&clients, &logger, network.clone());
            thread::sleep(Duration::from_millis(100));

            cleanup(None, Some((&peers, node_handles)), network.clone());

            debug!(logger, "ALL NODES SHUTDOWN");

            let node_handles =
                try_restore_cluster_with_network(&peers, &clients, &logger, network.clone())
                    .await
                    .unwrap();

            cleanup(Some(client_handles), Some((&peers, node_handles)), network);
        });
    }
}
