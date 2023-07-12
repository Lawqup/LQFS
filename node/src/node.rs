//! Defines the node struct and provides its APIs
use lqfs_lib::Fragment;
use raft::{prelude::Message, storage::MemStorage, Config, RawNode};

use std::{
    collections::HashMap,
    sync::mpsc::{channel, RecvTimeoutError},
    time::{Duration, Instant},
};

use uuid::Uuid;

type Callback = Box<dyn Fn() + Send>;
pub enum Msg {
    Proposal {
        client_id: Uuid,
        proposal_num: u32,
        callback: Callback,
    },
    Raft(Message),
}

pub struct Node {
    raft: RawNode<MemStorage>,
}

impl Node {
    pub fn new() -> Self {
        let config = Config {
            id: 1,
            ..Default::default()
        };

        config.validate().expect("Raft config should be valid");

        let storage = MemStorage::new_with_conf_state((vec![1], vec![]));
        let raft = RawNode::with_default_logger(&config, storage).unwrap();

        Self { raft }
    }

    pub fn recv_fragment(frag: Fragment) {
        todo!()
    }

    pub fn run(&mut self) {
        // We're using a channel, but this could be any stream of events.
        let (tx, rx) = channel();
        let timeout = Duration::from_millis(100);
        let mut remaining_timeout = timeout;

        // Send the `tx` somewhere else...
        tx.send(Msg::Proposal {
            client_id: Uuid::new_v4(),
            proposal_num: 1,
            callback: Box::new(|| println!("cb called!")),
        });

        let mut cbs = HashMap::new();
        loop {
            let now = Instant::now();

            match rx.recv_timeout(remaining_timeout) {
                Ok(Msg::Proposal {
                    client_id,
                    proposal_num,
                    callback,
                }) => {
                    cbs.insert((client_id, proposal_num), callback);
                }
                Ok(Msg::Raft(m)) => self.raft.step(m).unwrap(),
                Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => unimplemented!(),
            }

            let elapsed = now.elapsed();
            if elapsed >= remaining_timeout {
                remaining_timeout = timeout;
                // We drive Raft every 100ms.
                self.raft.tick();
                println!("tick");
            } else {
                remaining_timeout -= elapsed;
            }

            self.on_ready(&mut cbs)
        }
    }

    fn on_ready(&mut self, cbs: &mut HashMap<(Uuid, u32), Callback>) {
        todo!()
    }
}
