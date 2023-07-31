use crate::prelude::*;

include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));

struct FragManager {
    node_id: u64,
}

impl FragManager {
    fn new(node_id: u64) -> Self {
        Self { node_id }
    }

    fn apply(msg: Proposal) {
        todo!()
    }
}
