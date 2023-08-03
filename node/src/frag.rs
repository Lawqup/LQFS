use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::prelude::*;

use self::fragment::Fragment;

include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));

struct FragManager {
    dir: PathBuf,
}

impl FragManager {
    fn new(node_id: u64) -> Self {
        Self {
            dir: PathBuf::from(format!("store/node-{node_id}/")),
        }
    }

    fn apply(frag: Fragment) {
        todo!()
    }
}
