use std::{
    fs,
    io::{Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::prelude::*;

use self::fragment::Fragment;

include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));

pub struct FSManager {
    dir: PathBuf,
}

pub type FS = Arc<Mutex<FSManager>>;

impl FSManager {
    pub fn new(node_id: u64) -> Self {
        Self {
            dir: PathBuf::from(format!("store/node-{node_id}/")),
        }
    }

    /// Persists a fragment at {fs_dir}/{file_name}/{frag_index}:{total_frags_for_file}
    pub fn apply(&self, frag: Fragment) -> Result<()> {
        let dir = self.dir.to_str().unwrap();
        match fs::create_dir(format!("{dir}/{}", frag.name)) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }

        let mut file = fs::File::create(format!(
            "{dir}/{}/{}:{}",
            frag.name, frag.file_idx, frag.total_frags
        ))?;

        file.write_all(&frag.data)?;

        Ok(())
    }

    /// Returns the fragments of a file stored in this node in no particular order
    pub fn get_frags(&self, file_name: &str) -> Result<Vec<Fragment>> {
        let dir = format!("{}/{file_name}", self.dir.to_str().unwrap());
        fs::read_dir(dir.clone())?
            .filter_map(|p| p.ok())
            .map(|p| {
                let binding = p.file_name();
                let (idx, total) = binding.to_str().unwrap().rsplit_once(':').unwrap();
                let path = format!("{dir}/{idx}:{total}");

                let mut data = Vec::new();
                let mut file = fs::File::open(path)?;

                file.read_to_end(&mut data)?;

                Ok(Fragment {
                    name: file_name.to_owned(),
                    file_idx: idx.parse().unwrap(),
                    total_frags: total.parse().unwrap(),
                    data,
                    ..Default::default()
                })
            })
            .collect()
    }
}
