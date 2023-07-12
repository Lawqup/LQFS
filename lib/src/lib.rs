use std::{
    fs::File,
    io::{Read, Result},
};

use uuid::Uuid;

pub struct Fragment {
    pub data: Vec<u8>,
    pub file_id: Uuid,
    pub frag_idx: usize,
}

impl Fragment {
    const FRAG_SIZE: usize = 1024;

    pub fn from_file(path: &str) -> Result<Vec<Fragment>> {
        let mut f = File::open(path)?;
        let mut buf = Vec::new();

        f.read_to_end(&mut buf)?;

        let n_frags = (buf.len() as f64 / Self::FRAG_SIZE as f64).ceil() as usize;

        Ok((0..n_frags)
            .map(|frag_idx| {
                let data = buf
                    .iter()
                    .copied()
                    .skip(frag_idx * Self::FRAG_SIZE)
                    .take(Self::FRAG_SIZE)
                    .collect();

                Fragment {
                    data,
                    file_id: Uuid::new_v4(),
                    frag_idx,
                }
            })
            .collect())
    }
}
