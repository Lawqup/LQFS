use uuid::Uuid;

pub mod error;
pub mod msg;

pub struct Fragment {
    pub data: Vec<u8>,
    pub file_id: Uuid,
    pub frag_idx: usize,
}

impl Fragment {
    const FRAG_SIZE: usize = 1024;

    pub fn from_bytes(buf: Vec<u8>) -> Vec<Fragment> {
        let n_frags = (buf.len() as f64 / Self::FRAG_SIZE as f64).ceil() as usize;

        (0..n_frags)
            .map(|frag_idx| {
                let data = buf.iter().take(Self::FRAG_SIZE).copied().collect();

                Fragment {
                    data,
                    file_id: Uuid::new_v4(),
                    frag_idx,
                }
            })
            .collect()
    }
}
