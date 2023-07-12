use uuid::Uuid;

use crate::Fragment;

pub enum Msg {
    Proposal {
        client_id: Uuid,
        proposal_num: u32,
        callback: Box<dyn Fn() + Send>,
    },
}
