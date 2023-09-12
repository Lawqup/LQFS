mod app;
mod file;

use std::{fmt::Debug, future::Future, sync::Arc};

use app::*;
use gloo_file::{Blob, ObjectUrl};
use gloo_timers::future::TimeoutFuture;
use leptos::*;
use messages::*;
use services::file_store_client::FileStoreClient;
use tonic_web_wasm_client::Client;
use web_sys::File;

#[allow(non_snake_case)]
pub mod services {
    tonic::include_proto!("services");
}

#[allow(non_snake_case)]
pub mod messages {
    tonic::include_proto!("messages");
}

#[derive(Clone, Debug)]
pub struct FSClient(FileStoreClient<Client>);

impl FSClient {
    const MAX_BACKOFF: u32 = 5 * 60 * 1000;
    const FRAG_SIZE: usize = 4096;
    pub fn new() -> Self {
        Self(FileStoreClient::new(Client::new(
            "http://localhost:50051".to_string(),
        )))
    }

    async fn exponential_backoff<'a, T, Fu>(&self, req: impl Fn(FileStoreClient<Client>) -> Fu) -> T
    where
        T: Debug,
        Fu: Future<Output = Option<T>>,
    {
        let mut res = None;

        for i in 0.. {
            res = req(self.0.clone()).await;

            if res.is_some() {
                break;
            }

            let time = std::cmp::min(2_u32.pow(i) + 1000, Self::MAX_BACKOFF);
            log!("Timeout for request {time}");
            TimeoutFuture::new(time).await;
        }

        log!("Response recieved! {res:?}");
        res.unwrap()
    }

    pub async fn get_file_names(&self) -> Vec<String> {
        self.exponential_backoff(|mut client| async move {
            client
                .read_file_names(ReadFileNamesRequest {})
                .await
                .ok()
                .map(|r| r.into_inner().file_names)
        })
        .await
    }

    pub async fn download_file(&self, file_name: &str) -> ObjectUrl {
        let mut frags: Vec<messages::Fragment> = self
            .exponential_backoff(|mut client| async move {
                client
                    .read_frags(ReadFragsRequest {
                        file_name: file_name.to_string(),
                    })
                    .await
                    .ok()
                    .map(|r| r.into_inner().frags)
            })
            .await;

        frags.sort_by_key(|f| f.frag_idx);

        let data: Vec<_> = frags.into_iter().flat_map(|f| f.data).collect();

        let file = Blob::new(data.as_slice());

        // TODO: check total_frags and make sure we have everything
        // (We'll need to request multiple clusters once that's implemented)

        ObjectUrl::from(file)
    }

    pub async fn upload_file(&self, file: File) {
        let file_name = file.name();
        let data = gloo_file::futures::read_as_bytes(&gloo_file::File::from(file))
            .await
            .expect("Should have been able to read file");

        let total_frags = (data.len() as f64 / Self::FRAG_SIZE as f64).ceil() as usize;

        let frags: Vec<_> = (0..total_frags)
            .map(|frag_idx| {
                let data = data
                    .iter()
                    .copied()
                    .skip(frag_idx * Self::FRAG_SIZE)
                    .take(Self::FRAG_SIZE)
                    .collect();

                messages::Fragment {
                    file_name: file_name.to_string(),
                    frag_idx: frag_idx as u64,
                    total_frags: frag_idx as u64,
                    data,
                }
            })
            .collect();

        for frag in frags {
            let f = Arc::new(frag);
            self.exponential_backoff(|mut client| {
                let f1 = f.clone();
                async move {
                    client
                        .write_frag(f1.as_ref().clone())
                        .await
                        .ok()
                        .filter(|r| r.get_ref().success)
                }
            })
            .await;
        }
    }
}

pub fn main() {
    _ = console_log::init_with_level(log::Level::Debug);
    console_error_panic_hook::set_once();

    log!("csr mode - mounting to body");

    mount_to_body(|| {
        view! { <App/> }
    });
}
