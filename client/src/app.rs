use crate::FSClient;
use leptos::*;
use leptos_meta::*;
use leptos_router::*;

use crate::file::{FileEntry, UploadFileButton};

#[component]
pub fn App() -> impl IntoView {
    provide_meta_context();

    provide_context(FSClient::new());

    view! {
        <Stylesheet id="leptos" href="/pkg/tailwind.css"/>
        <Link rel="shortcut icon" type_="image/ico" href="/favicon.ico"/>
        <Router>
            <Routes>
                <Route path="" view=move || view! { <Home/> }/>
            </Routes>
        </Router>
    }
}

#[component]
fn Home() -> impl IntoView {
    let files = create_resource(
        || (),
        move |_| async move {
            log!("REQUESTING");

            let client: FSClient = expect_context();
            client.get_file_names().await
        },
    );

    view! {
        <div class="my-0 mx-auto max-w-3xl text-center h-screen">
            <h2 class="p-6 text-4xl">"Welcome to LQFS"</h2>
            <div class="flex justify-between px-10 pb-4">

                <p class="text-left">"Choose a file or upload one."</p>
                <UploadFileButton/>

            </div>

            <div class="py-2 h-3/5 bg-gray-300 rounded-lg overflow-auto">
                <ul>
                    {move || match files() {
                        Some(files) => {
                            view! {
                                <For
                                    each=move || files.clone()
                                    key=move |c| c.clone()

                                    view=|f| view! { <FileEntry file_name=f/> }
                                />
                            }
                        }
                        None => {
                            view! {
                                <For
                                    each=|| (0..20)
                                    key=|c| *c

                                    view=|_| {
                                        view! {
                                            <li class="animate-pulse max-w-full mx-4 my-2 rounded-md h-10 bg-slate-700"></li>
                                        }
                                    }
                                />
                            }
                        }
                    }}

                </ul>

            </div>

        </div>
    }
}
