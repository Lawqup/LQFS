use leptos::*;
use leptos_meta::*;
use leptos_router::*;

#[component]
pub fn App() -> impl IntoView {
    provide_meta_context();

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
    let (files, setFiles) = create_signal(None);

    view! {
        <div class="my-0 mx-auto max-w-3xl text-center h-screen">
            <h2 class="p-6 text-4xl">"Welcome to LQFS"</h2>
            <p class="px-10 pb-10 text-left">"Choose a file or upload one."</p>

            <div class="py-2 h-3/5 bg-gray-500 rounded-lg overflow-auto">
                <ul>
                    {move || {
                        if let Some(files) = files() {
                            files
                        } else {
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
