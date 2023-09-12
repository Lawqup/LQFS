use std::future::Future;

use leptos::*;
use leptos_icons::*;
use wasm_bindgen::prelude::*;
use web_sys::{Event, HtmlInputElement, InputEvent};

use crate::FSClient;

#[component]
pub fn FileEntry(file_name: String) -> impl IntoView {
    // TODO this doesnt need to be a signal
    let (file_name, _) = create_signal(file_name);
    let download = create_action(move |_: &()| async move {
        log!("DOWNLOADING");

        let client: FSClient = expect_context();
        let url = client.download_file(&file_name.get_untracked()).await;

        let a = document()
            .create_element("a")
            .unwrap()
            .dyn_into::<web_sys::HtmlElement>()
            .unwrap();

        a.set_attribute("href", &url).unwrap();
        a.set_attribute("download", &file_name.get_untracked())
            .unwrap();

        document().body().unwrap().append_child(&a).unwrap();
        a.click();
        document().body().unwrap().remove_child(&a).unwrap();
    });

    view! {
        <li class="max-w-full mx-4 my-2 rounded-md h-10 bg-slate-500 hover:bg-slate-700 px-4 flex items-center justify-between">
            <span class="text-left text-xl">{file_name}</span>
            <div class="w-40 flex items-center justify-between">
                <button on:click=move |_| download.dispatch(())>
                    <Icon icon=Icon::from(IoIcon::IoCodeDownloadSharp) class="text-white text-2xl"/>
                </button>
            </div>
        </li>
    }
}

#[component]
pub fn UploadFileButton() -> impl IntoView {
    let upload = create_action(move |e: &Event| {
        let el: HtmlInputElement = e.target().unwrap().dyn_into().unwrap();
        let f = el.files().unwrap().get(0).unwrap();

        async {
            let client: FSClient = expect_context();
            client.upload_file(f).await
        }
    });

    view! {
        <label
            for="upload"
            class="w-24 h-10 rounded-lg bg-gray-300 hover:bg-gray-400 flex items-center justify-center"
        >
            <input type="file" id="upload" on:change=move |e| upload.dispatch(e) hidden/>
            <Icon icon=Icon::from(AiIcon::AiPlusOutlined) class="text-black text-2xl"/>
        </label>
    }
}
