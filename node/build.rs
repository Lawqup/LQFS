fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile(&["services.proto", "messages.proto"], &["../protos"])?;
    Ok(())
}
