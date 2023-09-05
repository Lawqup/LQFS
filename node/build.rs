fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("protos/messages.proto")?;
    tonic_build::compile_protos("protos/services.proto")?;
    Ok(())
}
