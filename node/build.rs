fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("src/protos/fragment.proto")?;
    tonic_build::compile_protos("src/protos/service.proto")?;
    Ok(())
}
