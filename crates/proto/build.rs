use glob::glob;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos: Vec<_> = glob("../../vendor/mev-protos/**/*.proto")?
        .filter_map(Result::ok)
        .map(|p| p.to_str().unwrap().to_owned())
        .collect();

    tonic_build::configure()
        .build_server(true)
        .out_dir("src")
        .compile(&protos, &["../../vendor/mev-protos"])?;

    Ok(())
}