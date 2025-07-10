use clap::Parser;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    grpc_addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let addr = cli.grpc_addr.parse()?;
    be_gateway::run(addr).await
}
