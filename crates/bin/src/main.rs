use clap::Parser;

#[derive(Parser)]
struct Cli {
    #[arg(long, value_name = "HOST:PORT")]
    grpc_addr: String,

    // ───── Config overrides ─────
    #[arg(long, default_value = "http://127.0.0.1:8899")]
    rpc_url: String,
    #[arg(long, default_value = "127.0.0.1")]
    tpu_ip: String,
    #[arg(long, default_value_t = 8000)]
    tpu_port: u16,
    #[arg(long, default_value = "127.0.0.1")]
    tpu_forward_ip: String,
    #[arg(long, default_value_t = 8001)]
    tpu_forward_port: u16,
    #[arg(long, value_name = "PUBKEY")]
    builder_pubkey: String,
    #[arg(long, default_value_t = 100)]
    builder_commission: u64,
    #[arg(long, default_value_t = 100)]
    min_bundle_size: usize,
    #[arg(
        long,
        alias = "min_tip",
        value_name = "LAMPORTS",
        default_value_t = 1000
    )]
    min_tip_lamports: u64,
    /// Multiple `--program <ID>` allowed
    #[arg(long = "program", value_name = "PROGRAM_ID", num_args = 1.., required = true)]
    programs_of_interest: Vec<String>,
    #[arg(long, value_name = "PATH")]
    validator_keypair_path: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    let cfg = be_gateway::Config {
        rpc_url: cli.rpc_url,
        tpu_ip: cli.tpu_ip,
        tpu_port: cli.tpu_port,
        tpu_forward_ip: cli.tpu_forward_ip,
        tpu_forward_port: cli.tpu_forward_port,
        block_builder_pubkey: cli.builder_pubkey,
        block_builder_commission: cli.builder_commission,
        min_bundle_size: cli.min_bundle_size,
        min_tip_lamports: cli.min_tip_lamports,
        programs_of_interest: cli.programs_of_interest,
        validator_keypair_path: cli.validator_keypair_path,
    };

    let addr = cli.grpc_addr.parse()?;
    be_gateway::run(cfg, addr).await
}
