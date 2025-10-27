use std::process::exit;

use clap::Parser;
use staking_ui_service::{Result, input::l1::RpcStream};
use tide_disco::Url;

/// The backend service for the Espresso Network Staking UI.
#[derive(Debug, Parser)]
struct Options {
    /// HTTP endpoints for L1 RPC services.
    #[clap(long, env = "ESPRESSO_STAKING_SERVICE_L1_HTTP", value_delimiter = ',')]
    l1_http: Vec<Url>,

    /// WebSockets endpoints for L1 RPC services.
    #[clap(long, env = "ESPRESSO_STAKING_SERVICE_L1_WS", value_delimiter = ',')]
    l1_ws: Vec<Url>,
}

impl Options {
    async fn run(self) -> Result<()> {
        let _l1_input = RpcStream::new(&self.l1_http, &self.l1_ws).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let opt = Options::parse();
    if let Err(err) = opt.run().await {
        eprintln!("service failed: {err:#}");
        exit(1);
    }
}
