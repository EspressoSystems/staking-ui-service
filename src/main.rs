use std::{path::PathBuf, process::exit, sync::Arc};

use async_lock::RwLock;
use clap::Parser;
use staking_ui_service::{
    Result,
    input::l1::{self, RpcStream},
    persistence::sql,
};
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

    /// Location for persistent storage.
    #[clap(long, env = "ESPRESSO_STAKING_SERVICE_STORAGE")]
    storage: PathBuf,
}

impl Options {
    async fn run(self) -> Result<()> {
        let l1_input = RpcStream::new(&self.l1_http, &self.l1_ws).await?;
        let storage = sql::Persistence::new(&self.storage).await?;
        let l1_state = Arc::new(RwLock::new(l1::State::new(storage).await?));
        l1::State::subscribe(l1_state, l1_input).await;
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
