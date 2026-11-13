use anyhow::Result;

mod collector;
mod constants;
mod exchanges;

use crate::collector::Collector;
use exchanges::polymarket::discovery::*;

#[tokio::main]
async fn main() -> Result<()> {
    let symbols = fetch_all_markets().await?;
    let mut collector = Collector::new(symbols)?;

    collector.run().await?;

    Ok(())
}
