use anyhow::Result;

mod backoff;
mod collector;
mod constants;
mod exchanges;

use crate::{
    collector::Collector,
    exchanges::{adapter::Adapter, polymarket::Polymarket},
};

#[tokio::main]
async fn main() -> Result<()> {
    let exchange = Polymarket {};
    let symbols = exchange.discover().await?;
    let mut collector = Collector::new(exchange, symbols)?;

    collector.run().await?;

    Ok(())
}
