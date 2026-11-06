use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use engineering_challenge::util::{
    AlignedSbeWriter,
    dtos::challenge_orderbook::{SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain},
};
// use tokio::sync::mpsc::Sender;

// use crate::types::UpdateEvents;

// pub struct CentralProcessor {
//     pub assets_ids: HashMap<String, Orderbook>,
//     pub batches: Vec<Vec<String>>,
//     pub asset_id_batch_idx: HashMap<String, usize>,
//     pub batch_idx_sender_channel: HashMap<usize, Sender<UpdateEvents>>
// }

// impl CentralProcessor {
//     pub fn new() -> Self {

//     }
// }

#[derive(Debug)]
pub struct Orderbook {
    pub timestamp: DateTime<chrono::Utc>,
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
}

impl Orderbook {
    pub fn new() -> Self {
        Self {
            timestamp: Utc::now(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }
}
pub fn write_snapshot(
    asset_id: &str,
    orderbook: &Orderbook,
    writer: &mut AlignedSbeWriter,
) -> anyhow::Result<()> {
    let mut asks = vec![];
    for (price_level, quantity) in orderbook.asks.iter() {
        asks.push(SnapshotAsksEntryDomain {
            price_level: *price_level,
            quantity: *quantity,
        })
    }

    let mut bids = vec![];
    for (price_level, quantity) in orderbook.bids.iter().rev() {
        bids.push(SnapshotBidsEntryDomain {
            price_level: *price_level,
            quantity: *quantity,
        })
    }

    let snapshot = SnapshotDomain {
        asks,
        bids,
        exchange_timestamp: orderbook.timestamp,
        received_at: chrono::Utc::now(),
        symbol: asset_id.to_owned(),
    };
    writer.write_msg(&snapshot)?;

    Ok(())
}

pub fn take_snapshot(
    assets_ids: &HashMap<String, Orderbook>,
    writer: &mut AlignedSbeWriter,
) -> anyhow::Result<()> {
    for (asset_id, orderbook) in assets_ids.iter() {
        write_snapshot(asset_id, orderbook, writer)?;
    }
    Ok(())
}
