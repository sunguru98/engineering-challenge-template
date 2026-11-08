use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use engineering_challenge::util::{
    AlignedSbeWriter,
    dtos::challenge_orderbook::{SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain},
};

use crate::types::AssetIds;

#[derive(Debug)]
pub struct Books {
    pub socket_orderbook: HashMap<usize, Orderbook>,
    pub primary_socket_id: Option<usize>,
}

impl Books {
    pub fn new() -> Self {
        Self {
            socket_orderbook: HashMap::new(),
            primary_socket_id: None,
        }
    }
}

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

pub fn take_snapshot(assets_ids: &AssetIds, writer: &mut AlignedSbeWriter) -> anyhow::Result<()> {
    for (asset_id, books) in assets_ids.iter() {
        let Some(primary_socket_id) = books.primary_socket_id else {
            eprintln!("Skipping {asset_id} since no socket present");
            continue;
        };

        let Some(orderbook) = books.socket_orderbook.get(&primary_socket_id) else {
            continue;
        };

        write_snapshot(asset_id, orderbook, writer)?;
    }
    Ok(())
}
