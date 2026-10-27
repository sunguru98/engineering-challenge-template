use std::collections::{BTreeMap, HashMap};

use anyhow::Context;
use engineering_challenge::Side;
use engineering_challenge::util::AlignedSbeWriter;
use engineering_challenge::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::assets::extract_asset_id;
use crate::orderbook::{Orderbook, write_snapshot};
use crate::parse::convert_to_schema_int;
use crate::types::{BookEvent, MarketResolvedEvent, PriceChangeEvent, UpdateEvents};

pub async fn handle_market_resolved(
    assets_ids: &mut HashMap<String, Orderbook>,
    asset_worker_map: &mut HashMap<String, usize>,
    batch_sender_map: &HashMap<usize, mpsc::Sender<UpdateEvents>>,
    e_data: &serde_json::Value,
) -> anyhow::Result<()> {
    let event = MarketResolvedEvent::deserialize(e_data)?;

    for id in event.assets_ids {
        if !assets_ids.contains_key(&id) {
            continue;
        }
        let Some(&batch_idx) = asset_worker_map.get(&id) else {
            continue;
        };
        let Some(sender) = batch_sender_map.get(&batch_idx) else {
            continue;
        };

        sender
            .send(UpdateEvents::MarketResolved {
                asset_ids: vec![id.clone()],
            })
            .await?;
        assets_ids.remove(&id);
        asset_worker_map.remove(&id);
    }

    Ok(())
}

pub async fn handle_new_market(
    assets_ids: &mut HashMap<String, Orderbook>,
    asset_worker_map: &mut HashMap<String, usize>,
    batch_sender_map: &HashMap<usize, mpsc::Sender<UpdateEvents>>,
    next_batch_idx: &mut usize,
    batches_len: usize,
    e_data: &serde_json::Value,
) -> anyhow::Result<()> {
    let Some(asset_id) = extract_asset_id(e_data)? else {
        return Ok(());
    };

    if assets_ids.contains_key(&asset_id) {
        return Ok(());
    }

    let batch_idx = *next_batch_idx;
    let Some(sender) = batch_sender_map.get(&batch_idx) else {
        return Ok(());
    };

    sender
        .send(UpdateEvents::NewMarket {
            asset_ids: vec![asset_id.clone()],
        })
        .await?;

    assets_ids.insert(asset_id.clone(), Orderbook::new());
    asset_worker_map.insert(asset_id, batch_idx);
    *next_batch_idx = (batch_idx + 1) % batches_len;

    Ok(())
}

pub fn handle_book(
    assets_ids: &mut HashMap<String, Orderbook>,
    writer: &mut AlignedSbeWriter,
    e_data: &serde_json::Value,
) -> anyhow::Result<()> {
    let book_event = BookEvent::deserialize(e_data)?;
    let Some(orderbook) = assets_ids.get_mut(&book_event.asset_id) else {
        return Ok(());
    };

    let timestamp = chrono::DateTime::from_timestamp_millis(book_event.timestamp.parse()?)
        .context("Failed to parse timestamp")?;

    let mut asks = BTreeMap::new();
    for ask in &book_event.asks {
        asks.insert(
            convert_to_schema_int(&ask.price)?,
            convert_to_schema_int(&ask.size)?,
        );
    }

    let mut bids = BTreeMap::new();
    for bid in &book_event.bids {
        bids.insert(
            convert_to_schema_int(&bid.price)?,
            convert_to_schema_int(&bid.size)?,
        );
    }

    orderbook.timestamp = timestamp;
    orderbook.asks = asks;
    orderbook.bids = bids;

    write_snapshot(&book_event.asset_id, orderbook, writer)
}

pub fn handle_price_change(
    assets_ids: &mut HashMap<String, Orderbook>,
    writer: &mut AlignedSbeWriter,
    e_data: &serde_json::Value,
) -> anyhow::Result<()> {
    let event = PriceChangeEvent::deserialize(e_data)?;
    let exchange_timestamp = chrono::DateTime::from_timestamp_millis(event.timestamp.parse()?)
        .context("Failed to parse timestamp")?;

    for pc in event.price_changes {
        let Some(orderbook) = assets_ids.get_mut(&pc.asset_id) else {
            continue;
        };
        let Ok(price_level) = convert_to_schema_int(&pc.price) else {
            eprintln!("Invalid price: {}", pc.price);
            continue;
        };
        let Ok(quantity) = convert_to_schema_int(&pc.size) else {
            eprintln!("Invalid size: {}", pc.size);
            continue;
        };
        let side = match pc.side.as_str() {
            "SELL" => Side::Sell,
            "BUY" => Side::Buy,
            _ => continue,
        };

        orderbook.timestamp = exchange_timestamp;

        let book_side = match side {
            Side::Buy => &mut orderbook.bids,
            Side::Sell => &mut orderbook.asks,
        };
        if quantity == 0 {
            book_side.remove(&price_level);
        } else {
            book_side.insert(price_level, quantity);
        }

        writer.write_msg(&PriceLevelUpdateDomain {
            exchange_timestamp,
            backtest_timestamp: chrono::Utc::now(),
            price_level,
            quantity,
            side,
            symbol: pc.asset_id,
        })?;
    }

    Ok(())
}
