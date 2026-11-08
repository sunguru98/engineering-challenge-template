use std::collections::BTreeMap;

use anyhow::Context;
use chrono::{DateTime, TimeDelta, Utc};
use engineering_challenge::Side;
use engineering_challenge::util::AlignedSbeWriter;
use engineering_challenge::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;
use serde::Deserialize;

use crate::assets::extract_asset_id;
use crate::constants::STALL_TIMER_SECONDS;
use crate::orderbook::{Books, Orderbook, write_snapshot};
use crate::parse::convert_to_schema_int;
use crate::types::{
    AssetBatchMap, AssetIds, BatchSocketIdsMap, BookEvent, Command, MarketResolvedEvent,
    PriceChangeEvent, SocketEventSenderMap,
};

pub fn handle_market_resolved(
    assets_ids: &mut AssetIds,
    asset_batch_map: &mut AssetBatchMap,
    socket_event_sender_map: &SocketEventSenderMap,
    batch_socket_ids_map: &BatchSocketIdsMap,
    e_data: &serde_json::Value,
) -> anyhow::Result<()> {
    let event = MarketResolvedEvent::deserialize(e_data)?;

    for id in event.assets_ids {
        if !assets_ids.contains_key(&id) {
            continue;
        }
        let Some(&batch_idx) = asset_batch_map.get(&id) else {
            continue;
        };

        assets_ids.remove(&id);
        asset_batch_map.remove(&id);

        let Some(socket_ids) = batch_socket_ids_map.get(&batch_idx) else {
            continue;
        };

        for socket_id in socket_ids {
            let Some(event_sender) = socket_event_sender_map.get(socket_id) else {
                eprintln!("[market_resolved] Socket #{socket_id} not found");
                continue;
            };

            if let Err(err) = event_sender.try_send(Command::MarketResolved {
                asset_ids: vec![id.clone()],
            }) {
                eprintln!("Error sending market_resolved event: {err:?}");
                continue;
            };
        }
    }

    Ok(())
}

pub fn handle_new_market(
    assets_ids: &mut AssetIds,
    asset_batch_map: &mut AssetBatchMap,
    socket_event_sender_map: &SocketEventSenderMap,
    batch_socket_ids_map: &BatchSocketIdsMap,
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
    assets_ids.insert(asset_id.clone(), Books::new());
    asset_batch_map.insert(asset_id.clone(), batch_idx);
    *next_batch_idx = (batch_idx + 1) % batches_len;

    let Some(socket_ids) = batch_socket_ids_map.get(&batch_idx) else {
        eprintln!("No websockets found for new_market");
        return Ok(());
    };

    for socket_id in socket_ids {
        let Some(event_sender) = socket_event_sender_map.get(socket_id) else {
            eprintln!("[new_market] Socket #{socket_id} not found");
            continue;
        };

        if let Err(err) = event_sender.try_send(Command::NewMarket {
            asset_ids: vec![asset_id.clone()],
        }) {
            eprintln!("Error sending new_market event: {err:?}");
            continue;
        };
    }

    Ok(())
}

pub fn handle_book(
    socket_id: usize,
    assets_ids: &mut AssetIds,
    e_data: &serde_json::Value,
) -> anyhow::Result<()> {
    let book_event = BookEvent::deserialize(e_data)?;

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

    let Some(books) = assets_ids.get_mut(&book_event.asset_id) else {
        return Ok(());
    };

    if books.primary_socket_id.is_none() {
        books.primary_socket_id = Some(socket_id);
    }

    let mut orderbook = Orderbook::new();
    orderbook.timestamp = timestamp;
    orderbook.asks = asks;
    orderbook.bids = bids;

    books.socket_orderbook.insert(socket_id, orderbook);

    Ok(())
}

pub fn handle_price_change(
    socket_id: usize,
    assets_ids: &mut AssetIds,
    writer: &mut AlignedSbeWriter,
    e_data: &serde_json::Value,
    received_at: DateTime<Utc>,
) -> anyhow::Result<()> {
    let event = PriceChangeEvent::deserialize(e_data)?;
    let exchange_timestamp = DateTime::from_timestamp_millis(event.timestamp.parse()?)
        .context("Failed to parse timestamp")?;

    for pc in event.price_changes {
        let side = match pc.side.as_str() {
            "SELL" => Side::Sell,
            "BUY" => Side::Buy,
            _ => continue,
        };

        let Ok(price_level) = convert_to_schema_int(&pc.price) else {
            eprintln!("Invalid price: {}", pc.price);
            continue;
        };
        let Ok(quantity) = convert_to_schema_int(&pc.size) else {
            eprintln!("Invalid size: {}", pc.size);
            continue;
        };

        let Some(books) = assets_ids.get_mut(&pc.asset_id) else {
            continue;
        };

        if books.primary_socket_id.is_none() {
            books.primary_socket_id = Some(socket_id);
        }

        let orderbook = books
            .socket_orderbook
            .entry(socket_id)
            .or_insert_with(|| Orderbook::new());

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

        if books.primary_socket_id == Some(socket_id) {
            books.stalled_since = None;
            writer.write_msg(&PriceLevelUpdateDomain {
                exchange_timestamp,
                backtest_timestamp: received_at,
                price_level,
                quantity,
                side,
                symbol: pc.asset_id,
            })?;
        } else {
            match books.stalled_since {
                None => books.stalled_since = Some(received_at),
                Some(t) => {
                    let time_elapsed = received_at - t;
                    if time_elapsed >= TimeDelta::seconds(STALL_TIMER_SECONDS) {
                        books.primary_socket_id = Some(socket_id);
                        books.stalled_since = None;
                        if let Some(orderbook) = books.socket_orderbook.get(&socket_id) {
                            write_snapshot(&pc.asset_id, orderbook, writer)?;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
