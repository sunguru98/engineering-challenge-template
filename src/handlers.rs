use std::collections::BTreeMap;

use anyhow::Result;
use chrono::{DateTime, TimeDelta, Utc};
use engineering_challenge::Side;
use engineering_challenge::util::AlignedSbeWriter;
use engineering_challenge::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;

use crate::constants::STALL_TIMER_SECONDS;
use crate::messages::{Command, Level};
use crate::orderbook::{Books, Orderbook, write_snapshot};
use crate::types::{BatchSocketIdsMap, SocketEventSenderMap, SymbolBatchMap, SymbolBooksMap};

pub fn apply_snapshot(
    socket_id: usize,
    symbol_books_map: &mut SymbolBooksMap,
    symbol: String,
    timestamp: DateTime<Utc>,
    bids: Vec<Level>,
    asks: Vec<Level>,
) -> Result<()> {
    let asks: BTreeMap<i64, i64> = asks
        .into_iter()
        .map(|level| (level.price, level.quantity))
        .collect();

    let bids: BTreeMap<i64, i64> = bids
        .into_iter()
        .map(|level| (level.price, level.quantity))
        .collect();

    let Some(books) = symbol_books_map.get_mut(&symbol) else {
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

pub fn apply_level_update(
    socket_id: usize,
    symbol_books_map: &mut SymbolBooksMap,
    symbol: String,
    writer: &mut AlignedSbeWriter,
    timestamp: DateTime<Utc>,
    side: Side,
    price: i64,
    quantity: i64,
    received_at: DateTime<Utc>,
) -> Result<()> {
    let Some(books) = symbol_books_map.get_mut(&symbol) else {
        return Ok(());
    };

    if books.primary_socket_id.is_none() {
        books.primary_socket_id = Some(socket_id);
    }

    let orderbook = books
        .socket_orderbook
        .entry(socket_id)
        .or_insert_with(|| Orderbook::new());

    orderbook.timestamp = timestamp;

    let book_side = match side {
        Side::Buy => &mut orderbook.bids,
        Side::Sell => &mut orderbook.asks,
    };
    if quantity == 0 {
        book_side.remove(&price);
    } else {
        book_side.insert(price, quantity);
    }

    if books.primary_socket_id == Some(socket_id) {
        books.stalled_since = None;
        writer.write_msg(&PriceLevelUpdateDomain {
            exchange_timestamp: timestamp,
            backtest_timestamp: received_at,
            price_level: price,
            quantity,
            side,
            symbol,
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
                        write_snapshot(&symbol, orderbook, writer)?;
                    }
                }
            }
        }
    }

    Ok(())
}

pub fn add_symbol(
    symbol: String,
    symbol_books_map: &mut SymbolBooksMap,
    symbol_batch_map: &mut SymbolBatchMap,
    socket_event_sender_map: &SocketEventSenderMap,
    batch_socket_ids_map: &BatchSocketIdsMap,
    next_batch_idx: &mut usize,
    batches_len: usize,
) {
    if symbol_books_map.contains_key(&symbol) {
        return;
    }

    let batch_idx = *next_batch_idx;
    symbol_books_map.insert(symbol.clone(), Books::new());
    symbol_batch_map.insert(symbol.clone(), batch_idx);
    *next_batch_idx = (batch_idx + 1) % batches_len;

    let Some(socket_ids) = batch_socket_ids_map.get(&batch_idx) else {
        eprintln!("No websockets found for new_market");
        return;
    };

    for socket_id in socket_ids {
        let Some(event_sender) = socket_event_sender_map.get(socket_id) else {
            eprintln!("[new_market] Socket #{socket_id} not found");
            continue;
        };

        if let Err(err) = event_sender.try_send(Command::Subscribe {
            symbols: vec![symbol.clone()],
        }) {
            eprintln!("Error sending new_market event: {err:?}");
            continue;
        };
    }
}

pub fn remove_symbol(
    symbol: String,
    symbol_books_map: &mut SymbolBooksMap,
    symbol_batch_map: &mut SymbolBatchMap,
    socket_event_sender_map: &SocketEventSenderMap,
    batch_socket_ids_map: &BatchSocketIdsMap,
) {
    if !symbol_books_map.contains_key(&symbol) {
        return;
    }
    let Some(&batch_idx) = symbol_batch_map.get(&symbol) else {
        return;
    };

    symbol_books_map.remove(&symbol);
    symbol_batch_map.remove(&symbol);

    let Some(socket_ids) = batch_socket_ids_map.get(&batch_idx) else {
        return;
    };

    for socket_id in socket_ids {
        let Some(event_sender) = socket_event_sender_map.get(socket_id) else {
            eprintln!("[market_resolved] Socket #{socket_id} not found");
            continue;
        };

        if let Err(err) = event_sender.try_send(Command::Unsubscribe {
            symbols: vec![symbol.clone()],
        }) {
            eprintln!("Error sending market_resolved event: {err:?}");
            continue;
        };
    }
}
