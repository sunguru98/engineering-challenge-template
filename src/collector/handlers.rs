use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::util::AlignedSbeWriter;
use engineering_challenge::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;

use crate::collector::{
    orderbook::{Books, WriteAction},
    snapshot::write_snapshot,
    subscription::Subscription,
};
use crate::messages::{Command, Level};

pub fn apply_snapshot(
    socket_id: usize,
    symbol_books_map: &mut HashMap<String, Books>,
    symbol: String,
    timestamp: DateTime<Utc>,
    bids: Vec<Level>,
    asks: Vec<Level>,
) {
    let Some(books) = symbol_books_map.get_mut(&symbol) else {
        return;
    };

    books.apply_snapshot(socket_id, timestamp, bids, asks);
}

pub fn apply_level_update(
    socket_id: usize,
    symbol_books_map: &mut HashMap<String, Books>,
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

    match books.apply_update(socket_id, timestamp, side, price, quantity, received_at) {
        WriteAction::Update => writer.write_msg(&PriceLevelUpdateDomain {
            exchange_timestamp: timestamp,
            backtest_timestamp: received_at,
            price_level: price,
            quantity,
            side,
            symbol,
        })?,
        WriteAction::Snapshot => {
            if let Some(primary_orderbook) = books.primary_book() {
                write_snapshot(&symbol, primary_orderbook, writer)?;
            }
        }
        WriteAction::Nothing => {}
    }

    Ok(())
}

pub fn add_symbol(
    symbol: String,
    symbol_books_map: &mut HashMap<String, Books>,
    subscription: &mut Subscription,
) {
    if symbol_books_map.contains_key(&symbol) {
        return;
    }

    let Some(batch_idx) = subscription.assign_symbol(&symbol) else {
        return;
    };

    symbol_books_map.insert(symbol.clone(), Books::new());
    subscription.broadcast_to_batch(
        batch_idx,
        Command::Subscribe {
            symbols: vec![symbol],
        },
    );
}

pub fn remove_symbol(
    symbol: String,
    symbol_books_map: &mut HashMap<String, Books>,
    subscription: &mut Subscription,
) {
    if !symbol_books_map.contains_key(&symbol) {
        return;
    }

    let Some(batch_idx) = subscription.release_symbol(&symbol) else {
        return;
    };

    symbol_books_map.remove(&symbol);

    subscription.broadcast_to_batch(
        batch_idx,
        Command::Unsubscribe {
            symbols: vec![symbol],
        },
    );
}
